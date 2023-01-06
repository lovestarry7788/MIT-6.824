package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	ClientId  int64
	CommandId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data    map[string]string
	replyCh map[IndexAndTerm]chan CommonReply
	// cmd         map[ClientAndCommand]bool // 幂等
	cmd         map[int64]int64 // 对于每个 Client 执行到哪个 Command
	lastApplied int
}

type IndexAndTerm struct {
	Index int
	Term  int
}

type ClientAndCommand struct {
	ClientId  int64
	CommandId int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{Key: args.Key, Value: "", Op: "Get", ClientId: args.ClientId, CommandId: args.CommandId}
	reply.Err, reply.Value = kv.ProcessCommandHandler(cmd)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.Err, _ = kv.ProcessCommandHandler(cmd)
}

func (kv *KVServer) IsDuplicate(cmd Op) bool {
	if id, ok := kv.cmd[cmd.ClientId]; ok && id >= cmd.CommandId {
		return true
	}
	return false
}

func (kv *KVServer) ProcessCommandHandler(cmd Op) (Err, string) {
	var err Err
	var value string
	DPrintf("[Server.ProcessCommandHandler] [me: %v, cmd: %+v]\n", kv.me, cmd)
	kv.mu.Lock()
	if cmd.Op != "Get" && kv.IsDuplicate(cmd) {
		kv.mu.Unlock()
		return OK, ""
	}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		err, value = ErrWrongLeader, ""
		DPrintf("[Server.ProcessCommandHandler] [me: %v isn't Leader]\n", kv.me)
		kv.mu.Unlock()
		return err, value
	}
	it := IndexAndTerm{
		Index: index,
		Term:  term,
	}
	ch := make(chan CommonReply, 1)
	kv.replyCh[it] = ch
	DPrintf("[Server.ProcessCommandHandler] [cmd: %v, it: %v]\n", cmd, it)
	kv.mu.Unlock()

	select {
	case replyMsg := <-ch:
		err, value = replyMsg.Err, replyMsg.Value
	case <-time.After(replyTimeOut):
		err, value = ErrTimeOut, ""
	}

	go func() { // 关闭管道
		kv.mu.Lock()
		defer kv.mu.Unlock()
		ch, ok := kv.replyCh[it]
		if !ok {
			return
		}
		close(ch)
		delete(kv.replyCh, it)
	}()

	return err, value
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh // raft.ApplyMsg
		if kv.killed() {
			break
		}
		if msg.CommandValid {
			kv.handleCommand(msg)
		} else if msg.SnapshotValid {
			kv.handleSnapshot(msg)
		}
	}
}

func (kv *KVServer) handleCommand(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	reply := CommonReply{}

	kv.mu.Lock()
	if msg.CommandIndex <= kv.lastApplied {
		kv.mu.Unlock()
		return
	}

	if op.Op != "Get" && kv.IsDuplicate(op) {
		reply = CommonReply{OK, ""}
	} else {
		switch op.Op {
		case "Get":
			if value, ok := kv.data[op.Key]; ok {
				reply = CommonReply{OK, value}
			} else {
				reply = CommonReply{ErrNoKey, ""}
			}
		case "Put":
			kv.data[op.Key] = op.Value
			reply = CommonReply{OK, ""}
		case "Append":
			kv.data[op.Key] += op.Value
			reply = CommonReply{OK, ""}
		}
	}

	if replyCh, ok := kv.replyCh[IndexAndTerm{msg.CommandIndex, msg.CommandTerm}]; ok {
		replyCh <- reply
		DPrintf("[kv.handleCommand] [msg: %+v is applied]\n", msg)
	}
	/*
		clientAndcommand := ClientAndCommand{
			ClientId:  op.ClientId,
			CommandId: op.CommandId,
		}
		kv.cmd[clientAndcommand] = true
	*/
	kv.cmd[op.ClientId] = op.CommandId
	kv.lastApplied = msg.CommandIndex

	if kv.maxraftstate != -1 && (float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) > 0.9) {
		// 创建快照
		DPrintf("[handleCommand] [kv.rf.GetRaftStateSize: %v, kv.maxraftstate: %v]\n", kv.rf.GetRaftStateSize(), kv.maxraftstate)
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.data)
		e.Encode(kv.cmd)
		Snapshot := w.Bytes()
		go kv.rf.Snapshot(msg.CommandIndex, Snapshot)
		DPrintf("[handleCommand] [me: %v, Create snapshot success!]\n", kv.me)
	}

	kv.mu.Unlock()
}

func (kv *KVServer) handleSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	if msg.SnapshotIndex <= kv.lastApplied {
		kv.mu.Unlock()
		return
	}
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.lastApplied = msg.SnapshotIndex
		kv.readSnapshot(msg.Snapshot)
	}
	kv.mu.Unlock()
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.cmd)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.replyCh = make(map[IndexAndTerm]chan CommonReply)
	// kv.cmd = make(map[ClientAndCommand]bool)
	kv.cmd = make(map[int64]int64)
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.applier()

	return kv
}
