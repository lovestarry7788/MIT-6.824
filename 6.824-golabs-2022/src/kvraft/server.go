package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
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
	Key string
	Value string
	Op string
	ClientId int64
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
	replyMap map[int64]bool // 幂等

}

type IndexAndTerm struct {
	Index int
	Term  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{Key: args.Key, Value: "", Op: "Get", ClientId: args.ClientId}
	reply.Err, reply.Value = kv.ProcessCommandHandler(cmd)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId}
	reply.Err, _ = kv.ProcessCommandHandler(cmd)
}

func (kv *KVServer) IsDuplicate(cmd Op) bool {
	return kv.replyMap[cmd.ClientId]
}

func (kv *KVServer) ProcessCommandHandler(cmd Op) (Err, string){
	var err Err
	var value string
	kv.mu.Lock()
	if cmd.Op != "Get" && kv.IsDuplicate(cmd) {
		kv.mu.Unlock()
		return OK, ""
	}
	index, term, isLeader := kv.rf.Start(cmd)
	if !isLeader {
		err, value = ErrWrongLeader, ""
		return err, value
	}
	it := IndexAndTerm{
		Index: index,
		Term:  term,
	}
	ch := make(chan CommonReply, 1)
	kv.replyCh[it] = ch
	kv.mu.Unlock()

	select {
	case replyMsg := <-ch:
		err, value = replyMsg.Err,replyMsg.Value
	case <-time.After(replyTimeOut):
		err, value = ErrTimeOut, ""
	}
	return err, value
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
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
	op := msg.Command.
	switch msg.Command.(type) {
	case GetArgs:

	case PutAppendArgs:
		reply := CommonReply{}
		if value, ok := kv.data[msg.Command.(Op)]; ok {

		}
	}
}

func (kv *KVServer) handleSnapshot(msg raft.ApplyMsg) {

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

	return kv
}
