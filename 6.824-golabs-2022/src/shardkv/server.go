package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

const Debug = true

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

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]string
	replyCh     map[IndexAndTerm]chan CommonReply
	cmd         map[int64]int64 // 对于每个 Client 执行到哪个 Command
	lastApplied int

	sm               *shardctrler.Clerk                // 获取配置的客户端
	config           shardctrler.Config                // 当前的配置
	shardsAcceptable map[int]bool                      // shard -> bool
	needPullShards   map[int]int                       // shard -> configNum 表示我要拉取的分片在哪个 config
	needSendShards   map[int]map[int]map[string]string // (configNum, shard) -> data 迁移时需要发送的数据
	gcList           map[int]int                       // shard -> configNum 表示我要回收的分片是属于哪个 config
}

type IndexAndTerm struct {
	Index int
	Term  int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	cmd := Op{Key: args.Key, Value: "", Op: Get, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.Err, reply.Value = kv.ProcessCommandHandler(cmd)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.Err, _ = kv.ProcessCommandHandler(cmd)
}

func (kv *ShardKV) IsDuplicate(cmd Op) bool {
	if CommandId, ok := kv.cmd[cmd.ClientId]; ok && CommandId >= cmd.CommandId {
		return true
	}
	return false
}

func (kv *ShardKV) CloseChannel(it IndexAndTerm) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.replyCh[it]
	if !ok {
		return
	}
	close(ch)
	delete(kv.replyCh, it)
}

func (kv *ShardKV) ProcessCommandHandler(cmd Op) (Err, string) {
	var err Err
	var value string
	DPrintf("[Server.ProcessCommandHandler] [me: %v, cmd: %+v]\n", kv.me, cmd)
	kv.mu.Lock()
	if _, ok := kv.shardsAcceptable[key2shard(cmd.Key)]; !ok {
		kv.mu.Unlock()
		return ErrWrongGroup, ""
	}
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

	go kv.CloseChannel(it)

	return err, value
}

func (kv *ShardKV) applier() {
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

func (kv *ShardKV) handleCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.CommandIndex <= kv.lastApplied {
		return
	}

	switch msg.Command.(type) {
	case ConfigUpdateCommand:
		cmd := msg.Command.(ConfigUpdateCommand)
		DPrintf("[ShardKV.ConfigUpdateCommand] [cmd : %+v]\n", cmd)
		kv.updateConfig(cmd.Config)
	case ShardReplicationCommand:
		cmd := msg.Command.(ShardReplicationCommand)
		DPrintf("[ShardKV.ShardReplicationCommand] [cmd : %+v]\n", cmd)
		kv.replicateShard(cmd)
	case GcCommand:
		cmd := msg.Command.(GcCommand)
		DPrintf("[ShardKV.GcCommand] [cmd : %+v]\n", cmd)
		if _, ok := kv.needSendShards[cmd.Num][cmd.Shard]; ok {
			delete(kv.needSendShards[cmd.Num], cmd.Shard)
		}
		if replyCh, ok := kv.replyCh[IndexAndTerm{msg.CommandIndex, msg.CommandTerm}]; ok {
			replyCh <- CommonReply{Err: OK}
		}
	case GcSuccessCommand:
		cmd := msg.Command.(GcSuccessCommand)
		DPrintf("[ShardKV.GcSuccessCommand] [cmd : %+v]\n", cmd)
		if configNum, ok := kv.gcList[cmd.Shard]; ok && configNum == cmd.Num {
			delete(kv.gcList, cmd.Shard)
		}
	default:
		op := msg.Command.(Op)
		reply := CommonReply{}
		if _, ok := kv.shardsAcceptable[key2shard(op.Key)]; !ok {
			reply = CommonReply{ErrWrongGroup, ""}
		} else {
			if op.Op != Get && kv.IsDuplicate(op) {
				reply = CommonReply{OK, ""}
			} else {
				switch op.Op {
				case Get:
					if value, ok := kv.data[op.Key]; ok {
						reply = CommonReply{OK, value}
					} else {
						reply = CommonReply{ErrNoKey, ""}
					}
				case Put:
					kv.data[op.Key] = op.Value
					reply = CommonReply{OK, ""}
				case Append:
					kv.data[op.Key] += op.Value
					reply = CommonReply{OK, ""}
				}
			}
		}
		if replyCh, ok := kv.replyCh[IndexAndTerm{msg.CommandIndex, msg.CommandTerm}]; ok {
			replyCh <- reply
			DPrintf("[kv.handleCommand] [msg: %+v is applied]\n", msg)
		}
		kv.cmd[op.ClientId] = op.CommandId
		kv.lastApplied = msg.CommandIndex
	}

	if kv.needSnapshot() {
		kv.createSnapshot(msg.CommandIndex)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ConfigUpdateCommand{})
	labgob.Register(ShardReplicationCommand{})
	labgob.Register(GcCommand{})
	labgob.Register(GcSuccessCommand{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.replyCh = make(map[IndexAndTerm]chan CommonReply)
	kv.cmd = make(map[int64]int64)

	kv.sm = shardctrler.MakeClerk(kv.ctrlers)
	kv.shardsAcceptable = make(map[int]bool)
	kv.needPullShards = make(map[int]int)
	kv.needSendShards = make(map[int]map[int]map[string]string)
	kv.gcList = make(map[int]int)

	kv.readSnapshot(kv.rf.GetSnapshot())

	go kv.applier()
	go kv.Ticker(kv.ConfigureAction, ConfigureDuration)
	go kv.Ticker(kv.MigrationAction, MigrationDuration)
	go kv.Ticker(kv.GcAction, GcDuration)

	return kv
}

func (kv *ShardKV) Ticker(fn func(), timeout time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			fn()
		}
		time.Sleep(timeout)
	}
}

func (kv *ShardKV) getNextConfig(Num int) (shardctrler.Config, bool) {
	config := kv.sm.Query(Num + 1)
	return config, config.Num == Num+1
}

func (kv *ShardKV) ConfigureAction() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// 还有未拉取完的 shards
	if len(kv.needPullShards) > 0 {
		return
	}
	DPrintf("[ConfigureAction] [me: %+v, kv.needPullShards: %+v]\n", kv.me, kv.needPullShards)
	if config, ok := kv.getNextConfig(kv.config.Num); ok {
		kv.rf.Start(ConfigUpdateCommand{config})
		DPrintf("[ConfigureAction] [ConfigUpdateCommand: %+v]\n", config)
	}
}

func (kv *ShardKV) MigrationAction() {
	kv.mu.Lock()
	wg := sync.WaitGroup{}
	DPrintf("[MigrationAction] [me: %v, kv.needPullShards: %+v]\n", kv.me, kv.needPullShards)
	for shard, configNum := range kv.needPullShards {
		// DPrintf("[MigrationAction] [me: %v, shard: %v, configNum: %v]\n", kv.me, shard, configNum)
		config := kv.sm.Query(configNum)
		wg.Add(1)
		go func(config shardctrler.Config, shard int) {
			defer wg.Done()
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				return
			}
			gId := config.Shards[shard]
			args := ShardPullArgs{shard, config.Num}
			for _, server := range config.Groups[gId] {
				reply := ShardPullReply{}
				ok := kv.make_end(server).Call("ShardKV.ShardPull", &args, &reply) // 之前的 group 里拉取分片。
				DPrintf("[MigrationAction] [me: %v, args: %+v, ShardPull Data: %+v]\n", kv.me, args, reply.Data)
				if ok && reply.Err == OK {
					kv.rf.Start(ShardReplicationCommand{Data: reply.Data, Cmd: reply.Cmd, Shard: shard, Num: config.Num})
					break
				}
			}
		}(config, shard)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) GcAction() {
	kv.mu.Lock()
	wg := sync.WaitGroup{}
	DPrintf("[GcAction] [me: %v, kv.gcList: %+v]\n", kv.me, kv.gcList)
	for shard, configNum := range kv.gcList {
		// DPrintf("[MigrationAction] [me: %v, shard: %v, configNum: %v]\n", kv.me, shard, configNum)
		wg.Add(1)
		config := kv.sm.Query(configNum)
		go func(config shardctrler.Config, shard int) {
			defer wg.Done()
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				return
			}
			gId := config.Shards[shard]
			args := ShardGcArgs{Shard: shard, Num: config.Num}
			for _, server := range config.Groups[gId] {
				reply := ShardGcReply{}
				ok := kv.make_end(server).Call("ShardKV.ShardGc", &args, &reply) // 删除 PullSend
				if ok && reply.Err == OK {
					kv.rf.Start(GcSuccessCommand{Shard: shard, Num: config.Num}) // 删除 gcList
					break
				}
			}
		}(config, shard)
	}
	kv.mu.Unlock()
	wg.Wait()
}
