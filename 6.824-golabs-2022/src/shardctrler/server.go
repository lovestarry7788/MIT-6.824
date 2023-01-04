package shardctrler

import (
	"6.824/raft"
	"log"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	cmd         map[int64]int64
	replyCh     map[IndexAndTerm]chan CommonReply
	configs     []Config // indexed by config num
	lastApplied int
}

type IndexAndTerm struct {
	Index int
	Term  int
}

type Op struct {
	// Your data here.
	Op        string
	Servers   map[int][]string
	ClientId  int64
	CommandId int64
	GIDs      []int
	Shard     int
	GID       int
	Num       int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd := Op{Op: Join, Servers: args.Servers, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.WrongLeader, reply.Err, _ = sc.ProcessShardCtrlerCmd(cmd)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd := Op{Op: Leave, GIDs: args.GIDs, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.WrongLeader, reply.Err, _ = sc.ProcessShardCtrlerCmd(cmd)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd := Op{Op: Move, Shard: args.Shard, GID: args.GID, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.WrongLeader, reply.Err, _ = sc.ProcessShardCtrlerCmd(cmd)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd := Op{Op: Query, Num: args.Num, ClientId: args.ClientId, CommandId: args.CommandId}
	reply.WrongLeader, reply.Err, reply.Config = sc.ProcessShardCtrlerCmd(cmd)
}

func (sc *ShardCtrler) ProcessShardCtrlerCmd(cmd Op) (bool, Err, Config) {
	var err Err
	var config Config
	sc.mu.Lock()
	defer sc.mu.Unlock()
	index, term, isleader := sc.rf.Start(cmd)
	if !isleader {
		return true, ErrWrongLeader, config
	}
	it := IndexAndTerm{
		Index: index,
		Term:  term,
	}
	ch := make(chan CommonReply, 1)
	sc.replyCh[it] = ch
	select {
	case replyMsg := <-ch:
		err, config = replyMsg.Err, replyMsg.Config
	case <-time.After(replyTimeOut):
		err = ErrTimeOut

	}

	go func() {
		sc.mu.Lock()
		defer sc.mu.Unlock()
		ch, ok := sc.replyCh[it]
		if !ok {
			return
		}
		close(ch)
		delete(sc.replyCh, it)
	}()

	return false, err, config
}

func (sc *ShardCtrler) applier() {
	for {
		msg := <-sc.applyCh
		if msg.CommandValid {
			sc.handleCommand(msg)
		} else if msg.SnapshotValid {
			sc.handleSnapshot(msg)
		}
	}
}

func (sc *ShardCtrler) handleCommand(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	reply := CommonReply{}
	DPrintf("[ShardCtrler.handleCommand] [msg:%+v] \n", msg)
	sc.mu.Lock()
	if msg.CommandIndex <= sc.lastApplied {
		sc.mu.Unlock()
		return
	}

	switch op.Op {
	case Join: // 添加 group 和 servers 的映射
		config := Config{}
		lastConfig := sc.configs[len(sc.configs)-1]
		config.Num = lastConfig.Num + 1
		if config.Num == 1 {
			config.Groups = op.Servers
			config.Shards = sc.distributeShards(config.Groups)
		} else {
			config.Groups = make(map[int][]string)
			for gid, servers := range lastConfig.Groups {
				config.Groups[gid] = servers
			}
			for gid, servers := range op.Servers {
				config.Groups[gid] = servers
			}
			config.Shards = sc.distributeShards(config.Groups)
		}
		sc.configs = append(sc.configs, config)
		reply.Err, reply.Config = OK, Config{}
	case Leave: // 删除 group
		config := Config{}
		lastConfig := sc.configs[len(sc.configs)-1]
		config.Num = lastConfig.Num + 1
		config.Groups = make(map[int][]string)
		for gid, servers := range lastConfig.Groups {
			config.Groups[gid] = servers
		}
		for _, gid := range op.GIDs {
			delete(config.Groups, gid)
		}
		config.Shards = sc.distributeShards(config.Groups)
		reply.Err, reply.Config = OK, Config{}
	case Move: // 将指定的 shardId 交给 groupId
		lastConfig := sc.configs[len(sc.configs)-1]
		config := Config{
			Num:    lastConfig.Num + 1,
			Shards: lastConfig.Shards,
			Groups: lastConfig.Groups,
		}
		config.Shards[op.Shard] = op.GID
		reply.Err, reply.Config = OK, Config{}
	case Query: // 查询对应 num 的 config
		if op.Num < 0 || op.Num >= len(sc.configs) {
			reply.Err, reply.Config = OK, sc.configs[len(sc.configs)-1]
		} else {
			reply.Err, reply.Config = OK, sc.configs[op.Num]
		}
	}

	if replyCh, ok := sc.replyCh[IndexAndTerm{msg.CommandIndex, msg.CommandTerm}]; ok {
		replyCh <- reply
		DPrintf("[kv.handleCommand] [msg: %+v is applied]\n", msg)
	}

	sc.cmd[op.ClientId] = op.CommandId
	sc.lastApplied = msg.CommandIndex

	sc.mu.Unlock()
}

func (sc *ShardCtrler) distributeShards(groups map[int][]string) (shards [NShards]int) {
	if len(groups) == 0 {
		return
	}
	gIds, i := make([]int, len(groups)), 0
	for gId := range groups {
		gIds[i] = gId
		i++
	}
	average := NShards / len(gIds)
	if average == 0 {
		for shard := range shards {
			shards[shard] = gIds[shard]
		}
	} else {
		for shard := range shards {
			if shards[shard]/average >= len(gIds) {
				shards[shard] = gIds[len(gIds)-1]
			} else {
				shards[shard] = gIds[shard/average]
			}
		}
	}
	return
}

func (sc *ShardCtrler) handleSnapshot(msg raft.ApplyMsg) {

}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.cmd = make(map[int64]int64)
	sc.replyCh = make(map[IndexAndTerm]chan CommonReply)
	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	sc.configs[0].Groups = make(map[int][]string)

	go sc.applier()

	return sc
}
