package shardkv

import (
	"6.824/shardctrler"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrTimeOut         = "ErrTimeOut"
	ErrServerNotUpdate = "ErrServerNotUpdate"
	replyTimeOut       = time.Duration(500) * time.Millisecond
	ConfigureDuration  = time.Duration(50) * time.Millisecond
	MigrationDuration  = time.Duration(30) * time.Millisecond
	GcDuration         = time.Duration(50) * time.Millisecond
)

const ( // kvRaft 的状态
	Get    = "Get"
	Put    = "Put"
	Append = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	CommandId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonReply struct {
	Err   Err
	Value string
}

type ConfigUpdateCommand struct {
	Config shardctrler.Config
}

type ShardPullArgs struct {
	Shard int
	Num   int
}

type ShardPullReply struct {
	Data map[string]string
	Cmd  map[int64]int64
	Err  Err
}

type ShardGcArgs struct {
	Shard int
	Num   int
}

type ShardGcReply struct {
	Err Err
}

type GcSuccessCommand struct {
	Shard int
	Num   int
}

type GcCommand struct {
	Shard int
	Num   int
}

type ShardReplicationCommand struct {
	Shard int
	Num   int
	Cmd   map[int64]int64
	Data  map[string]string
}
