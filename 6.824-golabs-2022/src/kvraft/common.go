package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeOut     = "ErrTimeOut"
	replyTimeOut   = time.Duration(500) * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	CommandId int64
	ClientId  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	CommandId int64
	ClientId  int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// 表示 GetReply 和 PutReply
type CommonReply struct {
	Err   Err
	Value string
}
