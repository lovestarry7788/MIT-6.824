package raft

import "time"

type LogEntry struct {
	Command interface{} // 业务方自定的命令，写请求之类
	Index   int         // 日志的序号，由领导者分配
	Term    int         // 写入命令的领导者处于的任期
}

type AppendEntriesArgs struct {
	Term         int        // Leader 任期号
	Leader       int        // Leader id 为了能帮助客户端重定向到 Leader 服务器
	PrevLogIndex int        // 表示当前要复制的日志项，前一条日志项的索引值。
	PrevLogTerm  int        // 表示当前要复制的日志项，前一条日志项的任期编号。
	Entries      []LogEntry // 需要同步的日志条目
	LeaderCommit int        // leader 已提交的日志号
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// fast back up
	XIndex int // 日志冲突时，记录冲突日志的 Index；如果不存在日志，否则记录 -1。
	XTerm  int // 日志冲突时，记录冲突日志的 Term
	XLen   int //
}

func (rf *Raft) FirstLog() LogEntry {
	return rf.log[0]
}

func (rf *Raft) LastLog() LogEntry {
	if len(rf.log) == 0 {
		return LogEntry{
			Index: rf.lastIncludedIndex,
			Term:  rf.lastIncludedTerm,
		}
	}
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) FindLog(idx int) LogEntry {
	if idx == rf.lastIncludedIndex {
		return LogEntry{
			Index: rf.lastIncludedIndex,
			Term:  rf.lastIncludedTerm,
		}
	} else if len(rf.log) == 0 {
		panic("Cannot Find the log0.")
	} else {
		return rf.log[idx-rf.lastIncludedIndex-1]
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	// 要复制的Index日志已经在快照中 / 可理解为 server 落后太多，raft采用 快照+持久化 的方式来恢复
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		DPrintf("[HandleAppendEntries] [%v send snapshot to %v, snapshot: %v]\n", rf.me, server, rf.snapshot)
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		reply := &InstallSnapshotReply{}
		rf.mu.Unlock()
		if ok := rf.sendInstallSnapshot(server, args, reply); !ok {
			DPrintf("rf.sendInstallSnapshot error, from: %v, to: %v, args: %v, reply: %v\n", rf.me, server, args, reply)
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term { // 过期请求
			return
		}
		// 发现新的 term, leader 变成 follower
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.toFollower()
			rf.votedFor = -1
			rf.persist()
			return
		}
		rf.matchIndex[server] = rf.lastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		return
	}

	DPrintf("[HandleAppendEntries] [me: %v, server: %v, PrevLogIndex: %v, PrevLogTerm: %v]\n", rf.me, server, rf.nextIndex[server]-1, rf.FindLog(rf.nextIndex[server]-1).Term)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leader:       rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.FindLog(rf.nextIndex[server] - 1).Term,
		LeaderCommit: rf.commitIndex,
	}
	if rf.nextIndex[server] > rf.lastIncludedIndex && rf.lastIncludedIndex+len(rf.log) >= rf.nextIndex[server] {
		args.Entries = rf.log[(rf.nextIndex[server] - rf.lastIncludedIndex - 1):]
	} else {
		args.Entries = nil
	}
	reply := &AppendEntriesReply{}
	rf.mu.Unlock()
	// 要复制的要在 log 的范围内
	// if len(rf.log)-1 >= rf.nextIndex[server] {
	// request
	if ok := rf.sendAppendEntries(server, args, reply); !ok {
		DPrintf("rf.sendAppendEntries error, from: %v, to: %v, args: %v, reply: %v\n", rf.me, server, args, reply)
		return
	}
	DPrintf("log replicated %v from %v to %v, reply: %+v\n", args.Entries, rf.me, server, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.currentTerm != args.Term { // 过期请求
		return
	}
	// 发现新的 term, leader 变成 follower
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.toFollower()
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		// PrevLogIndex 处无日志
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen + 1
		} else {
			rf.nextIndex[server] = reply.XIndex
		}
	}

	// }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	reply.Success = true
	DPrintf("[AppendEntries] [me: %v, rf.lastIncludedIndex: %v, args.PrevLogIndex: %v, rf.LastLog().Index: %v, len(rf.log): %v]\n", rf.me, rf.lastIncludedIndex, args.PrevLogIndex, rf.LastLog().Index, len(rf.log))
	// term < currentTerm, reject.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm { // Turn to follower if find a leader.
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}

	rf.ElectTimerReset()
	rf.toFollower()

	// 日志不一致，返回不成功 AppendEntries RPC implementation 2.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.Success = false
			reply.XTerm = -1
			reply.XLen = rf.lastIncludedIndex
			return
		}
	} else if args.PrevLogIndex > rf.LastLog().Index {
		reply.Success = false
		reply.XTerm = -1
		reply.XLen = rf.LastLog().Index
		return
	} else if rf.FindLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.FindLog(args.PrevLogIndex).Term
		for i := rf.lastIncludedIndex + 1; i <= args.PrevLogIndex; i++ {
			if rf.FindLog(i).Term == reply.XTerm {
				reply.XIndex = i
				break
			}
		}
		return
	}

	DPrintf("[AppendEntries] [me: %v, rf.log: %v, logEntry: %v, args.PreLogIndex: %v]\n", rf.me, rf.log, args.Entries, args.PrevLogIndex)

	// AppendEntries RPC implementation 3. 4.
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index <= rf.lastIncludedIndex {
			continue
		} else if index > rf.LastLog().Index {
			rf.log = append(rf.log, logEntry)
		} else if rf.FindLog(index).Term != logEntry.Term {
			rf.log = append(rf.log[:(index-rf.lastIncludedIndex-1)], logEntry)
		}
	}

	DPrintf("[AppendEntries] [me: %v, LeaderCommit: %v, commitIndex: %v, rf.log: %v]\n", rf.me, args.LeaderCommit, rf.commitIndex, rf.log)

	// AppendEntries RPC implementation 5.
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, rf.LastLog().Index)
		if rf.commitIndex > commitIndex {
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
}

// updateCommitIndex 5.3 启动协程，每次日志复制时都更新Index，等大多数日志的matchIndex[i] >= N 时，CommitIndex + 1
func (rf *Raft) updateCommitIndex() {
	for !rf.killed() {
		time.Sleep(time.Duration(5) * time.Millisecond)
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		for i := rf.LastLog().Index; i > rf.commitIndex; i-- {
			num := 0
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= i && rf.currentTerm == rf.FindLog(i).Term {
					num++
				}
			}
			// 算上自己，超过半数
			if num >= len(rf.peers)/2 {
				DPrintf("[UpdateCommitIndex] [me: %v, commitIndex: %v]", rf.me, i)
				rf.commitIndex = i
				rf.applyCond.Broadcast()
				break
			}
		}
		rf.mu.Unlock()
	}
}
