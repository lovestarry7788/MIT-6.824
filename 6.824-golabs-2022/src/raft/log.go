package raft

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
	// PrevLogIndex int // 日志冲突时，冲突日志的 Index
	// PreLogTerm   int // 日志冲突时，冲突日志的 Term
	// Len          int
	Term    int
	Success bool
}

func (rf *Raft) FirstLog() LogEntry {
	return rf.log[0]
}

func (rf *Raft) LastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleAppendEntries(server int) {
	defer rf.persist()
	// 要复制的Index日志已经在快照中 / 可理解为 server 落后太多，raft采用 快照+持久化 的方式来恢复
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Lock()
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			data:              rf.snapshot,
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
			return
		}
		rf.matchIndex[server] = rf.lastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		return
	}

	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leader:       rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		Entries:      rf.log[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex,
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
	DPrintf("log replicated %v from %v to %v, status: %v\n", args.Entries, rf.me, server, reply.Success)

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
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else { // 日志不一致，更新 nextIndex
		rf.nextIndex[server] = Max(1, rf.nextIndex[server]-1)
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

	DPrintf("[AppendEntries] [args.PrevLogTerm: %v, args.PrevLogIndex: %v, args.Term: %v, rf.currentTerm: %v]\n", args.PrevLogTerm, args.PrevLogIndex, args.Term, rf.currentTerm)
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
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// AppendEntries RPC implementation 3. 4.
	i := 0
	j := args.PrevLogIndex + 1
	for i = 0; i < len(args.Entries); i++ {
		if j >= len(rf.log) {
			break
		} else if rf.log[j].Term == args.Entries[i].Term { // term 和 index 都相同
			j++
		} else {
			rf.log = append(rf.log[:j], args.Entries[i:]...)
			DPrintf("[log replicated success] [Entries: %v, me: %v]\n", args.Entries[i:], rf.me)
			i = len(args.Entries)
			j = len(rf.log) - 1
			break
		}
	}

	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		DPrintf("[log replicated success] [Entries: %v, me: %v]\n", args.Entries[i:], rf.me)
		j = len(rf.log) - 1
	} else {
		j-- // commitIndex
	}

	DPrintf("me: %v, LeaderCommit: %v, commitIndex: %v, j: %v\n", rf.me, args.LeaderCommit, rf.commitIndex, j)
	// AppendEntries RPC implementation 5.
	if args.LeaderCommit > rf.commitIndex {
		commitIndex := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, j)
		if rf.commitIndex > commitIndex {
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
}
