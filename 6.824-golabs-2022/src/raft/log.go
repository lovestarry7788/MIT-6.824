package raft

type LogEntry struct {
	Command interface{} // 业务方自定的命令，写请求之类
	Index   int         // 日志的序号，由领导者分配
	Term    int         // 写入命令的领导者处于的任期
}

type AppendEntriesArgs struct {
	Term         int        // Leader 任期号
	Leader       int        // Leader id 为了能帮助客户端重定向到 Leader 服务器
	PrevLogIndex int        // 前一条日志的索引值
	PrevLogTerm  int        // 前一条日志的任期值
	Entries      []LogEntry // 需要同步的日志条目
	LeaderCommit int        // leader 已提交的日志号
}

type AppendEntriesReply struct {
	PrevLogIndex int // 日志冲突时，冲突日志的 Index
	PreLogTerm   int // 日志冲突时，冲突日志的 Term
	Len          int
	Term         int
	Success      bool
}

func (rf *Raft) LastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) HandleAppendEntries(server int) {
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		Leader:       rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}

	// args.Entries = make([]LogEntry, )
	// copy()

	rf.mu.Unlock()
	// 并发发送请求
	ok := rf.sendAppendEntries(server, args, reply)

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
		if reply.Term == -1 {
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

	// term < currentTerm, reject.
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else { // Turn to follower if find a leader.
		rf.currentTerm = args.Term
		rf.toFollower()
	}

	rf.ElectTimerReset()

	// Reject if log doesn't contain a matching previous entry
	if ok := rf.FindEntry(args.PrevLogIndex, args.PrevLogTerm); !ok {
		reply.Success = false

		return
	}

}

func (rf *Raft) FindEntry(index, term int) bool {

}
