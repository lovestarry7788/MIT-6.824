package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
// tester
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	// 快照过期，安装快照失败，即使没有更新到状态机，协程也在尝试更新，没有必要再同步
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("%v CondInstallSnapshot fault\n", rf.me)
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 快照比最后一个还大
	if lastIncludedIndex > rf.LastLog().Index {
		rf.log = make([]LogEntry, 0)
	} else { // 否则一定在log中出现，因为前面有 lastIncludedIndex <= rf.commitIndex
		realIndex := lastIncludedIndex - rf.FirstLog().Index
		rf.log = rf.log[realIndex:]
	}
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.snapshot = snapshot
	rf.persistSaveStateAndSnapshot()

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 把 index（包含）都打包为快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if index <= rf.lastIncludedIndex {
		DPrintf("%v Cannot find index: %v log when Snapshot", rf.me, index)
		return
	}
	rf.mu.Lock()
	// 找到 index 在 log 的位置
	realIndex := index - rf.FirstLog().Index - 1
	rf.lastIncludedTerm = rf.log[realIndex].Term
	rf.lastIncludedIndex = rf.log[realIndex].Index
	rf.log = rf.log[realIndex+1:]
	rf.snapshot = snapshot
	rf.persistSaveStateAndSnapshot()
	DPrintf("[Snapshot] [index: %v, readlIndex: %v, lastIncludedTerm: %v, lastIncludedIndex: %v, log: %v]\n", index, realIndex, rf.lastIncludedTerm, rf.lastIncludedIndex, rf.log)
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 使用快照来进行 AppendEntries
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	DPrintf("[InstallSnapshot] [args.PrevLogTerm: %v, args.PrevLogIndex: %v, args.Term: %v, rf.currentTerm: %v]\n", args.LastIncludedTerm, args.LastIncludedIndex, args.Term, rf.currentTerm)
	// term < currentTerm, reject.
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm { // Turn to follower if find a leader.
		rf.votedFor = -1
		args.Term = rf.currentTerm
	}
	rf.ElectTimerReset()
	rf.toFollower()
	if args.LastIncludedIndex <= rf.commitIndex { // 过期的快照，[rf.lastApplied, rf.commitIndex) 部分已经起协程在更新，直接不处理
		return
	}

	// CondInstallSnapShot 应用之后通知 Raft 去更新，所以这里不更新
	// rf.commitIndex = args.LastIncludedIndex
	// rf.lastApplied = args.LastIncludedIndex

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}
