package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 快照过期，安装快照失败，即使没有更新到状态机，协程也在尝试更新，没有必要再同步
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("%v CondInstallSnapshot fault\n", rf.me)
		return false
	}

	// 快照比最后一个还大
	if lastIncludedIndex > rf.LastLog().Index {
		rf.log = make([]LogEntry, 0)
	} else if lastIncludedIndex > rf.lastIncludedIndex { // 否则一定在log中出现，因为前面有 lastIncludedIndex <= rf.commitIndex
		if len(rf.log) > 0 {
			realIndex := lastIncludedIndex - rf.FirstLog().Index + 1
			rf.log = rf.log[realIndex:]
		}
	}
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.snapshot = snapshot
	rf.persistSaveStateAndSnapshot()
	DPrintf("[CondInstallSnapshot] [me: %v, rf.lastIncludedIndex: %v]\n", rf.me, rf.lastIncludedIndex)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 把 index（包含）之前都打包为快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		DPrintf("%v Cannot find index: %v log when Snapshot\n", rf.me, index)
		return
	} else if index > rf.lastIncludedIndex+len(rf.log) {
		DPrintf("Index %v is not within the scope of log\n", index)
		return
	}
	// 找到 index 在 log 的位置
	realIndex := index - rf.FirstLog().Index
	rf.lastIncludedTerm = rf.log[realIndex].Term
	rf.lastIncludedIndex = rf.log[realIndex].Index
	rf.log = rf.log[realIndex+1:]
	rf.snapshot = snapshot
	rf.persistSaveStateAndSnapshot()
	DPrintf("[Snapshot] [me: %v, index: %v, log: %v, snapshot: %v]\n", rf.me, index, rf.log, snapshot)
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
	DPrintf("[InstallSnapshot] [me: %v, args.Snapshot: %v]\n", rf.me, args.Data)
	// term < currentTerm, reject.
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm { // Turn to follower if find a leader.
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	rf.ElectTimerReset()
	rf.toFollower()
	if args.LastIncludedIndex <= rf.commitIndex { // 过期的快照，[rf.lastApplied, rf.commitIndex) 部分已经起协程在更新，直接不处理
		return
	}

	// CondInstallSnapShot 应用之后通知 Raft 去更新，所以这里不更新
	// rf.commitIndex = args.LastIncludedIndex
	// rf.lastApplied = args.LastIncludedIndex

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
