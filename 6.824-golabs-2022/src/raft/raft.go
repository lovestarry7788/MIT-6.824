package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state 锁
	peers     []*labrpc.ClientEnd // RPC end points of all peers 同伴
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[] 自己的编号
	dead      int32               // set by Kill()
	snapshot  []byte

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers: 持久化的状态，落盘，可恢复
	currentTerm int        // 服务器已知最新的任期
	votedFor    int        // 当前任期内收到选票的候选者id
	log         []LogEntry // 日志

	// Volatile state on all servers: 存在内存中，不落盘，用心跳等方式去更新。
	commitIndex int // 被复制到半数以上的节点将标记为 Commit 状态
	lastApplied int // 被提交到状态机上的日志为 Applied 状态，Applied <= Commit

	// Volatile state on leaders: 可丢失的状态
	nextIndex  []int // 下一个log 的 index
	matchIndex []int // 已复制的最大的 log entry index

	state State // 状态

	ElectTimer     *time.Timer
	HeartBeatTimer *time.Timer

	applyCh chan ApplyMsg

	// 条件变量
	applyCond *sync.Cond

	// 上一次快照信息
	lastIncludeIndex int
	lastIncludeTerm  int
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 把 index（包含）都打包为快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if index < rf.FirstLog().Index {
		DPrintf("%v Cannot find index: %v log when Snapshot", rf.me, index)
		return
	}
	rf.mu.Lock()
	// 找到 index 在 log 的位置
	realIndex := index - rf.FirstLog().Index
	rf.lastIncludeTerm = rf.log[realIndex].Term
	rf.lastIncludeIndex = rf.log[realIndex].Index
	rf.snapshot = snapshot
	rf.persistSaveStateAndSnapshot()
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人的任期
	CandidateId  int // 请求投票的候选人ID
	LastLogIndex int // 候选人的最近一条日志索引
	LastLogTerm  int // 候选人的最近一条日志的所属任期
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer func() { // 返回当前节点更新后的任期
		reply.Term = rf.currentTerm
	}()
	reply.VoteGranted = false
	// 1. 如果一个节点接受到一个任期较小的请求，直接拒绝 || 已经投过票
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		return
	}

	// 2. 如果一个节点接受到一个任期较大的请求，赋值并转换为 Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
	}

	// 3. 日志更完整（任期或序号更大）
	if args.LastLogTerm > rf.LastLog().Term || (args.LastLogTerm == rf.LastLog().Term && args.LastLogIndex >= rf.LastLog().Index) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := (rf.state == Leader)
	// Your code here (2B).
	if isLeader {
		rf.mu.Lock()
		index = len(rf.log)
		term = rf.currentTerm
		logEntry := LogEntry{
			Command: command,
			Index:   index,
			Term:    term,
		}
		rf.log = append(rf.log, logEntry)
		DPrintf("%v start append log %v success!\n", rf.me, logEntry)
		rf.persist()
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("%v is Killed", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartElection() {
	defer rf.persist()
	DPrintf("%v Start Election!\n", rf.me)
	rf.toCandidate()
	rf.currentTerm = rf.currentTerm + 1 // 新一轮选举开始，任期+1
	voteNumber := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.LastLog().Index,
			LastLogTerm:  rf.LastLog().Term,
		}
		reply := &RequestVoteReply{} // 返回询问机器的term 和 投票情况

		go func(i int) { // 并发发出投票
			ok := rf.sendRequestVote(i, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm { // 5.2.3 发现大于任期
					rf.currentTerm = reply.Term
					rf.toFollower()
					rf.votedFor = -1
				} else if reply.Term == rf.currentTerm && reply.VoteGranted {
					voteNumber++
					if voteNumber > len(rf.peers)/2 {
						rf.toLeader()
					}
				}
			}
		}(i)
	}
}

// 心跳检测
func (rf *Raft) BroadcastHeartBeat() {
	if rf.state == Leader {
		DPrintf("%v broadcast HeartBeat!", rf.me)
		for i := range rf.peers {
			if i != rf.me {
				go rf.HandleAppendEntries(i)
			}
		}
	}
}

// ResetTimer
func (rf *Raft) ElectTimerReset() { // 150 ms - 300 ms 之间
	time := time.Duration(300+rand.Intn(1000)) * time.Millisecond
	DPrintf("%v ElectTimer reset to %v\n", rf.me, time)
	rf.ElectTimer.Reset(time)
}

func (rf *Raft) HeartBeatTimerReset() { // 要比选举超时的时间要短
	time := time.Duration(150) * time.Millisecond
	DPrintf("%v HeartBeatTimer reset to %v\n", rf.me, time)
	rf.HeartBeatTimer.Reset(time)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.ElectTimer.C: // 选举超时
			rf.mu.Lock()
			if rf.state != Leader {
				rf.StartElection()
				rf.ElectTimerReset()
			}
			rf.mu.Unlock()
		case <-rf.HeartBeatTimer.C: // 心跳检测
			if rf.state == Leader {
				rf.BroadcastHeartBeat()
				rf.HeartBeatTimerReset()
			}
		}
	}
}

func (rf *Raft) applyToStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex == rf.lastApplied {
			rf.applyCond.Wait()
		}
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.mu.Unlock()
		for i := lastApplied + 1; i <= commitIndex; i++ {
			applyMsg := ApplyMsg{
				Command:      rf.log[i].Command,
				CommandIndex: i,
				CommandValid: true,
			}
			rf.applyCh <- applyMsg
		}
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		DPrintf("[%v Applied Log %v Success, commitIndex: %v, lastApplied: %v]\n", rf.me, lastApplied, rf.commitIndex, rf.lastApplied)
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:            peers,
		persister:        persister,
		me:               me,
		currentTerm:      0,
		votedFor:         -1,
		log:              make([]LogEntry, 0),
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		state:            Follower,
		ElectTimer:       time.NewTimer(time.Duration(150) * time.Millisecond),
		HeartBeatTimer:   time.NewTimer(time.Duration(150+rand.Float64()*150) * time.Millisecond),
		applyCh:          applyCh,
		lastIncludeIndex: -1,
		lastIncludeTerm:  -1,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyToStateMachine()

	return rf
}
