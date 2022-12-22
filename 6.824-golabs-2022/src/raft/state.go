package raft

import (
	"time"
)

type State int

const (
	Follower  State = 0
	Candidate State = 1
	Leader    State = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == Leader)
}

func (rf *Raft) toFollower() {
	DPrintf("[me: %v turn to Follower]\n", rf.me)
	rf.state = Follower
}

func (rf *Raft) toCandidate() {
	DPrintf("[me: %v turn to Candidate]\n", rf.me)
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[me: %v turn to Leader]\n", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.Start(nil)
	go rf.updateCommitIndex()
	// 立即开始心跳
	rf.HeartBeatTimer.Reset(time.Duration(0))
}
