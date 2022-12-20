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
	DPrintf("%v turn to Follower!\n", rf.me)
	rf.state = Follower
}

func (rf *Raft) toCandidate() {
	DPrintf("%v turn to Candidate!\n", rf.me)
	rf.state = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
}

func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%v turn to Leader!\n", rf.me)
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = 0
	}
	// go rf.Start(nil)
	go rf.updateCommitIndex()
	// 立即开始心跳
	rf.HeartBeatTimer.Reset(time.Duration(0))
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
