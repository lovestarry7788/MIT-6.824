package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	data              []byte
}

type InstallSnapshotReply struct {
	term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	defer func() {
		reply.term = rf.currentTerm
	}()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {

	}
}
