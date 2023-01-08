package shardkv

import (
	"6.824/labgob"
	"6.824/raft"
	"bytes"
)

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && (float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) > 0.9)
}

func (kv *ShardKV) createSnapshot(CommandIndex int) {
	DPrintf("[handleCommand] [kv.rf.GetRaftStateSize: %v, kv.maxraftstate: %v]\n", kv.rf.GetRaftStateSize(), kv.maxraftstate)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.cmd)
	Snapshot := w.Bytes()
	go kv.rf.Snapshot(CommandIndex, Snapshot)
	DPrintf("[handleCommand] [me: %v, Create snapshot success!]\n", kv.me)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.cmd)
}

func (kv *ShardKV) handleSnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	if msg.SnapshotIndex <= kv.lastApplied {
		kv.mu.Unlock()
		return
	}
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.lastApplied = msg.SnapshotIndex
		kv.readSnapshot(msg.Snapshot)
	}
	kv.mu.Unlock()
}
