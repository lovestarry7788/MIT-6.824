package shardkv

import (
	"6.824/shardctrler"
	"github.com/mohae/deepcopy"
	"time"
)

/*
ConfigureAction:
拉取到新的配置，更新配置，并构造出需要发出的分片与需要拉取的分片。
*/
func (kv *ShardKV) updateConfig(config shardctrler.Config) {
	if config.Num <= kv.config.Num {
		return
	}
	oldConfig := kv.config
	needSendShards := kv.shardsAcceptable
	kv.config = config
	kv.shardsAcceptable = make(map[int]bool)
	for shard, gId := range config.Shards {
		if gId != kv.gid {
			continue
		}
		if _, ok := needSendShards[shard]; ok || oldConfig.Num == 0 { // 这个分片在之前的 group 中也出现。
			kv.shardsAcceptable[shard] = true
			delete(needSendShards, shard) // 不需要发送出去
		} else { // 否则需要从别的配置中拉取当前的分片
			kv.needPullShards[shard] = oldConfig.Num
		}
	}

	if len(needSendShards) > 0 {
		for shard := range needSendShards {
			data := make(map[string]string)
			for k, v := range kv.data {
				if key2shard(k) == shard { // 一个group管理很多个分片，需要发送的分片的数据提出来
					data[k] = v
					delete(kv.data, k)
				}
			}

			if shardData, ok := kv.needSendShards[oldConfig.Num]; !ok {
				shardData := make(map[int]map[string]string)
				shardData[shard] = data
				kv.needSendShards[oldConfig.Num] = shardData
			} else {
				shardData[shard] = data
			}
		}
	}
}

/*
MigrationAction:
1. 轮询 needPullShards，拉取分片，并进行复制。
*/
func (kv *ShardKV) ShardPull(args ShardPullArgs, reply ShardPullReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if args.Num >= kv.config.Num {
		reply.Err = ErrServerNotUpdate
		return
	}

	reply = ShardPullReply{
		Data: deepcopy.Copy(kv.needSendShards[args.Num][args.Shard]).(map[string]string),
		Err:  OK,
	}
}

func (kv *ShardKV) ShardGc(args ShardGcArgs, reply ShardGcReply) {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if _, ok := kv.needSendShards[args.Num][args.Shard]; !ok {
		reply.Err = OK
		return
	}
	cmd := GcCommand{Shard: args.Shard, Num: args.Num}
	index, term, isLeader := kv.rf.Start(cmd)
	ch := make(chan CommonReply, 1)
	it := IndexAndTerm{Index: index, Term: term}
	kv.replyCh[it] = ch
	kv.mu.Unlock()
	select {
	case replyMsg := <-ch:
		reply.Err = replyMsg.Err
	case <-time.After(replyTimeOut):
		reply.Err = ErrTimeOut
	}
	go kv.CloseChannel(it)
}

func (kv *ShardKV) replicateShard(cmd ShardReplicationCommand) {
	if _, ok := kv.needPullShards[cmd.Shard]; !ok {
		return
	}
	// 删除需要拉取的 shard
	delete(kv.needPullShards, cmd.Shard)
	if _, ok := kv.shardsAcceptable[cmd.Shard]; !ok {
		for k, v := range cmd.Data {
			kv.data[k] = v
		}
		kv.shardsAcceptable[cmd.Shard] = true
		kv.gcList[cmd.Shard] = cmd.Num
	}
}
