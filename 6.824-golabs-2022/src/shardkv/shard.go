package shardkv

import (
	"6.824/shardctrler"
	"github.com/mohae/deepcopy"
)

func (kv *ShardKV) ShardPull(args ShardPullArgs, reply ShardPullReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply = ShardPullReply{
		Shard: args.Shard,
		Num:   args.Num,
		Data:  deepcopy.Copy(kv.needSendShards[args.Num][args.Shard]).(map[string]string),
		Err:   OK,
	}
}

func (kv *ShardKV) replicateShard(cmd ShardReplicationCommand) {
	if _, ok := kv.needPullShards[cmd.Shard]; !ok {
		return
	}
	delete(kv.needPullShards, cmd.Shard)

}

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
					delete(data, k)
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
