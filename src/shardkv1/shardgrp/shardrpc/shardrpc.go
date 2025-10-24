package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
	rpc.ClientMeta
}

type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	rpc.Reply
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
	rpc.ClientMeta
}

type InstallShardReply struct {
	rpc.Reply
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
	rpc.ClientMeta
}

type DeleteShardReply struct {
	rpc.Reply
}
