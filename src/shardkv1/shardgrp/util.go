package shardgrp

import (
	"log"

	"6.5840/kvsrv1/rpc"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}
func (kv *KVServer) deepCopyShard(shard *ShardData) *ShardData {
	if shard == nil {
		return nil
	}

	shardCopy := &ShardData{
		Status:    shard.Status,
		ConfigNum: shard.ConfigNum,
		KvStore:   nil,
		Cache:     nil,
	}

	// Deep copy KvStore map
	if shard.KvStore != nil {
		shardCopy.KvStore = make(map[string]*Vav, len(shard.KvStore))
		for key, vav := range shard.KvStore {
			shardCopy.KvStore[key] = &Vav{
				Value:   vav.Value,   // string is immutable, safe
				Version: vav.Version, // int is copied by value
			}
		}
	}

	// Deep copy Cache map
	if shard.Cache != nil {
		shardCopy.Cache = make(map[string]*Cache, len(shard.Cache))
		for clientId, cache := range shard.Cache {
			shardCopy.Cache[clientId] = &Cache{
				Seq: cache.Seq,
				Req: deepCopyReq(cache.Req), // Need to copy the request too
			}
		}
	}

	return shardCopy
}
func deepCopyReq(req any) any {
	if req == nil {
		return nil
	}

	switch r := req.(type) {
	case *rpc.GetReply:
		return &rpc.GetReply{
			Value:   r.Value,
			Version: r.Version,
			Reply:   rpc.Reply{Err: r.Err},
		}
	case *rpc.PutReply:
		return &rpc.PutReply{
			Reply: rpc.Reply{Err: r.Err},
		}
	default:
		// If you have other reply types, add them here
		return req
	}
}
