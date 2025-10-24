package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu sync.Mutex // Protects configuration updates
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	config := cfg.String()
	shardgrp.DPrintf("[InitConfig]: %+v", cfg)
	sck.IKVClerk.Put("Config", config, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	value, version, err := sck.IKVClerk.Get("Config")
	shardgrp.DPrintf("[Query]: (%+v)", new)
	if err != rpc.OK {
		return
	}

	oldCfg := shardcfg.FromString(value)

	for shard, newG := range new.Shards {
		oldG := oldCfg.Shards[shard]
		if oldG != newG {
			shardId := shardcfg.Tshid(shard)
			_, srvs, _ := oldCfg.GidServers(shardId)
			clnt := shardgrp.MakeClerk(sck.clnt, srvs)
			data, _ := clnt.FreezeShard(shardId, new.Num)

			{
				_, srvs, _ := new.GidServers(shardId)
				clnt := shardgrp.MakeClerk(sck.clnt, srvs)
				clnt.InstallShard(shardId, data, new.Num)
			}

			clnt.DeleteShard(shardId, new.Num)
		}
	}
	sck.IKVClerk.Put("Config", new.String(), version)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, _, _ := sck.IKVClerk.Get("Config")
	shCfg := shardcfg.FromString(value)
	return shCfg
}
