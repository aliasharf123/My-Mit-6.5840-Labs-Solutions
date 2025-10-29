package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	FINISHED = "Finished"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu sync.Mutex
	// You will have to modify this struct.
	group2clerk map[tester.Tgid]*shardgrp.Clerk // group → group RPC client
}

func (sck *ShardCtrler) getClerkForShard(gid tester.Tgid, config *shardcfg.ShardConfig) *shardgrp.Clerk {
	sck.mu.Lock()
	defer sck.mu.Unlock()

	// if exists, reuse it
	if c, ok := sck.group2clerk[gid]; ok {
		return c
	}

	// create a new client for this shard group
	servers := config.Groups[gid]
	newClerk := shardgrp.MakeClerk(sck.clnt, servers)
	sck.group2clerk[gid] = newClerk
	return newClerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt, group2clerk: make(map[tester.Tgid]*shardgrp.Clerk)}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	value, _, _ := sck.IKVClerk.Get("Config")
	config := shardcfg.FromString(value)
	shardgrp.DPrintf("[InitController]: %+v", config)
	{
		key := "NextConfig" + strconv.Itoa(int(config.Num)+1)
		value, _, err := sck.IKVClerk.Get(key)
		if err != rpc.ErrNoKey && value != FINISHED {
			sck.ChangeConfigTo(shardcfg.FromString(value))
		}
	}
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
// configuration from the current o ne to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	shardgrp.DPrintf("[ChangeConfigTo]: (%+v)", new)
	value, version, _ := sck.IKVClerk.Get("Config")
	oldCfg := shardcfg.FromString(value)
	if new.Num < oldCfg.Num {
		return
	}
	acquire, newConfig := sck.acquire(new)
	if !acquire {
		return
	}

	frozenShards := make(map[shardcfg.Tshid][]byte)
	for shard, newG := range newConfig.Shards {
		oldG := oldCfg.Shards[shard]
		shardId := shardcfg.Tshid(shard)

		if oldG != newG {
			data, isFinished := sck.requestFreezeShard(oldG, oldCfg, shardId, newConfig)
			if isFinished {
				return
			}
			frozenShards[shardId] = data
		}
	}

	for shardId, data := range frozenShards {
		isFinished := sck.requestInstallShard(shardId, newConfig, data)
		if isFinished {
			return
		}
	}

	for shardId := range frozenShards {
		if isFinished := sck.requestDeleteShard(oldCfg, shardId, newConfig); isFinished {
			return
		}
	}

	sck.IKVClerk.Put("Config", newConfig.String(), version)
	sck.finished(newConfig)
	shardgrp.DPrintf("[Finished ChangeConfigTo]: (%+v)", newConfig.Num)

}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	value, _, _ := sck.IKVClerk.Get("Config")
	shCfg := shardcfg.FromString(value)
	return shCfg
}

func (sck *ShardCtrler) acquire(new *shardcfg.ShardConfig) (bool, *shardcfg.ShardConfig) {
	key := "NextConfig" + strconv.Itoa(int(new.Num))

	err := sck.IKVClerk.Put(key, new.String(), 0)
	if err == rpc.OK {
		return true, new
	}
	start := time.Now() // Start timer
	ms := 3200 + (rand.Int63() % 1000)
	for {
		// loop until the key's released
		value, version, err := sck.IKVClerk.Get(key)
		if err == rpc.OK {
			if value == FINISHED {
				return false, nil
			} else if time.Since(start) > time.Duration(ms) {
				err := sck.IKVClerk.Put(key, value, version)
				if err == rpc.OK {
					return true, shardcfg.FromString(value)
				}
				start = time.Now()
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (sck *ShardCtrler) finished(new *shardcfg.ShardConfig) {
	key := "NextConfig" + strconv.Itoa(int(new.Num))
	_, version, _ := sck.IKVClerk.Get(key)
	sck.IKVClerk.Put(key, FINISHED, version)
}

func (sck *ShardCtrler) requestFreezeShard(oldG tester.Tgid,
	oldCfg *shardcfg.ShardConfig,
	shardId shardcfg.Tshid,
	new *shardcfg.ShardConfig) ([]byte, bool) {

	clnt := sck.getClerkForShard(oldG, oldCfg)
	for {
		data, err := clnt.FreezeShard(shardId, new.Num)

		if err == rpc.ErrMaybe {
			value, _, _ := sck.IKVClerk.Get("NextConfig" + strconv.Itoa(int(new.Num)))
			if value == FINISHED {
				return nil, true
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return data, false
	}
}
func (sck *ShardCtrler) requestInstallShard(shardId shardcfg.Tshid,
	new *shardcfg.ShardConfig,
	data []byte) bool {
	newG := new.Shards[shardId]
	clnt := sck.getClerkForShard(newG, new)

	for {
		err := clnt.InstallShard(shardId, data, new.Num)
		if err == rpc.ErrMaybe {
			value, _, _ := sck.IKVClerk.Get("NextConfig" + strconv.Itoa(int(new.Num)))
			if value == FINISHED {
				return true
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return false
	}
}
func (sck *ShardCtrler) requestDeleteShard(oldCfg *shardcfg.ShardConfig,
	shardId shardcfg.Tshid,
	new *shardcfg.ShardConfig) bool {
	oldG := oldCfg.Shards[shardId]
	var isFinished bool
	if _, ok := new.Groups[oldG]; ok {
		clnt := sck.getClerkForShard(oldG, oldCfg)

		for {
			err := clnt.DeleteShard(shardId, new.Num)
			if err == rpc.ErrMaybe {
				value, _, _ := sck.IKVClerk.Get("NextConfig" + strconv.Itoa(int(new.Num)))
				if value == FINISHED {
					isFinished = true
					break
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			isFinished = false
			break
		}
	} else {
		sck.mu.Lock()
		delete(sck.group2clerk, oldG)
		sck.mu.Unlock()
	}
	return isFinished
}
