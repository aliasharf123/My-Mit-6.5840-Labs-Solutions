package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	mu   sync.Mutex
	// You will have to modify this struct.
	group2clerk map[tester.Tgid]*shardgrp.Clerk // group → group RPC client
	config      *shardcfg.ShardConfig
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt:        clnt,
		sck:         sck,
		group2clerk: make(map[tester.Tgid]*shardgrp.Clerk),
		config:      sck.Query(), // initial config
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	for {
		clerk := ck.getClerkForShard(shard)
		value, version, err := clerk.Get(key)

		if err == rpc.ErrWrongGroup {
			ck.refreshConfig()
			continue
		}

		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)

	for {
		clerk := ck.getClerkForShard(shard)
		err := clerk.Put(key, value, version)

		if err == rpc.ErrWrongGroup {
			ck.refreshConfig()
			continue
		}
		return err
	}
}

func (ck *Clerk) refreshConfig() {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	newConfig := ck.sck.Query()

	shardgrp.DPrintf("[refreshConfig]: currentConfig=(%+v)", ck.config)
	if newConfig.Num == ck.config.Num {
		return
	}
	shardgrp.DPrintf("[refreshConfig]: THE CONFIG WAS CHANCED")

	newGroup2Clerk := make(map[tester.Tgid]*shardgrp.Clerk)

	// Reuse clerks for groups that still exist
	for gid := range newConfig.Groups {
		if oldClerk, ok := ck.group2clerk[gid]; ok {
			newGroup2Clerk[gid] = oldClerk
		}
	}

	ck.group2clerk = newGroup2Clerk
	ck.config = newConfig
}
func (ck *Clerk) getClerkForShard(shard shardcfg.Tshid) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	gid, servers, _ := ck.config.GidServers(shard)

	// if exists, reuse it
	if c, ok := ck.group2clerk[gid]; ok {
		return c
	}

	// create a new client for this shard group
	newClerk := shardgrp.MakeClerk(ck.clnt, servers)
	ck.group2clerk[gid] = newClerk
	return newClerk
}
