package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Vav struct {
	Value   string
	Version rpc.Tversion
}
type Cache struct {
	Seq int64
	Req any
}

type ShardStatus int

const (
	ShardNotOwned ShardStatus = iota // shard not currently owned by this group
	ShardOwned                       // shard is owned and active
	ShardFrozen                      // shard is frozen during migration
)

type ShardData struct {
	KvStore   map[string]*Vav
	Cache     map[string]*Cache
	ShardOps  *ShardOperationState
	Status    ShardStatus
	ConfigNum shardcfg.Tnum
}
type ShardOperationState struct {
	FreezeData  *shardrpc.FreezeShardReply
	InstallData *shardrpc.InstallShardReply
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu sync.Mutex

	// Your definitions here.
	shards [shardcfg.NShards]*ShardData
}

func getShardID(req any) shardcfg.Tshid {
	switch r := req.(type) {
	case rpc.IArgs:
		return shardcfg.Key2Shard(r.GetKey())
	case *shardrpc.FreezeShardArgs:
		return r.Shard
	case *shardrpc.InstallShardArgs:
		return r.Shard
	case *shardrpc.DeleteShardArgs:
		return r.Shard
	default:
		return -1
	}
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[DoOp]: (%+v) {%d}", req, kv.me)

	// Extract ClientId & Seq & Key from the request
	clientId, seq := req.(rpc.ClientMetaAccessor).GetClientMeta()
	shardId := getShardID(req)
	if cached, ok := kv.isCacheHit(clientId, seq, shardId); ok {
		return cached.Req
	}
	// Apply operation
	var reply any
	switch r := req.(type) {
	case *rpc.GetArgs:
		reply = kv.doGet(r)
	case *rpc.PutArgs:
		reply = kv.doPut(r)
	case *shardrpc.InstallShardArgs:
		reply = kv.doInstallShard(r)
	case *shardrpc.DeleteShardArgs:
		reply = kv.doDeleteShard(r)
	case *shardrpc.FreezeShardArgs:
		reply = kv.doFreezeShard(r)
	}

	// Save latest result for deduplication
	if shard := kv.shards[shardId]; shard.Cache != nil {
		kv.shards[shardId].Cache[clientId] = &Cache{
			Seq: seq,
			Req: reply,
		}
	}

	return reply
}
func (kv *KVServer) isCacheHit(cid string, seq int64, shardID shardcfg.Tshid) (*Cache, bool) {
	shard := kv.shards[shardID]
	if shard.Cache == nil {
		return nil, false
	}
	if cache, ok := shard.Cache[cid]; ok && seq <= cache.Seq {
		return cache, true
	}
	return nil, false
}

func (kv *KVServer) doGet(req *rpc.GetArgs) *rpc.GetReply {
	reply := &rpc.GetReply{}
	DPrintf("[doGet]: (%+v) s{%d}", req, kv.me)

	// Check if shard belongs to this group
	shard := kv.shards[shardcfg.Key2Shard(req.Key)]
	if shard.Status == ShardNotOwned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// Read from correct shard store
	val, ok := shard.KvStore[req.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = val.Value
		reply.Version = val.Version
		reply.Err = rpc.OK
	}
	return reply
}

func (kv *KVServer) doPut(req *rpc.PutArgs) *rpc.PutReply {
	reply := &rpc.PutReply{}
	DPrintf("[doGet]: (%+v) s{%d}", req, kv.me)
	// Check if shard belongs to this group
	shard := kv.shards[shardcfg.Key2Shard(req.Key)]
	if shard.Status != ShardOwned {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}

	// Normal Put logic but inside shard storage
	val, ok := shard.KvStore[req.Key]
	if !ok {
		if req.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}
	} else {
		if val.Version != req.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}
	}

	// Success write
	shard.KvStore[req.Key] = &Vav{
		Version: req.Version + 1,
		Value:   req.Value,
	}
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) *shardrpc.FreezeShardReply {

	shardID := args.Shard
	shard := kv.shards[shardID]

	if shard.ConfigNum > args.Num {
		return &shardrpc.FreezeShardReply{}
	} else if shard.ConfigNum == args.Num {
		if shard.ShardOps != nil && shard.ShardOps.FreezeData != nil {
			return shard.ShardOps.FreezeData
		}
	}
	if shard.ShardOps == nil {
		shard.ShardOps = &ShardOperationState{}
	}
	shard.Status = ShardFrozen
	reply := &shardrpc.FreezeShardReply{
		Num:   args.Num,
		Reply: rpc.Reply{Err: rpc.OK},
		State: kv.encodeShards(args.Shard),
	}
	shard.ConfigNum = args.Num
	shard.ShardOps.FreezeData = reply
	return reply
}

// Install the supplied state for the specified shard.
func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	shardID := args.Shard
	shard := kv.shards[shardID]
	reply := &shardrpc.InstallShardReply{}

	if shard.ConfigNum > args.Num {
		return reply
	} else if shard.ConfigNum == args.Num {
		if shard.ShardOps != nil && shard.ShardOps.InstallData != nil {
			return shard.ShardOps.InstallData
		}
	}
	if shard.ShardOps == nil {
		shard.ShardOps = &ShardOperationState{}
	}
	reply.Err = rpc.OK
	kv.shards[shardID] = kv.decodeShards(args.State)
	kv.shards[shardID].Status = ShardOwned
	kv.shards[shardID].ConfigNum = args.Num
	kv.shards[shardID].ShardOps.InstallData = reply
	return reply
}

// Delete the specified shard.
func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	shardID := args.Shard
	shard := kv.shards[shardID]
	reply := &shardrpc.DeleteShardReply{}

	if shard.ConfigNum > args.Num {
		return reply
	}

	kv.shards[args.Shard] = &ShardData{
		Status:    ShardNotOwned,
		KvStore:   nil,
		Cache:     nil,
		ShardOps:  nil,
		ConfigNum: args.Num,
	}
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) encodeShards(ShardId shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	shardCopy := kv.deepCopyShard(kv.shards[ShardId])
	// Encode shard map and current config
	if e.Encode(shardCopy) != nil {
		log.Fatalf("encodeShards: failed to encode shards, (%+v)", kv.shards[ShardId])
	}
	return w.Bytes()
}

// Decode shard data from snapshot and restore it
func (kv *KVServer) decodeShards(data []byte) *ShardData {
	if data == nil || len(data) < 1 {
		return nil
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var shard *ShardData
	if d.Decode(&shard) != nil {
		log.Fatalf("decodeShards: shard map decode error")
	}
	return shard
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.shards) != nil {
		log.Fatalf("Snapshot: failed to encode shards, (%+v)", kv.shards)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var shards [shardcfg.NShards]*ShardData

	if d.Decode(&shards) != nil {
		log.Fatal("Restore: Decode error")
		return
	}

	kv.shards = shards

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	DPrintf("[Get]: (%+v) {%d}", args, kv.me)

	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	DPrintf("[Server Get]: (%+v) s{%d}", res, kv.me)

	*reply = *res.(*rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, res := kv.rsm.Submit(args)
	DPrintf("[Server Put]: (%+v) s{%d}", res, kv.me)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	DPrintf("[FreezeShard]: (%d) {%d}", args.Num, kv.me)
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	DPrintf("[InstallShard]: (%d) {%d}", args.Num, kv.me)

	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*shardrpc.InstallShardReply)

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	DPrintf("[DeleteShard]: (%d) {%d}", args.Num, kv.me)

	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	labgob.Register(&shardrpc.FreezeShardArgs{})
	labgob.Register(&shardrpc.InstallShardArgs{})
	labgob.Register(&shardrpc.DeleteShardArgs{})
	labgob.Register(&shardrpc.FreezeShardReply{})
	labgob.Register(&shardrpc.InstallShardReply{})
	labgob.Register(&shardrpc.DeleteShardReply{})
	labgob.Register(&ShardOperationState{})
	labgob.Register(rsm.Op{})
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})
	labgob.Register(&rpc.PutReply{})
	labgob.Register(&rpc.GetReply{})
	labgob.Register(&Cache{})
	labgob.Register(&Vav{})
	labgob.Register(&ShardData{})
	kv := &KVServer{gid: gid, me: me,
		shards: [12]*ShardData{},
	}

	for sid := range shardcfg.NShards {
		kv.shards[sid] = &ShardData{
			KvStore:  make(map[string]*Vav),
			Cache:    make(map[string]*Cache),
			ShardOps: &ShardOperationState{},
		}
		if gid == 1 {
			kv.shards[sid].Status = ShardOwned
		} else {
			kv.shards[sid].Status = ShardNotOwned
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []tester.IService{kv, kv.rsm.Raft()}
}
