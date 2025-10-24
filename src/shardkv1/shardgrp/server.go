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

type ShardData struct {
	KvStore  map[string]*Vav
	Cache    map[string]*Cache
	IsFrozen bool
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu sync.Mutex

	// Your definitions here.
	shards           map[shardcfg.Tshid]*ShardData
	currentConfigNum shardcfg.Tnum
}

func (kv *KVServer) isWrongGroup(key string) (*ShardData, bool) {
	shardID := shardcfg.Key2Shard(key)

	shard, ok := kv.shards[shardID]
	if !ok || shard == nil {
		// This shard does not exist here, so wrong group
		return shard, true
	}
	return shard, false
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
	shard := kv.shards[shardId]
	if shard != nil {
		kv.shards[shardId].Cache[clientId] = &Cache{
			Seq: seq,
			Req: reply,
		}
	}

	return reply
}
func (kv *KVServer) isCacheHit(cid string, seq int64, shardID shardcfg.Tshid) (*Cache, bool) {
	shard := kv.shards[shardID]
	if shard == nil {
		return nil, false
	}
	if cache, ok := shard.Cache[cid]; ok && seq <= cache.Seq {
		return cache, true
	}
	return nil, false
}

func (kv *KVServer) doGet(req *rpc.GetArgs) *rpc.GetReply {
	reply := &rpc.GetReply{}

	// Check if shard belongs to this group
	shard, wrong := kv.isWrongGroup(req.Key)
	if wrong {
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
	// Check if shard belongs to this group
	shard, wrong := kv.isWrongGroup(req.Key)
	if wrong || shard.IsFrozen {
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
	reply := &shardrpc.FreezeShardReply{}
	if kv.currentConfigNum > args.Num {
		reply.Err = rpc.ErrVersion
		reply.Num = kv.currentConfigNum
		return reply
	}
	kv.currentConfigNum = args.Num
	reply.Num = args.Num
	reply.Err = rpc.OK
	reply.State = kv.encodeShards(args.Shard)
	kv.shards[args.Shard].IsFrozen = true
	return reply
}

// Install the supplied state for the specified shard.
func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) *shardrpc.InstallShardReply {
	reply := &shardrpc.InstallShardReply{}
	if kv.currentConfigNum > args.Num {
		reply.Err = rpc.ErrVersion
		return reply
	}
	kv.currentConfigNum = args.Num
	reply.Err = rpc.OK
	kv.shards[args.Shard] = kv.decodeShards(args.State)
	return reply
}

// Delete the specified shard.
func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) *shardrpc.DeleteShardReply {
	reply := &shardrpc.DeleteShardReply{}
	if kv.currentConfigNum > args.Num {
		reply.Err = rpc.ErrVersion
		return reply
	}
	kv.currentConfigNum = args.Num
	reply.Err = rpc.OK
	delete(kv.shards, args.Shard)
	return reply
}

func (kv *KVServer) encodeShards(ShardId shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// Encode shard map and current config
	if e.Encode(kv.shards[ShardId]) != nil {
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

	e.Encode(kv.shards)
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

	var shards map[shardcfg.Tshid]*ShardData
	if d.Decode(&shards) != nil {
		log.Fatal("Decode error")
		return
	}

	kv.shards = shards
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := kv.rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	*reply = *res.(*shardrpc.InstallShardReply)

}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
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
	labgob.Register(rsm.Op{})
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})
	labgob.Register(&rpc.PutReply{})
	labgob.Register(&rpc.GetReply{})
	labgob.Register(&Cache{})
	labgob.Register(&Vav{})
	labgob.Register(&ShardData{})
	kv := &KVServer{gid: gid, me: me, shards: make(map[shardcfg.Tshid]*ShardData)}

	if gid == 1 {
		for sid := range shardcfg.NShards {
			kv.shards[shardcfg.Tshid(sid)] = &ShardData{
				KvStore:  make(map[string]*Vav),
				Cache:    make(map[string]*Cache),
				IsFrozen: false,
			}
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
