package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
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

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	mu   sync.Mutex

	// Your definitions here.
	kvaStore    map[string]*Vav
	cachesStore map[string]*Cache // clientID → cached

}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Extract ClientId & Seq from the request
	clientId, seq := req.(rpc.ClientMetaAccessor).GetClientMeta()

	// Deduplication: ignore duplicate or old commands
	if cached, ok := kv.isCacheHit(clientId, seq); ok {
		return cached.Req
	}

	// Apply operation
	var reply any
	switch r := req.(type) {
	case *rpc.GetArgs:
		reply = kv.doGet(r)
	case *rpc.PutArgs:
		reply = kv.doPut(r)
	}

	// Save latest result for deduplication
	kv.cachesStore[clientId] = &Cache{
		Seq: seq,
		Req: reply,
	}

	return reply
}
func (kv *KVServer) isCacheHit(cid string, seq int64) (*Cache, bool) {
	if cache, ok := kv.cachesStore[cid]; ok && seq <= cache.Seq {
		return cache, true
	}
	return nil, false
}
func (kv *KVServer) doGet(req *rpc.GetArgs) *rpc.GetReply {
	reply := &rpc.GetReply{}
	val, ok := kv.kvaStore[req.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = val.Value
		reply.Version = val.Version
		reply.Err = rpc.OK
	}
	return reply
}

// handle Put requests
func (kv *KVServer) doPut(req *rpc.PutArgs) *rpc.PutReply {
	reply := &rpc.PutReply{}
	val, ok := kv.kvaStore[req.Key]
	if !ok {
		if req.Version != 0 {
			// rsm.DPrintf("[DoPut]: the state of store in {%d}: (%+v)", kv.me, kv.kvaStore)
			reply.Err = rpc.ErrNoKey
			return reply
		}
	} else {
		if val.Version != req.Version {
			reply.Err = rpc.ErrVersion
			// rsm.DPrintf("[DoPut Err Version]: the val = (%+v)", val)
			return reply
		}
	}

	// success path
	kv.kvaStore[req.Key] = &Vav{
		Version: req.Version + 1,
		Value:   req.Value,
	}

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvaStore)
	e.Encode(kv.cachesStore)
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

	var kvaStore map[string]*Vav
	var cachesStore map[string]*Cache
	if d.Decode(&kvaStore) != nil || d.Decode(&cachesStore) != nil {
		log.Fatal("Decode error")
		return
	}

	kv.cachesStore = cachesStore
	kv.kvaStore = kvaStore
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(&rpc.PutArgs{})
	labgob.Register(&rpc.GetArgs{})
	labgob.Register(&rpc.PutReply{})
	labgob.Register(&rpc.GetReply{})
	labgob.Register(&Cache{})
	labgob.Register(&Vav{})
	kv := &KVServer{me: me, kvaStore: make(map[string]*Vav),
		cachesStore: make(map[string]*Cache)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
