package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Vav struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	mu   sync.Mutex

	// Your definitions here.
	kvaStore map[string]Vav
	lastSeen map[uuid.UUID]int64 // clientID → last executed Seq
	lastResp map[uuid.UUID]any   // clientID → last reply
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	switch req := req.(type) {
	case *rpc.GetArgs:
		return kv.doGet(req)
	case *rpc.PutArgs:
		return kv.doPut(req)
	default:
		return nil
	}
}

func (kv *KVServer) doGet(req *rpc.GetArgs) *rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := &rpc.GetReply{}
	val, ok := kv.kvaStore[req.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = val.value
		reply.Version = val.version
		reply.Err = rpc.OK
	}
	return reply
}

// handle Put requests
func (kv *KVServer) doPut(req *rpc.PutArgs) *rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := &rpc.PutReply{}
	val, ok := kv.kvaStore[req.Key]
	if !ok {
		if req.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}
	} else {
		if val.version != req.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}
	}

	// success path
	kv.kvaStore[req.Key] = Vav{
		version: req.Version + 1,
		value:   req.Value,
	}

	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	kv.mu.Lock()

	if seq, ok := kv.lastSeen[args.ClientId]; ok && seq >= args.Seq {
		if rep, ok := kv.lastResp[args.ClientId].(*rpc.GetReply); ok {
			*reply = *rep
		}
		kv.mu.Unlock()
		return
	}
	rsm := kv.rsm
	kv.mu.Unlock()

	err, res := rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return

	}
	rep, ok := res.(*rpc.GetReply)
	if ok {
		reply.Err = rep.Err
		reply.Value = rep.Value
		reply.Version = rep.Version
		kv.mu.Lock()
		kv.lastSeen[args.ClientId] = args.Seq
		kv.lastResp[args.ClientId] = rep
		kv.mu.Unlock()
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	kv.mu.Lock()

	if seq, seen := kv.lastSeen[args.ClientId]; seen && args.Seq <= seq {
		// Already applied → return cached reply
		cached := kv.lastResp[args.ClientId].(*rpc.PutReply)
		reply.Err = cached.Err
		kv.mu.Unlock()
		return
	}

	rsm := kv.rsm
	kv.mu.Unlock()
	err, res := rsm.Submit(args)
	if err == rpc.ErrWrongLeader {
		reply.Err = err
		return
	}
	rep, ok := res.(*rpc.PutReply)
	if ok {
		reply.Err = rep.Err
		kv.mu.Lock()
		kv.lastSeen[args.ClientId] = args.Seq
		kv.lastResp[args.ClientId] = rep
		kv.mu.Unlock()
	}
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
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me, kvaStore: make(map[string]Vav),
		lastSeen: make(map[uuid.UUID]int64),
		lastResp: make(map[uuid.UUID]any)}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
