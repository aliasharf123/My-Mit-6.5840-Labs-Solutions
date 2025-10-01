package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	Me  int       // server id
	Req any       // client request
	Id  uuid.UUID // unique identifier
}
type OpResult struct {
	Req any
	Err rpc.Err
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	isLeader  bool
	term      int
	lastIndex int
	opChMap   map[uuid.UUID]chan OpResult
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		opChMap:      map[uuid.UUID]chan OpResult{},
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		go rsm.Reader()
	}
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	var op Op
	op.Id = uuid.New()
	op.Me = rsm.me
	op.Req = req

	// switch req.(type) {
	// case IncRep:
	// Calling to `start` operation
	rsm.lastIndex, rsm.term, rsm.isLeader = rsm.rf.Start(op)

	// If raft.Start() indicates peer not reader should return `rpc.ErrWrongLeader`
	if !rsm.isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	// Waiting for messages from applychan
	ch := make(chan OpResult, 1)
	rsm.opChMap[op.Id] = ch
	rsm.mu.Unlock()

	// Wait with timeout

	select {
	case opRes := <-ch:
		if opRes.Err == rpc.ErrWrongLeader {
			opRes.Req = nil
		}
		return opRes.Err, opRes.Req
	case <-time.After(2 * time.Second):
		return rpc.ErrWrongLeader, nil
	}
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {

		opReq := msg.Command.(Op)
		result := OpResult{
			Req: rsm.sm.DoOp(opReq.Req),
			Err: rpc.OK,
		}

		rsm.mu.Lock()
		if msg.CommandTerm != rsm.term {
			// Wrong leader at time of commit
			result.Err = rpc.ErrWrongLeader
		}
		// Notify waiting client if exists
		if ch, ok := rsm.opChMap[opReq.Id]; ok {
			ch <- result
			delete(rsm.opChMap, opReq.Id) // clean up to avoid leaks
		}

		rsm.mu.Unlock()
	}
}
