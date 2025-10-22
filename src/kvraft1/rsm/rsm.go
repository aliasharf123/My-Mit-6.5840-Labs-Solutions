package rsm

import (
	"log"
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
	Me  int    // server id
	Req any    // client request
	Id  string // unique identifier
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
	ps           *tester.Persister

	// Your definitions here.
	isLeader  bool
	term      int
	lastIndex int
	opChMap   map[string]chan OpResult
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
		opChMap:      map[string]chan OpResult{},
		ps:           persister,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		rsm.sm.Restore(rsm.ps.ReadSnapshot())
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
	op.Id = uuid.New().String()
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
	var response OpResult
	select {
	case result := <-ch:
		if result.Err == rpc.ErrWrongLeader {
			result.Req = nil
		}
		response.Err, response.Req = result.Err, result.Req
	case <-time.After(2 * time.Second):
		response.Err, response.Req = rpc.ErrWrongLeader, nil
	}
	return response.Err, response.Req
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		// DPrintf("[Reader]: message (%+v) {%d}", msg, rsm.me)

		if msg.CommandValid {
			opReq := msg.Command.(Op)
			result := OpResult{
				Req: rsm.sm.DoOp(opReq.Req),
				Err: rpc.OK,
			}

			rsm.mu.Lock()

			// Check leadership after commit
			if !rsm.isLeader {
				result.Err = rpc.ErrWrongLeader
			}

			// Get waiting channel (no second lock)
			ch, ok := rsm.opChMap[opReq.Id]
			if ok {
				delete(rsm.opChMap, opReq.Id)
			}
			if rsm.maxraftstate != -1 && rsm.maxraftstate < rsm.rf.PersistBytes() {
				rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
			}

			rsm.mu.Unlock()

			// Send result after unlock to avoid blocking while holding lock
			if ok {
				select {
				case ch <- result:
				default: // prevent blocking forever
				}
				close(ch)
			}
		} else if msg.SnapshotValid {
			DPrintf("[SnapShot]: {%d}", rsm.me)
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Unlock()
		} else {
			log.Fatalf("Invalid applyMsg, %+v\n", msg)
		}
	}
}
