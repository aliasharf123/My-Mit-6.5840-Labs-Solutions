package kvraft

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int32 // remember the last known leader

	requestId int64
	clientID  uuid.UUID
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, clientID: uuid.New()}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := rpc.GetArgs{Key: key, ClientId: ck.clientID, Seq: reqId}
	var reply rpc.GetReply

	for {
		ok := ck.callLeader("KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrNoKey {
				return "", 0, rpc.ErrNoKey
			}
			break
		}
		time.Sleep(100 * time.Millisecond) // wait before retrying all servers again
	}
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := rpc.PutArgs{
		Value:    value,
		Key:      key,
		Version:  version,
		Seq:      reqId,
		ClientId: ck.clientID,
	}
	reply := rpc.PutReply{}
	ft := true // flag to check if its first try

	for {
		ok := ck.callLeader("KVServer.Put", &args, &reply)
		if ok {
			if !ft && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			break
		}
		ft = false
		time.Sleep(100 * time.Millisecond)
	}

	return reply.Err
}

func (ck *Clerk) callLeader(rpcValue string, args interface{}, reply interface{}) bool {

	leader := int(atomic.LoadInt32(&ck.leaderId))
	for i := 0; i < len(ck.servers); i++ {
		// cycle from known leaderId
		index := (leader + i) % len(ck.servers)

		ok := ck.clnt.Call(ck.servers[index], rpcValue, args, reply)
		if ok {
			var err rpc.Err
			switch rep := reply.(type) {
			case *rpc.GetReply:
				err = rep.Err
			case *rpc.PutReply:
				err = rep.Err
			}
			if err != rpc.ErrWrongLeader {
				atomic.StoreInt32(&ck.leaderId, int32(index))
				return ok
			}
		}
	}

	return false
}
