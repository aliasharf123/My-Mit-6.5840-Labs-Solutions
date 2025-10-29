package shardgrp

import (
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int32 // remember the last known leader

	requestId int64
	clientID  string
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, clientID: uuid.New().String()}
	return ck
}
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := rpc.GetArgs{Args: rpc.Args{Key: key},
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply rpc.GetReply

	for {
		ok := ck.callLeader("KVServer.Get", &args, &reply)
		DPrintf("[shard client Get]: (%+v) clId=(%s) seq=(%d) (%d)", reply.Err, ck.clientID, reqId, ok)

		if !ok && reply.Err == "" {
			return "", 0, rpc.ErrWrongGroup
		}
		if ok {
			if reply.Err != rpc.OK {
				return reply.Value, reply.Version, reply.Err
			}
			break
		}
	}
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := rpc.PutArgs{
		Value:      value,
		Args:       rpc.Args{Key: key},
		Version:    version,
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId},
	}
	reply := rpc.PutReply{}
	ft := true // flag to check if its first try

	for {
		ok := ck.callLeader("KVServer.Put", &args, &reply)
		DPrintf("[shard client Put]: (%+v) clId=(%s) seq=(%d) (%d)", reply.Err, ck.clientID, reqId, ok)

		if !ok && reply.Err == "" && !ft {
			return rpc.ErrWrongGroup
		}
		if ok {
			if !ft && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			break
		}
		ft = false
	}

	return reply.Err
}
func (ck *Clerk) callLeader(rpcValue string, args interface{}, reply rpc.ReplyI) bool {

	leader := int(atomic.LoadInt32(&ck.leaderId))
	for i := 0; i < len(ck.servers); i++ {
		// cycle from known leaderId
		index := (leader + i) % len(ck.servers)
		reply.SetErr("")

		ok := ck.clnt.Call(ck.servers[index], rpcValue, args, reply)
		DPrintf("[callLeader]: (%+v) clId=(%s) server=(%s) (%v)(%s)", reply.GetErr(), ck.clientID, ck.servers[index], ok, rpcValue)
		if ok {
			err := reply.GetErr()
			if err != rpc.ErrWrongLeader {
				atomic.StoreInt32(&ck.leaderId, int32(index))
				return ok
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := shardrpc.FreezeShardArgs{
		Shard:      s,
		Num:        num,
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply shardrpc.FreezeShardReply
	for {
		ok := ck.callLeader("KVServer.FreezeShard", &args, &reply)
		if ok {
			return reply.State, reply.Err
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := shardrpc.InstallShardArgs{
		Shard:      s,
		Num:        num,
		State:      state,
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply shardrpc.InstallShardReply
	for {
		ok := ck.callLeader("KVServer.InstallShard", &args, &reply)
		if ok {
			return reply.Err
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	reqId := atomic.AddInt64(&ck.requestId, 1)
	args := shardrpc.DeleteShardArgs{
		Shard:      s,
		Num:        num,
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply shardrpc.DeleteShardReply
	for {
		ok := ck.callLeader("KVServer.DeleteShard", &args, &reply)
		if ok {
			return reply.Err
		}
	}
}
