package shardgrp

import (
	"sync/atomic"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int32 // remember the last known leader
	clientID string
}

func MakeClerk(clnt *tester.Clnt, servers []string, clientId string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, clientID: clientId}
	return ck
}
func (ck *Clerk) Get(key string, reqId int64) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Args: rpc.Args{Key: key},
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply rpc.GetReply

	for {
		ok := ck.callLeader("KVServer.Get", &args, &reply)

		if ok {
			return reply.Value, reply.Version, reply.Err
		} else if reply.Err == "" {
			return "", 0, rpc.ErrWrongGroup
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion, reqId int64) rpc.Err {
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

		if ok {
			if !ft && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return reply.Err
		} else if reply.Err == "" && !ft {
			return rpc.ErrWrongGroup
		}
		ft = false
	}
}
func (ck *Clerk) callLeader(rpcValue string, args interface{}, reply rpc.ReplyI) bool {
	leader := int(atomic.LoadInt32(&ck.leaderId))
	for i := 0; i < len(ck.servers); i++ {
		// cycle from known leaderId
		index := (leader + i) % len(ck.servers)
		reply.SetErr("")

		ok := ck.clnt.Call(ck.servers[index], rpcValue, args, reply)
		if ok {
			err := reply.GetErr()
			if err != rpc.ErrWrongLeader {
				atomic.StoreInt32(&ck.leaderId, int32(index))
				return ok
			}
		}
	}
	return false
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum, reqId int64) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		Shard:      s,
		Num:        num,
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply shardrpc.FreezeShardReply

	for {
		ok := ck.callLeader("KVServer.FreezeShard", &args, &reply)
		if ok {
			return reply.State, reply.Err
		} else if reply.Err == "" {
			return nil, rpc.ErrMaybe
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum, reqId int64) rpc.Err {
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
		} else if reply.Err == "" {
			return rpc.ErrMaybe
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum, reqId int64) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard:      s,
		Num:        num,
		ClientMeta: rpc.ClientMeta{ClientId: ck.clientID, Seq: reqId}}
	var reply shardrpc.DeleteShardReply

	for {
		ok := ck.callLeader("KVServer.DeleteShard", &args, &reply)

		if ok {
			return reply.Err
		} else if reply.Err == "" {
			return rpc.ErrMaybe
		}
	}
}
