package raft

import (
	"fmt"

	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Offset            int    // byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	Done              bool   // true if this is the last chunk

}
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dsp := fmt.Sprintf("the (%d) Server ", rf.me)
	details := fmt.Sprintf("receive snapshot from %d", args.LeaderId)
	tester.Annotate("install snapshot", dsp, details)

	// DPrintf("hi from server %d in snapshot", rf.me)

	reply.Term = rf.persistentState.CurrentTerm
	if !rf.isFollower(args.Term) {
		return
	}
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	reply.Term = rf.persistentState.CurrentTerm
	rf.receivedHeartbeatOrVoteGrant = true

	if !args.Done {
		return
	}

	newLog := []LogEntry{{
		Term:    args.LastIncludedTerm,
		Index:   args.LastIncludedIndex,
		Command: nil,
	}}

	snapshotBoundary := args.LastIncludedIndex - rf.lastIncludedIndex

	if snapshotBoundary < len(rf.persistentState.Log) &&
		rf.persistentState.Log[snapshotBoundary].Term == args.LastIncludedTerm {
		newLog = append(newLog, rf.persistentState.Log[snapshotBoundary+1:]...)
	}
	rf.persistentState.Log = newLog

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)

	rf.persist(args.Data)
	rf.smsg = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyEntriesCondVar.Signal()
	// DPrintf("hi from server %d in snapshot (the end)", rf.me)

}
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.persistentState.CurrentTerm < reply.Term {
		rf.BackToFollower(reply.Term)
		return
	}

	if args.LastIncludedIndex != rf.persistentState.Log[0].Index {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	rf.persist(args.Data)
}
