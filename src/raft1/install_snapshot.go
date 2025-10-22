package raft

import (
	"6.5840/raftapi"
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

	// dsp := fmt.Sprintf("the (%d) Server ", rf.me)
	// details := fmt.Sprintf("receive snapshot from %d", args.LeaderId)
	// tester.Annotate("install snapshot", dsp, details)

	// DPrintf("hi from server %d in snapshot", rf.me)

	reply.Term = rf.persistentState.CurrentTerm

	if !rf.isFollower(args.Term) {
		return
	}

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	rf.receivedHeartbeatOrVoteGrant = true
	reply.Term = rf.persistentState.CurrentTerm

	if !args.Done {
		return
	}

	firstLogIndex := rf.persistentState.Log[0].Index
	if firstLogIndex <= args.LastIncludedIndex {
		rf.persistentState.Log = append([]LogEntry{}, LogEntry{
			Index:   args.LastIncludedIndex,
			Term:    args.LastIncludedTerm,
			Command: nil,
		})
	} else if firstLogIndex < args.LastIncludedIndex {
		trimLen := args.LastIncludedIndex - firstLogIndex
		rf.persistentState.Log = append([]LogEntry{}, rf.persistentState.Log[trimLen:]...)
		rf.persistentState.Log[0].Command = nil
	}

	rf.commitIndex, rf.lastApplied = args.LastIncludedIndex, args.LastIncludedIndex

	rf.persist(args.Data)
	rf.smsg = &raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
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
		rf.receivedHeartbeatOrVoteGrant = true
		return
	}

	if args.LastIncludedIndex != rf.persistentState.Log[0].Index {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex

	rf.persist(args.Data)
}
