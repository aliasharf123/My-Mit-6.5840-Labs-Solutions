package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Log entry Object
type LogEntry struct {
	Term    int         //when entry was received by leader
	Command interface{} // command for state machine
	Index   int
}

// Raft states
const (
	Follower = iota
	Candidate
	Leader
)

type PersistentState struct {
	CurrentTerm int        // latest term server has seen
	VotedFor    int        // candidateId that received vote in current term (or -1 if none)
	Log         []LogEntry // log entries (each entry contains a command and term)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers:
	persistentState *PersistentState

	// Volatile state on all servers:
	commitIndex                  int // index of highest log entry known to be committed
	lastApplied                  int // Index of highest log entry applied to state machine
	state                        int
	receivedHeartbeatOrVoteGrant bool                   // Check the heartbeat
	applyChChan                  *chan raftapi.ApplyMsg // channel to send committed entries
	voteGranted                  int                    // voted granted count (init by zero)
	applyEntriesCondVar          *sync.Cond
	replicatorCond               []*sync.Cond // used to signal replicator goroutine to batch replicating entries
	// Volatile state on leaders:
	nextIndex  []int // initialized to last leader log + 1
	matchIndex []int // initialized to first entry index

	// snapshot msg
	smsg *raftapi.ApplyMsg
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyEntriesCondVar.Wait()
		}

		firstLog := rf.persistentState.Log[0]
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.persistentState.Log[lastApplied+1-firstLog.Index:commitIndex+1-firstLog.Index])
		if rf.smsg != nil {
			msg := rf.smsg
			rf.smsg = nil
			rf.mu.Unlock()
			*rf.applyChChan <- *msg
		} else {
			rf.mu.Unlock()
		}
		for _, entry := range entries {
			*rf.applyChChan <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()

		if rf.lastApplied < commitIndex {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for !rf.killed() {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateLog(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.persistentState.Log[rf.getLastIndex()].Index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persistentState.CurrentTerm, rf.state == Leader
}

func (rf *Raft) persist(snapshot []byte) {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.persistentState)
	if snapshot == nil {
		rf.persister.Save(w.Bytes(), rf.persister.ReadSnapshot())
	} else {
		rf.persister.Save(w.Bytes(), snapshot)
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&rf.persistentState); err != nil {
		DPrintf("error decoding: %+v\n", err)
	} else {
		rf.lastApplied = rf.persistentState.Log[0].Index
		rf.commitIndex = rf.persistentState.Log[0].Index
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("the snapshot called in server (%d)", rf.me)

	if index <= rf.persistentState.Log[0].Index {
		return
	}
	logIndex := index - rf.persistentState.Log[0].Index

	rf.persistentState.Log = append([]LogEntry{}, rf.persistentState.Log[logIndex:]...)
	rf.persistentState.Log[0].Command = nil
	rf.persist(snapshot)
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.persistentState.CurrentTerm
	}()

	reply.VoteGranted = false

	if !rf.isFollower(args.Term) {
		return
	}

	canVote := rf.persistentState.VotedFor == -1 || rf.persistentState.VotedFor == args.CandidateId
	if canVote && rf.isLogUpToDate(args) {
		rf.persistentState.VotedFor = args.CandidateId
		reply.VoteGranted = true
		rf.receivedHeartbeatOrVoteGrant = true
		rf.persist(nil)
	}
}

// Raft determines which of two logs is more up-to-date
// by comparing the index and term of the last entries in the
// logs. If the logs have last entries with different terms, then``
// the log with the later term is more up-to-date. If the logs
// end with the same term, then whichever log is longer is
// more up-to-date

func (rf *Raft) isLogUpToDate(args *RequestVoteArgs) bool {
	lastLog := rf.persistentState.Log[rf.getLastIndex()]
	return (args.LastLogTerm > lastLog.Term) || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	rf.persistentState.Log = append(rf.persistentState.Log, LogEntry{
		Term:    rf.persistentState.CurrentTerm,
		Command: command,
		Index:   rf.persistentState.Log[rf.getLastIndex()].Index + 1,
	})

	rf.persist(nil)

	lastLog := rf.persistentState.Log[rf.getLastIndex()]

	rf.nextIndex[rf.me] = lastLog.Index + 1
	rf.matchIndex[rf.me] = lastLog.Index

	// DPrintf("(%d) Send broadcast to other servers", rf.me)
	rf.BroadcastHeartbeat(false)

	return lastLog.Index, lastLog.Term, true
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateLog(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}
func (rf *Raft) replicateLog(peerId int) {

	if rf.killed() {
		return
	}
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[peerId] <= rf.persistentState.Log[0].Index {
		// DPrintf("server (%d) send snap shot to (%d) server: data(%v) ", rf.me, peerId, rf.persister.ReadSnapshot())

		args := &InstallSnapshotArgs{
			Term:              rf.persistentState.CurrentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.persistentState.Log[0].Index,
			LastIncludedTerm:  rf.persistentState.Log[0].Term,
			Offset:            0,
			Done:              true,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		rf.sendInstallSnapshot(peerId, args)
		return
	}

	rf.checkCommitUpdate()

	var reply AppendEntriesReply
	args := AppendEntriesArgs{
		Term:         rf.persistentState.CurrentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	nextIndex := rf.nextIndex[peerId] - rf.persistentState.Log[0].Index
	prevLog := rf.persistentState.Log[nextIndex-1]
	args.PrevLogIndex = prevLog.Index
	args.PrevLogTerm = prevLog.Term
	args.Entries = make([]LogEntry, len(rf.persistentState.Log[nextIndex:]))
	copy(args.Entries, rf.persistentState.Log[nextIndex:])

	rf.mu.Unlock()

	ok := rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.persistentState.CurrentTerm {
			rf.BackToFollower(reply.Term)
			rf.receivedHeartbeatOrVoteGrant = true
			return
		}
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[peerId] = args.PrevLogIndex + len(args.Entries) + 1
			}
			rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1

			rf.checkCommitUpdate()
			return
		} else {
			if reply.ConflictIndex != -1 {
				rf.nextIndex[peerId] = reply.ConflictIndex - 1
			} else {
				rf.nextIndex[peerId] = rf.nextIndex[peerId] - 1
			}
			if rf.nextIndex[peerId] < 1 {
				rf.nextIndex[peerId] = 1
			}
			return
		}

	}

}

func (rf *Raft) checkCommitUpdate() {
	for i := rf.getLastIndex(); i > rf.commitIndex-rf.persistentState.Log[0].Index; i-- {
		replicationCount := 0
		for peer := range len(rf.peers) {
			if rf.matchIndex[peer] >= i+rf.persistentState.Log[0].Index {
				replicationCount++
			}
		}
		if replicationCount > rf.majorityThreshold() && rf.persistentState.Log[i].Term == rf.persistentState.CurrentTerm {
			rf.commitIndex = i + rf.persistentState.Log[0].Index
			rf.applyEntriesCondVar.Signal()
			break
		}
	}
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex

}
type AppendEntriesReply struct {
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // index for start of mismatch between leader and server (optimization)
}

// the AppendEntries request send by only the leader
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("server (%d) sent appendEntry to (%d) with Entry \n %d", args.LeaderId, rf.me, len(args.Entries))
	// dsp := fmt.Sprintf("the (%d) Server", rf.me)
	// details := fmt.Sprintf("receive from (%d) the the entry: \n %+v", args.LeaderId, args.Entries)
	// tester.Annotate("Receive appendEntry", dsp, details)

	reply.Success = false
	reply.Term = rf.persistentState.CurrentTerm
	reply.ConflictIndex = -1

	if !rf.isFollower(args.Term) {
		return
	}
	rf.receivedHeartbeatOrVoteGrant = true

	prevLogIndex := args.PrevLogIndex - rf.persistentState.Log[0].Index
	if prevLogIndex < 0 {
		// force to send a snapshot
		reply.ConflictIndex = 0
		return
	}

	if prevLogIndex >= len(rf.persistentState.Log) {
		reply.ConflictIndex = rf.persistentState.Log[rf.getLastIndex()].Index
		return
	}

	if rf.persistentState.Log[prevLogIndex].Term != args.PrevLogTerm {
		var conflictIndex int
		for i := prevLogIndex; i > 0; i-- {
			if rf.persistentState.Log[i-1].Term != rf.persistentState.Log[prevLogIndex].Term {
				conflictIndex = i
				break
			}
		}
		reply.ConflictIndex = conflictIndex + rf.persistentState.Log[0].Index
		return
	}

	reply.Success = true
	// rule.3 and .4
	for i, entry := range args.Entries {
		targetIndex := prevLogIndex + 1 + i

		if targetIndex > rf.getLastIndex() {
			rf.persistentState.Log = append(rf.persistentState.Log, args.Entries[i:]...)
			break
		} else if rf.persistentState.Log[targetIndex].Term != entry.Term {
			rf.persistentState.Log = rf.persistentState.Log[:targetIndex]
			rf.persistentState.Log = append(rf.persistentState.Log, args.Entries[i:]...)
			break
		}
		// If terms match, continue checking next entry
	}

	rf.persist(nil)

	// rule.5 in figure 2
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if args.LeaderCommit-rf.persistentState.Log[0].Index >= len(rf.persistentState.Log) {
			rf.commitIndex = rf.persistentState.Log[rf.getLastIndex()].Index
		}
	}
	rf.applyEntriesCondVar.Signal()

}
func (rf *Raft) isFollower(leaderTerm int) bool {

	// notify the leader
	if leaderTerm < rf.persistentState.CurrentTerm {
		return false
	} else if leaderTerm > rf.persistentState.CurrentTerm {
		rf.BackToFollower(leaderTerm)
	}
	return true
}

func (rf *Raft) getLastIndex() int {
	return len(rf.persistentState.Log) - 1
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("the server (%d) is dead", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// election timeout range from 350 to 550
		// milliseconds.
		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		continueElection := false

		// serverRole := rf.sCurrentState()
		rf.mu.Lock()

		// dsp := fmt.Sprintf("the (%d) Server ", rf.me)
		// details := fmt.Sprintf("the (%d) Server is %s in (%d) term,\n log = %+v with length (%d)", rf.me, serverRole, rf.persistentState.CurrentTerm, rf.persistentState.Log, len(rf.persistentState.Log))
		// tester.Annotate("Servers State", dsp, details)

		args := RequestVoteArgs{
			CandidateId: rf.me,
		}

		if !rf.receivedHeartbeatOrVoteGrant && rf.state != Leader {
			// DPrintf("the server become candidate %d \n", rf.me)
			rf.state = Candidate
			rf.persistentState.CurrentTerm += 1
			rf.persistentState.VotedFor = rf.me
			rf.voteGranted = 1
			continueElection = true
			args.Term = rf.persistentState.CurrentTerm

			rf.persist(nil)

			lastLogIndex := 0
			if len(rf.persistentState.Log) >= 2 {
				lastLogIndex = len(rf.persistentState.Log) - 1
				args.LastLogIndex = rf.persistentState.Log[lastLogIndex].Index
			}

			if lastLogIndex > 0 {
				args.LastLogTerm = rf.persistentState.Log[lastLogIndex].Term
			}
		} else {
			rf.receivedHeartbeatOrVoteGrant = false
		}

		rf.mu.Unlock()
		if continueElection {
			for i := range len(rf.peers) {
				if i != rf.me {
					go rf.PerformRequestVote(i, &args)
				}
			}
		}

	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.persistentState = &PersistentState{
		VotedFor: -1,
		Log:      make([]LogEntry, 1),
	}
	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.applyChChan = &applyCh
	rf.applyEntriesCondVar = sync.NewCond(&rf.mu)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.replicatorCond = make([]*sync.Cond, len(peers))

	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, rf.persistentState.Log[rf.getLastIndex()].Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// start replicator goroutine to replicate entries in batch
			go rf.replicator(i)
		}
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to apply commands to state machine
	go rf.applier()

	return rf
}
func (rf *Raft) heartbeat() {

	for !rf.killed() {
		rf.mu.Lock()
		serverRole := rf.state
		rf.mu.Unlock()

		if serverRole != Leader {
			break
		}
		// DPrintf("the leader (%d) send a heartbeat", rf.me)
		rf.BroadcastHeartbeat(true)

		time.Sleep(time.Duration(125) * time.Millisecond)
	}
}

func (rf *Raft) PerformRequestVote(targetPeerIndex int, args *RequestVoteArgs) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(targetPeerIndex, args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.state != Candidate {
		return
	}
	// DPrintf("the vote from %d server is %d with term %d when the candidate term is %d", targetPeerIndex, rf.voteGranted, reply.Term, rf.persistentState.CurrentTerm)
	if reply.VoteGranted && args.Term == rf.persistentState.CurrentTerm {
		rf.voteGranted += 1
		// DPrintf("Grant vote from %d, the number (%d) to candidate number (%d) \n", targetPeerIndex, rf.voteGranted, args.CandidateId)
		// details := fmt.Sprintf("Grant vote from %d, the number (%d) to candidate number (%d) \n", targetPeerIndex, rf.voteGranted, args.CandidateId)
		// tester.Annotate("voting", "vote", details)
		if rf.voteGranted > rf.majorityThreshold() {
			rf.state = Leader

			for i := range len(rf.peers) {
				rf.nextIndex[i] = rf.persistentState.Log[rf.getLastIndex()].Index + 1
				rf.matchIndex[i] = 0
			}
			// DPrintf("The peer (%d) become the leader", args.CandidateId)
			// details := fmt.Sprintf("The peer (%d) become the leader", args.CandidateId)
			// tester.Annotate("leader elected", "leader", details)
			go rf.heartbeat()
		}
	} else if reply.Term > rf.persistentState.CurrentTerm {
		rf.BackToFollower(reply.Term)
		rf.receivedHeartbeatOrVoteGrant = true
		return
	}
}

func (rf *Raft) majorityThreshold() int {
	return len(rf.peers) / 2
}

func (rf *Raft) BackToFollower(term int) {
	rf.persistentState.CurrentTerm = term
	rf.persistentState.VotedFor = -1

	rf.state = Follower
	rf.voteGranted = 0

	rf.persist(nil)
}
