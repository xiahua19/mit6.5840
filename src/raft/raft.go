package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

///////////////////////////////////////////////////////////////////////////////////////
/////////////////////////// DEFINED CONST ENUM OR VARIABLES ///////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

// enum different roles (follower, candidate, leader) for servers
const (
	Follower = iota
	Candidate
	Leader
)

// HeartBeatTimeOut
// const variable for heart beat timeout
const (
	HeartBeatTimeOut          = 101
	ElectTimeOutBase          = 450
	CommitCheckTimeInterval   = time.Duration(250) * time.Millisecond
	ElectTimeOutCheckInterval = time.Duration(100) * time.Millisecond
)

///////////////////////////////////////////////////////////////////////////////////////
///// DEFINED ENTRY, APPLYMSG, REQUESTVOTE, APPENDENTRIES, INSTALLSNAPSHOT STRUCT /////
///////////////////////////////////////////////////////////////////////////////////////

// Entry (log)
// in raft, every event is called an entry which contains `term` and an `interface`
// only leader can create entries
// an entry is commited when most server accept it successfully
type Entry struct {
	Term int
	Cmd  interface{} // can be any type
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool        // whether this ApplyMsg is valid
	Command      interface{} // actual command
	CommandIndex int         // the index of this command in log

	SnapshotValid bool   // whether contain snapshot data
	Snapshot      []byte // actual snapshot data
	SnapshotTerm  int    // term when create snapshot
	SnapshotIndex int    // last index in snapshot
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// (invoked by candidates to gather votes)
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// (used by servers to reply RequestVoteArgs from one server)
// (1. reply false if term < currentTerm)
// (2. if votedFor is null or candidatedId, and candidate's log is at least as up-to-date as receiver's log, grant vote)
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itsel
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs
// (invoked by leader to replicate log entries)
// (and also used as heartbeat)
type AppendEntriesArgs struct {
	Term         int     // leader's term
	LeaderId     int     // so follower can redirect clients
	PrevLogIndex int     // index of log entry immediately preceding new ones
	PrevLogTerm  int     // term of PrevLogIndex entry
	Entries      []Entry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader's commitIndex
}

// AppendEntriesReply
// example AppendEntriesArgs RPC reply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
	XTerm   int  // the term where follower's log conflicts with leader
	XIndex  int  // the index of first log with term equal to XTerm
	XLen    int  // the length of follower's log
}

// InstallSnapshotArgs
// (send snapshot data from leader to other servers)
type InstallSnapshotArgs struct {
	Term              int         // leader’s term
	LeaderId          int         // so follower can redirect clients
	LastIncludedIndex int         // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int         // term of lastIncludedIndex snapshot file
	Data              []byte      // [] raw bytes of the snapshot chunk
	LastIncludedCmd   interface{} // used to occupy the position 0
}

// InstallSnapshotReply
// (reply InstallSnapshotArgs to leader)
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////// DEFINED RAFT STRUCT /////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state on all servers
	// (updated on stable storage before responding to RPCs)
	currentTerm int     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int     // candidateId that received vote in current term (or null if none)
	log         []Entry // log entries, each entry contains command for state machine and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int           // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int           // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	applyCh     chan ApplyMsg // used to transfer message when apply log to state-machine

	// volatile state on leaders
	// (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// other state on all servers
	timeStamp time.Time  // the time received last valid message (ApplyMsg), to determine whether timeout to trigger an election
	role      int        // the role of this server (follower, candidate, leader)
	muVote    sync.Mutex // mutex to protect the voteCount
	voteCount int        // the vote count

	// used by snapshot
	snapShot          []byte // snapshot
	lastIncludedIndex int    // the largest index in snapshot
	lastIncludedTerm  int    // the highest term in snapshot

	// heart timer
	heartTimer *time.Timer
	voteTimer  *time.Timer
	rd         *rand.Rand
	condApply  *sync.Cond
}

///////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////// DEFINED RAFT FUNCTIONS ///////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////// PERSISTENCE //////////////////////////////////////////

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapShot)
}

// readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var votedFor int
	var currentTerm int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("readPresist railed\n")
	} else {
		rf.voteFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("server %v readPersist success\n", rf.me)
	}
}

///////////////////////////////// SNAPSHOT //////////////////////////////////////////

// RealLogIdx
// from virtual index (global real index) to real index (log index)
// for example, snapshot will truncate log, virtual index 0~lastIncludedIndex are in snapshot
// virtual index after (lastIncludeIndex+1) are in the log, with real index start from 0
func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

// VirtualLogIdx
// from real index to virtual index
func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

// Snapshot
// the service says it has created a snapshot that has all info up to and including index.
// this means the service no longer needs the log through (and including) that index.
// Raft should now trim its log as much as possible.
// (accept and save the snapshot and update the log and state in raft)
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO 1. check the snapshot's validation
	// NOTE: snapshot's index must less than or equal to commitIndex, otherwise it may contain uncommitted logs
	//		 snapshot's index must greater than or equal to rf.lastIncludedIndex, otherwise it may be an old snapshot
	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		return
	}

	// TODO 2. save snapshot
	rf.snapShot = snapshot

	// TODO 3. update raft state
	// truncate log, update lastIncludedIndex, lastIncludedTerm, lastApplied
	// the log in snapshot must be commited and applied
	rf.lastIncludedTerm = rf.log[rf.RealLogIdx(index)].Term
	rf.log = rf.log[rf.RealLogIdx(index):]
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	// TODO 4. call persist()
	rf.persist()
}

// readSnapshot
// save the passed data to snapShot
func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("server %v read snapshot failed\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v read snapshot succeess\n", rf.me)
}

// handleInstallSnapshot
// (leader send InstallSnapshotArgs to other servers)
func (rf *Raft) handleInstallSnapshot(serverTo int) {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()

	// TODO 1. check if it is leader
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	// TODO 2. construct InstallSnapshotArgs send to other servers
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Cmd,
	}

	rf.mu.Unlock()

	// TODO 3. send InstallSnapshotArgs to other servers
	ok := rf.sendInstallSnapshot(serverTo, args, reply)
	if !ok {
		return
	}

	// TODO 4. check the reply term, whether is has been an old leader
	// 		   if so, update some fields in it
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.voteFor = -1
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	// TODO 5. update the nextIndex to receiver
	rf.nextIndex[serverTo] = rf.VirtualLogIdx(1)
}

// InstallSnapshot handler
// (servers reply the InstallSnapshotArgs from leader)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO 1. if the leader is old, then reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	// TODO 2. if the leader is new, update the fields in it
	//		   rf.currentTerm, rf.voteFor, rf.role
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	rf.role = Follower
	rf.ResetVoteTimer()

	// TODO 3. if existing log entry has same index and term as snapshot's last included entry
	// 		   retain log entries following it and reply
	//		   else, clear the rf.log (but put an placeholder on index 0)
	// NOTE: the log entry index should be virtual index
	hasEntry := false
	rIdx := 0
	for ; rIdx < len(rf.log); rIdx++ {
		if rf.VirtualLogIdx(rIdx) == args.LastIncludedIndex &&
			rf.log[rIdx].Term == args.LastIncludedTerm {
			hasEntry = true
			break
		}
	}

	// TODO 4. update the fields according to args
	// NOTE:   rf.snapShot, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.commitIndex, rf.lastApplied, rf.Term
	msg := &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	if hasEntry {
		rf.log = rf.log[rIdx:]
	} else {
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{
			Term: rf.lastIncludedTerm,
			Cmd:  args.LastIncludedCmd,
		})
	}

	rf.snapShot = args.Data
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm

	// TODO 4. apply the ApplyMsg to state machine, persist it
	rf.applyCh <- *msg
	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(serverTo int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[serverTo].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//////////////////////////////// VOTE AND ELECT ///////////////////////////////////////

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO 1. if the leader is old, then reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[RequestVote] server %v reject vote for %v", rf.me, args.CandidateId)
		return
	}

	// TODO 2. if the leader is new, update the fields in it
	// NOTE:   rf.voteFor, rf.currentTerm, rf.role
	if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.persist()
	}

	// TODO 3. if leader's log is at least as up-to-date as receiver's log, grant vote
	// NOTE:   check voteFor is -1 or args.CandidateId
	//		   check leader's log is at least as up-to-date as receiver's log
	//		   		if args.LastLogTerm larger than the largest term in rf.log
	//				or is equal but the args.LastLogIndex is greater than the rf.log's length
	// UPDATE: rf.currentTerm, rf.voteFor, rf.role, rf.timeStamp, reply.Term, reply.VoteGranted
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.VirtualLogIdx(len(rf.log)-1)) {

			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.voteFor = args.CandidateId
			rf.ResetVoteTimer()
			rf.role = Follower
			rf.persist()
			reply.VoteGranted = true

			DPrintf("[RequestVote] server %v grant vote for %v", rf.me, args.CandidateId)
			return
		}
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("[RequestVote] server %v reject vote for %v", rf.me, args.CandidateId)

}

// Whether this server need a vote
// (if timeout but not received any message, this sever start a vote for it)
func (rf *Raft) ticker() {
	for !rf.killed() {
		DPrintf("[ticker] server %v currentTerm=%v, voteFor=%v, killed=%v", rf.me, rf.currentTerm, rf.voteFor, rf.killed())

		<-rf.voteTimer.C
		rf.mu.Lock()

		if rf.role != Leader {
			go rf.Elect()
		}
		rf.ResetVoteTimer()
		rf.mu.Unlock()
	}
}

// Elect
// start an elect for sever itself
// 1. lock the contents; 2. update the fields; 3. construct a RequestVote;
// 4. unlock the contents; 5. send the RequestVote RPC to other servers
func (rf *Raft) Elect() {
	// lock
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO 1. start elect, update the fields in it
	// NOTE:   rf.currentTerm, rf.role, rf.voteFor, rf.voteCount, rf.timeStamp
	rf.currentTerm += 1
	rf.role = Candidate
	rf.voteFor = rf.me
	voteCount := 1
	var muVote sync.Mutex

	// TODO 2. construct a RequestVoteArgs
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.VirtualLogIdx(len(rf.log) - 1),
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// TODO 3. send the RequestVote (rf.collectVote) to other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		DPrintf("[Elect] server %v send RequestVote to peer %v with args=%+v", rf.me, i, args)
		go rf.collectVote(i, args, &muVote, &voteCount)
	}
}

// collectVote
// send and receive the RequestVote to other servers
func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs, muVote *sync.Mutex, voteCount *int) {
	// TODO 1. send args to one server and get its answer (rf.GetVoteAnswer)
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	DPrintf("[collectVote] server %v collectVote to peer %v with voteCount=%v, len(rf.peers)=%v and args=%+v, get voteAnswer=%v", rf.me, serverTo, rf.voteCount, len(rf.peers), args, voteAnswer)
	if !voteAnswer {
		return
	}

	muVote.Lock()
	// TODO 2. check if it has achieve half of vote (rf.voteCount), if so return
	if *voteCount > len(rf.peers)/2 {
		muVote.Unlock()
		return
	}

	// TODO 3. increase voteCount, check if it has archive half of vote
	// 		   if so, first check if it is changed to Follower (there is already a new leader)
	// 		   then, update the fields: rf.role, initialize rf.nextIndex and rf.matchIndex, start send heartbeats

	*voteCount += 1
	if *voteCount > len(rf.peers)/2 {
		rf.mu.Lock()

		if rf.role == Follower || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			muVote.Unlock()
			return
		}

		DPrintf("[collectVote] server %v win the vote with voteCount=%v", rf.me, rf.voteCount)
		rf.role = Leader

		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
			rf.matchIndex[i] = rf.lastIncludedIndex
		}

		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}
	muVote.Unlock()
}

// GetVoteAnswer
// send and receive the RequestVote to other servers
func (rf *Raft) GetVoteAnswer(serverTo int, args *RequestVoteArgs) bool {
	// TODO 1. copy the args, and call rf.sendRequestVote to send RequestVote to server and revive its reply
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverTo, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO 2. check whether rf.currentTerm is modified
	// 		   if so there is already a leader, deprecate current vote
	if sendArgs.Term != rf.currentTerm {
		return false
	}

	// TODO 3. check if server reply a larger term, current vote is deprecated, update some fields
	// NOTE:   rf.currentTerm, rf.voteFor, rf.role
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.persist()
	}

	// return the server's vote
	return reply.VoteGranted
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) // call the RequestVote on a server (rpc endpoint)
	return ok
}

////////////////////////// APPENDENTRIES AND HEARTBEAIES ////////////////////////////////

// AppendEntries
// (append an entry to it and reply)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO 1. check if it is a message send by old leader
	//         if so, need tell the sender the latest term and return false
	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// TODO 2. record the access timeStamp
	rf.ResetVoteTimer()

	// TODO 3. check if it is the first message send by new leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.persist()
	}

	// heart beat function
	if len(args.Entries) == 0 {
		DPrintf("[AppendEntries] server %v receive leader %v's heart beat, time reset to %v", rf.me, args.LeaderId, rf.timeStamp)
	} else {
		DPrintf("[AppendEntries] server %v receive leader %v's AppendEntries: %+v, time reset to %v", rf.me, args.LeaderId, args, rf.timeStamp)
	}

	// TODO 4. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	//if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	//	reply.Term = rf.currentTerm
	//	rf.mu.Unlock()
	//	reply.Success = false
	//	return
	//}
	if args.PrevLogIndex >= rf.VirtualLogIdx(len(rf.log)) {
		reply.XLen = rf.VirtualLogIdx(len(rf.log)) // the length of this follower's log
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = -1 // there is no log at PrevLogIndex
		return
	} else if rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term // term not match

		index := args.PrevLogIndex
		for rf.log[rf.RealLogIdx(index)].Term == reply.XTerm && index > rf.lastIncludedIndex {
			index -= 1
		}
		reply.XIndex = index + 1 // the index of first log with term equal to reply.XTerm
		reply.XLen = rf.VirtualLogIdx(len(rf.log))
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// TODO 5. If an existing entry conflicts with a new one (same index but different terms),
	// 		   delete the existing entry and all that follow it
	// TODO 6. Append any new entries not already in the log
	for idx, log := range args.Entries {
		ridx := rf.RealLogIdx(args.PrevLogIndex) + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
			// 某位置发生了冲突, 覆盖这个位置开始的所有内容
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			// 没有发生冲突但长度更长了, 直接拼接
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}

	rf.persist()
	reply.Success = true
	reply.Term = rf.currentTerm

	// TODO 7. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.VirtualLogIdx(len(rf.log)-1))))
		rf.condApply.Signal()
	}
}

// SendHeartBeats
// used by leaders to send heart beats to other servers
func (rf *Raft) SendHeartBeats() {

	// TODO 1. when this leader is still alive, loop infinite
	for rf.killed() == false {
		<-rf.heartTimer.C
		rf.mu.Lock()

		// TODO 2. check whether this server is still a leader, if not return
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		// TODO 3. iterate all peers, send AppendEntriesArgs (call rf.handleHeartBeat) to them
		// NOTE:   check if the length of rf.log is larger or equal than rf.nextIndex[i]
		//		   if so, send the remain entries to peers
		//		   else, send an empty entries to peers
		for i := 0; i < len(rf.peers); i++ {

			if i == rf.me {
				continue
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				LeaderCommit: rf.commitIndex,
			}

			sendInstallSnapshot := false
			if args.PrevLogIndex < rf.lastIncludedIndex {
				sendInstallSnapshot = true
			} else if rf.VirtualLogIdx(len(rf.log)-1) > args.PrevLogIndex {
				args.Entries = rf.log[rf.RealLogIdx(args.PrevLogIndex+1):]
			} else {
				args.Entries = make([]Entry, 0)
			}

			if sendInstallSnapshot {
				go rf.handleInstallSnapshot(i)
			} else {
				args.PrevLogTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
				go rf.handleHeartBeat(i, args)
			}
		}

		rf.mu.Unlock()
		rf.heartTimer.Reset(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

// handleHeartBeat
// (leader send AppendEntriesArgs to other servers and receive their replies)
func (rf *Raft) handleHeartBeat(serverTo int, args *AppendEntriesArgs) {
	// TODO 1. copy the args, and call rf.sendAppendEntries to send AppendEntries to server and revive its reply
	sendArgs := *args
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, &sendArgs, &reply)

	if !ok {
		return
	}

	// TODO 2. check whether rf.currentTerm is modified
	// 		   if so there is already a leader, deprecate current vote
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return
	}

	// TODO 3. if the server reply success, do below things
	//		   (1) update this server's matchIndex (args.PrevLogIndex + len(args.Entries)) and nextIndex
	//		   (2) commit the logs from back to front
	//			   iterate log from back to front, count the number of servers that need the commit
	//			   if the count up to the half of total number, this log can be commited and then break
	//  	   else if reply.Term is larger than rf.currentTerm, this leader is old, need reset some fields
	//		   	   NOTE: rf.currentTerm, rf.voteFor, rf.role, rf.timeStamp
	//		   else if reply.Term equal to rf.currentTerm, and it is also a leader,
	//		   Indicates that the corresponding follower does not have an item matching prevLogTerm at the prevLogIndex position
	//		   or prevLogIndex does not exist, and nextIndex is decremented. The transmitter will try again next time
	if reply.Success {
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		if newMatchIdx > rf.matchIndex[serverTo] {
			rf.matchIndex[serverTo] = newMatchIdx
		}

		newNextIdx := args.PrevLogIndex + len(args.Entries) + 1
		if newNextIdx > rf.nextIndex[serverTo] {
			rf.nextIndex[serverTo] = newNextIdx
		}

		lastLogIndex := rf.VirtualLogIdx(len(rf.log) - 1)
		for lastLogIndex > rf.commitIndex {
			count := 1

			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				// log from 0 to lastLogIndex has been replicated to this server
				if rf.matchIndex[i] >= lastLogIndex && rf.log[rf.RealLogIdx(lastLogIndex)].Term == rf.currentTerm {
					count += 1
				}
			}

			if count > len(rf.peers)/2 {
				rf.commitIndex = lastLogIndex
				break
			}
			lastLogIndex -= 1
		}
		rf.commitIndex = lastLogIndex
		rf.condApply.Signal()
		return
	}

	// this server is old, reset some fields
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.ResetVoteTimer()
		rf.persist()
		return
	}

	// follower does not have an item matching prevLogTerm at the prevLogIndex position
	// or prevLogIndex does not exist
	//if reply.Term == rf.currentTerm && rf.role == Leader {
	//	rf.nextIndex[serverTo] -= 1
	//	rf.mu.Unlock()
	//	return
	//}
	if reply.Term == rf.currentTerm && rf.role == Leader {
		// PrevLogIndex not in the follower
		if reply.XTerm == -1 {
			if rf.lastIncludedIndex >= reply.XLen {
				go rf.handleInstallSnapshot(serverTo)
			} else {
				rf.nextIndex[serverTo] = reply.XLen
			}
			return
		}
		// PrevLogTerm not match
		index := rf.nextIndex[serverTo] - 1
		if index < rf.lastIncludedIndex {
			index = rf.lastIncludedIndex
		}

		for index > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(index)].Term > reply.Term {
			index -= 1
		}

		if index == rf.lastIncludedIndex && rf.log[rf.RealLogIdx(index)].Term > reply.XTerm {
			go rf.handleInstallSnapshot(serverTo)
		} else if rf.log[rf.RealLogIdx(index)].Term == reply.XTerm {
			rf.nextIndex[serverTo] = index + 1
		} else {
			if reply.XIndex <= rf.lastIncludedIndex {
				go rf.handleInstallSnapshot(serverTo)
			} else {
				rf.nextIndex[serverTo] = reply.XIndex
			}
		}
		return
	}
}

// sendAppendEntries
// send an append entry to a certain server and receive its reply
func (rf *Raft) sendAppendEntries(serverTo int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverTo].Call("Raft.AppendEntries", args, reply)
	return ok
}

/////////////////////////////////// API FOR RAFT /////////////////////////////////////////

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}

	newEntry := &Entry{
		Term: rf.currentTerm,
		Cmd:  command,
	}
	rf.log = append(rf.log, *newEntry)
	rf.persist()

	defer func() {
		rf.heartTimer.Reset(time.Duration(1) * time.Millisecond)
	}()
	return rf.VirtualLogIdx(len(rf.log) - 1), rf.currentTerm, true
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}

		msgBuf := make([]*ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied

		for rf.commitIndex > tmpApplied {
			tmpApplied += 1
			if tmpApplied <= rf.lastIncludedIndex {
				continue
			}

			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.RealLogIdx(tmpApplied)].Cmd,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.RealLogIdx(tmpApplied)].Term,
			}
			msgBuf = append(msgBuf, msg)
			DPrintf("[CommitChecker] server %v apply cmd %v(index %v ) to state machine\n", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()

		for _, msg := range msgBuf {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}

			rf.mu.Unlock()
			rf.applyCh <- *msg
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}
	}
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.applyCh = applyCh
	rf.condApply = sync.NewCond(&rf.mu)
	rf.rd = rand.New(rand.NewSource(int64(rf.me)))
	rf.heartTimer = time.NewTimer(0)
	rf.voteTimer = time.NewTimer(0)
	rf.ResetVoteTimer()

	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
		// rf.matchIndex[i] = rf.lastIncludedIndex
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.CommitChecker()

	return rf
}
