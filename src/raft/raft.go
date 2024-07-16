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
	HeartBeatTimeOut        = 150
	CommitCheckTimeInterval = 500
)

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////// DEFINED MESSAGE, ARGS, REPLY STRUCT /////////////////////////
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	// if index > commitedIndex or index <= lastIncludedIndex

	// TODO 2. save snapshot

	// TODO 3. update raft state
	// truncate log, update lastIncludedIndex, lastIncludedTerm, lastApplied
	// the log in snapshot must be commited and applied

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

func (rf *Raft) handleInstallSnapshot(serverTo int) {
	reply := &InstallSnapshotReply{}

	rf.mu.Lock()
	// DPrintf("server %v handleInstallSnapshot 获取锁mu", rf.me)

	if rf.role != Leader {
		// 自己已经不是Lader了, 返回
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Cmd,
	}

	rf.mu.Unlock()
	// DPrintf("server %v handleInstallSnapshot 释放锁mu", rf.me)

	// 发送RPC时不要持有锁
	ok := rf.sendInstallSnapshot(serverTo, args, reply)
	if !ok {
		// RPC发送失败, 下次再触发即可
		return
	}

	rf.mu.Lock()
	// DPrintf("server %v handleInstallSnapshot 获取锁mu", rf.me)
	defer func() {
		// DPrintf("server %v handleInstallSnapshot 释放锁mu", rf.me)
		rf.mu.Unlock()
	}()

	if reply.Term > rf.currentTerm {
		// 自己是旧Leader
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.ResetTimer()
		rf.persist()
		return
	}

	rf.nextIndex[serverTo] = rf.VirtualLogIdx(1)
}

//////////////////////////////// VOTE AND ELECT ///////////////////////////////////////

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	// 1. reply false if term < currentTerm (sender is old)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		DPrintf("server %v reject the server %v vote: old term %v,\n\t args=%+v\n",
			rf.me, args.CandidateId, args.Term, args)
		return
	}

	// it hasn't vote, reset the voteFor
	if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
	}

	// at least as up-to-date as receiver's log, grant vote
	if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		// to ensure that it hasn't grant a vote
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogIndex >= len(rf.log)-1 && args.LastLogTerm == rf.log[len(rf.log)-1].Term) {
			// 2. If voteFor is null or candidateId, and candidate's log is up-to-date as receiver's log, grant vote
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			rf.voteFor = args.CandidateId
			rf.role = Follower
			rf.timeStamp = time.Now()

			rf.mu.Unlock()
			reply.VoteGranted = true
			DPrintf("server %v grant vote for server %v\n\t args=%+v\n", rf.me, args.CandidateId, args)
			return
		}
	} else {
		DPrintf("server %v reject vote for server %v\n\t args=%+v\n", rf.me, args.CandidateId, args)
	}

	reply.Term = rf.currentTerm
	rf.mu.Unlock()
	reply.VoteGranted = false
}

// Whether this server need a vote
// (if timeout but not received any message, this sever start a vote for it)
func (rf *Raft) ticker() {
	rd := rand.New(rand.NewSource(int64(rf.me)))
	for rf.killed() == false {
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + rd.Intn(300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// lock the server to perform whether it need a vote
		rf.mu.Lock()
		if rf.role != Leader {
			go rf.Elect()
		}
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

	// update the fields
	rf.currentTerm += 1       // increase the term
	rf.role = Candidate       // become a candidate
	rf.voteFor = rf.me        // vote for itself
	rf.voteCount = 1          // from itself
	rf.timeStamp = time.Now() // vote for itself is also a message, need update the time

	// construct a RequestVote
	requestVoteArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	// unlock
	rf.mu.Unlock()

	// send the RequestVote for other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.collectVote(i, requestVoteArgs)
	}
}

// collectVote
// send and receive the RequestVote to other servers
func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	// send args to one server and get its answer
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}

	// check if it has archive half of vote
	rf.muVote.Lock()
	if rf.voteCount > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}

	// increase the vote count
	rf.voteCount += 1
	if rf.voteCount > len(rf.peers)/2 {
		rf.mu.Lock()
		// whether its role is modified by others
		// if so, there is already a leader
		if rf.role == Follower {
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}

		rf.role = Leader
		// initialize nextIndex and matchIndex
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}

		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}
	rf.muVote.Unlock()
}

// GetVoteAnswer
// send and receive the RequestVote to other servers
func (rf *Raft) GetVoteAnswer(serverTo int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverTo, &sendArgs, &reply)
	// if RPC fail, return false
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check whether the currentTerm is modified
	// if so, there is already a leader, deprecate current vote
	if sendArgs.Term != rf.currentTerm {
		return false
	}

	// if server reply a larger term, current vote is deprecated
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.role = Follower
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

	// a message send by old leader
	// need tell the sender the latest term and return false
	if args.Term < rf.currentTerm {
		// 1. Reply false if term < currentTerm
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.Success = false
		return
	}

	// record the access timeStamp
	rf.timeStamp = time.Now()

	// the first message send by new leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.role = Follower
	}

	// heart beat function
	if len(args.Entries) == 0 {
		DPrintf("server %v receive leader %v's heart beat", rf.me, args.LeaderId)
	} else {
		DPrintf("server %v receive leader %v's AppendEntries: %+v\n", rf.me, args.LeaderId, args)
	}

	isConflict := false
	// check the PrevLogIndex and PrevLogTerm
	// if there is no valid log at PrevLogIndex or PrevLogTerm not matches, return false
	if args.PrevLogIndex >= len(rf.log) {
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		isConflict = true
		DPrintf("server %v's log doesn't has log at PrevLogIndex, the length of log is %v\n", rf.me, args.PrevLogIndex, reply.XLen)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for rf.log[i].Term == reply.XTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		isConflict = true
		DPrintf("server %v's log doesn't match at PrevLogIndex %v, args.Term=%v, real term=%v\n", rf.me, args.PrevLogTerm, reply.XTerm)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	for idx, log := range args.Entries {
		ridx := args.PrevLogIndex + 1 + idx
		if ridx < len(rf.log) && rf.log[ridx].Term != log.Term {
			// conflict on this position, need to rewrite new content
			rf.log = rf.log[:ridx]
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		} else if ridx == len(rf.log) {
			rf.log = append(rf.log, args.Entries[idx:]...)
			break
		}
	}
	// 4. Append any new entries not already in the log

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.LeaderCommit > rf.commitIndex {
		// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	}
	rf.mu.Unlock()
}

// SendHeartBeats
// used by leaders to send heart beats to other servers
func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v start send heart beats", rf.me)

	for !rf.killed() {
		rf.mu.Lock()
		// if the server is dead or is note the leader, just return
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			appendEntriesArgs := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}

			if len(rf.log) >= rf.nextIndex[i] {
				// there are log(s) to be sent
				appendEntriesArgs.Entries = rf.log[rf.nextIndex[i]:]
				DPrintf("leader %v start send AppendEntries to server %v", rf.me, i)
			} else {
				// send heart beat
				appendEntriesArgs.Entries = make([]Entry, 0)
				DPrintf("leader %v start send heartbeat to server %v\n\t args=%+v", rf.me, i, appendEntriesArgs)
			}

			go rf.handleHeartBeat(i, appendEntriesArgs)
		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartBeatTimeOut) * time.Millisecond)
	}
}

// handleHeartBeat
func (rf *Raft) handleHeartBeat(serverTo int, args *AppendEntriesArgs) {
	sendArgs := *args
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverTo, &sendArgs, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	// if currentTerm is modified, return
	if sendArgs.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// if the server reply success
	if reply.Success {
		// update this server's matchIndex and nextIndex
		rf.matchIndex[serverTo] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverTo] = rf.matchIndex[serverTo] + 1

		// commit the logs from back to front
		// iterate log from bask to front, count the number of servers that need the commit
		// if the count up to the half of total number, this log can be commited
		lastLogIndex := len(rf.log) - 1
		for lastLogIndex > rf.commitIndex {
			count := 1
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= lastLogIndex && rf.log[lastLogIndex].Term == rf.currentTerm {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = lastLogIndex
				break
			}
			lastLogIndex -= 1
		}
	}

	if reply.Term > rf.currentTerm {
		DPrintf("server %v old leader receive updated term %v, become follower", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.voteFor = -1
		rf.role = Follower
		rf.timeStamp = time.Now()
		rf.mu.Unlock()
		return
	}

	// if term is also same and it's leader, but PrevLogIndex is not match
	// minus the nextIndex with 1, and try again
	if reply.Term == rf.currentTerm && rf.role == Leader {
		rf.nextIndex[serverTo] -= 1
		rf.mu.Unlock()
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

	var term int
	var isleader bool
	// Your code here (3A).
	return term, isleader
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
	return len(rf.log) - 1, rf.currentTerm, true
}

func (rf *Raft) CommitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			applyMsg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Term,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *applyMsg
			DPrintf("server %v apply command %v (index is %v) to state machine\n", rf.me, applyMsg.Command, applyMsg.CommandIndex)
		}
		rf.mu.Unlock()
		time.Sleep(CommitCheckTimeInterval)
	}
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.timeStamp = time.Now()
	rf.role = Follower

	// initialize from state persisted before a crash
	rf.readSnapshot(persister.ReadSnapshot())
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log))
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
