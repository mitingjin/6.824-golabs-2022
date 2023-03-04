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
	"math/rand"
	"sort"

	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

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
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Log

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state State

	activeTime time.Time

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	includedIndex int
	includedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persistL() {
	// Your code here (2C).
	// Example:
	persistentState := rf.persistentStateL()
	rf.persister.SaveRaftState(persistentState)
}

func (rf *Raft) persistentStateL() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.includedIndex)
	e.Encode(rf.includedTerm)
	e.Encode(rf.log)
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersistL(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, includedIndex, includedTerm int
	var logs []Log
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&includedIndex) != nil || d.Decode(&includedTerm) != nil || d.Decode(&logs) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.includedIndex = includedIndex
		rf.includedTerm = includedTerm
		rf.log = logs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.includedIndex || index > rf.lastLogIndexL() {
		return
	}

	rf.includedTerm = rf.logAtL(index).Term
	rf.log = append([]Log(nil), rf.log[index-rf.includedIndex:]...)
	rf.includedIndex = index

	rf.persister.SaveStateAndSnapshot(rf.persistentStateL(), snapshot)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollowerL(args.Term)
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && func() bool {
		return args.LastLogTerm > rf.lastLogTermL() || (args.LastLogTerm == rf.lastLogTermL() && args.LastLogIndex >= rf.lastLogIndexL())
	}() {
		rf.votedFor = args.CandidateId
		rf.persistL()
		rf.resetActiveTimeL()
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm || rf.state == Candidate {
		rf.convertToFollowerL(args.Term)
	}

	rf.resetActiveTimeL()

	if args.PrevLogIndex < rf.includedIndex {
		reply.ConflictIndex = rf.lastLogIndexL() + 1
		reply.ConflictTerm = -1
		return
	} else if args.PrevLogIndex == rf.includedIndex {
		if args.PrevLogTerm != rf.includedTerm {
			reply.ConflictIndex = rf.lastLogIndexL() + 1
			reply.ConflictTerm = -1
			return
		}
	} else if args.PrevLogIndex > rf.includedIndex {
		if args.PrevLogIndex > rf.lastLogIndexL() {
			reply.ConflictIndex = rf.lastLogIndexL() + 1
			reply.ConflictTerm = -1
			return
		} else if args.PrevLogIndex <= rf.lastLogIndexL() {
			if rf.logAtL(args.PrevLogIndex).Term != args.PrevLogTerm {
				reply.ConflictTerm = rf.logAtL(args.PrevLogIndex).Term
				reply.ConflictIndex = sort.Search(args.PrevLogIndex-rf.includedIndex, func(i int) bool {
					return rf.log[i+1].Term == reply.ConflictTerm
				}) + 1 + rf.includedIndex
				return
			}
		}
	}

	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1-rf.includedIndex], args.Entries...)
		rf.persistL()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndexL())
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	}()

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollowerL(args.Term)
	}

	rf.resetActiveTimeL()

	if args.LastIncludedIndex <= rf.includedIndex || args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	if args.LastIncludedIndex < rf.lastLogIndexL() {
		if rf.logAtL(args.LastIncludedIndex).Term != args.LastIncludedTerm {
			rf.log = []Log{{}}
		} else {
			rf.log = append([]Log(nil), rf.log[args.LastIncludedIndex-rf.includedIndex:]...)
		}
	} else {
		rf.log = []Log{{}}
	}

	rf.includedIndex = args.LastIncludedIndex
	rf.includedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.persistentStateL(), args.Data)

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.includedTerm,
		SnapshotIndex: rf.includedIndex,
	}
	rf.mu.Unlock()
	rf.applyCh <- applyMsg
	rf.mu.Lock()
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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

	term := rf.currentTerm

	// Your code here (2B).
	if rf.state != Leader {
		return -1, term, false
	}
	index := rf.lastLogIndexL() + 1
	log := Log{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, log)
	rf.persistL()

	return index, term, true
}

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		electionTimeout := electionTimeout()
		switch rf.state {
		case Follower:
			if time.Since(rf.activeTime) > electionTimeout {
				rf.convertToCandidateL()
			}
		case Candidate:
			if time.Since(rf.activeTime) > electionTimeout {
				rf.startElectionL()
			}
		case Leader:
			if time.Since(rf.activeTime) > heartbeatDuration() {
				rf.appendEntriesL()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.lastLogIndexL() > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logAtL(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) convertToFollowerL(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persistL()
	}
}

func (rf *Raft) convertToCandidateL() {
	if rf.state == Follower {
		rf.state = Candidate
		rf.startElectionL()
	}
}

func (rf *Raft) convertToLeaderL() {
	if rf.state == Candidate {
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.lastLogIndexL() + 1
			rf.matchIndex[i] = 0
		}
		rf.appendEntriesL()
	}
}

func (rf *Raft) startElectionL() {
	if rf.state == Candidate {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.persistL()
		rf.resetActiveTimeL()
		votes := 1
		term := rf.currentTerm
		for i := range rf.peers {
			if i != rf.me {
				go func(server int, votes *int) {
					rf.mu.Lock()
					if rf.state != Candidate {
						rf.mu.Unlock()
						return
					}
					args := RequestVoteArgs{
						Term:         term,
						CandidateId:  rf.me,
						LastLogIndex: rf.lastLogIndexL(),
						LastLogTerm:  rf.lastLogTermL(),
					}
					reply := RequestVoteReply{}
					rf.mu.Unlock()
					if ok := rf.sendRequestVote(server, &args, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.state == Candidate && rf.currentTerm == term {
							if reply.Term > rf.currentTerm {
								rf.convertToFollowerL(reply.Term)
							} else if reply.Term == rf.currentTerm {
								if reply.VoteGranted {
									*votes++
									if *votes > len(rf.peers)/2 {
										rf.convertToLeaderL()
									}
								}
							}
						}
					}
				}(i, &votes)
			}
		}
	}
}

func (rf *Raft) appendEntriesL() {
	if rf.state == Leader {
		rf.resetActiveTimeL()
		term := rf.currentTerm
		for i := range rf.peers {
			if i != rf.me {
				go func(server int) {
					rf.mu.Lock()
					if rf.state != Leader {
						rf.mu.Unlock()
						return
					}
					if rf.nextIndex[server] > rf.lastLogIndexL()+1 {
						rf.nextIndex[server] = rf.lastLogIndexL() + 1
					}
					if rf.nextIndex[server]-1 < rf.includedIndex {
						args := InstallSnapshotArgs{
							Term:              term,
							LeaderId:          rf.me,
							LastIncludedIndex: rf.includedIndex,
							LastIncludedTerm:  rf.includedTerm,
							Data:              rf.persister.ReadSnapshot(),
						}
						reply := InstallSnapshotReply{}
						rf.mu.Unlock()
						if ok := rf.sendInstallSnapshot(server, &args, &reply); ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if rf.state == Leader && rf.currentTerm == term {
								if reply.Term > rf.currentTerm {
									rf.convertToFollowerL(reply.Term)
								} else if reply.Term == rf.currentTerm {
									rf.nextIndex[server] = args.LastIncludedIndex + 1
									rf.matchIndex[server] = args.LastIncludedIndex
									rf.updateCommitIndexL()
								}
							}
						}
					} else {
						args := AppendEntriesArgs{
							Term:         term,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[server] - 1,
							PrevLogTerm:  rf.logAtL(rf.nextIndex[server] - 1).Term,
							Entries:      nil,
							LeaderCommit: rf.commitIndex,
						}
						if rf.lastLogIndexL() >= rf.nextIndex[server] {
							args.Entries = append([]Log(nil), rf.log[rf.nextIndex[server]-rf.includedIndex:]...)
						}
						reply := AppendEntriesReply{}
						rf.mu.Unlock()
						if ok := rf.sendAppendEntries(server, &args, &reply); ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							if rf.state == Leader && rf.currentTerm == term {
								if reply.Term > rf.currentTerm {
									rf.convertToFollowerL(reply.Term)
								} else if reply.Term == rf.currentTerm {
									if reply.Success {
										rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
										rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
										rf.updateCommitIndexL()
									} else {
										rf.nextIndex[server] = reply.ConflictIndex
										if reply.ConflictTerm != -1 {
											if index := sort.Search(len(rf.log), func(i int) bool {
												return rf.log[i].Term == reply.ConflictTerm
											}); index != len(rf.log) {
												rf.nextIndex[server] = index + 1 + rf.includedIndex
											}
										}
									}
								}
							}
						}
					}
				}(i)
			}
		}
	}
}

func (rf *Raft) updateCommitIndexL() {
	rf.matchIndex[rf.me] = rf.lastLogIndexL()
	matchIndex := append([]int(nil), rf.matchIndex...)
	sort.Sort(sort.Reverse(sort.IntSlice(matchIndex)))
	if n := matchIndex[len(rf.peers)/2]; n > rf.commitIndex && rf.logAtL(n).Term == rf.currentTerm {
		rf.commitIndex = n
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) logAtL(index int) *Log {
	if index < rf.includedIndex {
		return nil
	}
	if index == rf.includedIndex {
		return &Log{Term: rf.includedTerm}
	}
	return &rf.log[index-rf.includedIndex]
}

func (rf *Raft) lastLogIndexL() int {
	return len(rf.log) - 1 + rf.includedIndex
}

func (rf *Raft) lastLogTermL() int {
	return max(rf.includedTerm, rf.log[len(rf.log)-1].Term)
}

func (rf *Raft) resetActiveTimeL() {
	rf.activeTime = time.Now()
}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func electionTimeout() time.Duration {
	rand.NewSource(time.Now().UnixNano())
	return time.Duration(500+rand.Intn(500)) * time.Millisecond
}

func heartbeatDuration() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []Log{{}}

	rf.activeTime = time.Now()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersistL(persister.ReadRaftState())
	rf.lastApplied = rf.includedIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()

	return rf
}
