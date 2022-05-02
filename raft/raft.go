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
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//

/*
each message has 3 parts
CommandIndex: the index to which the command will be applied
*/
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// type State string

const (
	Leader    = "Leader"
	Candidate = "Candidate"
	Follower  = "Follower"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// all servers
	commitIndex int // last valid index to which commit is done
	lastApplied int // last index to which an entry is applied

	// leaders
	nextIndex  []int // an array with the length of number of peers showing the next available index for each server
	matchIndex []int // an array with the same size of nextIndex storing the matched index for each server

	// other states
	state       string
	voteCount   int
	applyCh     chan ApplyMsg
	winElectCh  chan bool
	stepDownCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool
}

/*
returns the current term and if the server thinks it's a leader
*/
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

//
// lock must be held before calling this.
//

/*
should save some required information to handle recoveries from failures
*/
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil {
		panic("failed to encode raft persistent state")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// bootstrap without any state?
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logs) != nil {
		panic("failed to decode raft persistent state")
	}
}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//

/*
each candidate who initiates a voting, reports some of its features.
*/
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//

/*
each participant in the voting fills the two bellow variables.
using the replies, a candidate can decide if it can be a leader or
it must step down to follower.
*/
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arguments structure.
// field names must start with capital letters!
//

/*
just a leader is able to send such a query.
if entries don't contain a message, they'll be
considered as hearbeats.
the prevLogIndex and prevLogTerm variables are
set associated with the server of interest.
*/
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

//
// AppendEntries RPC reply structure.
// field names must start with capital letters!
//

/*
the last index and current term of a specific server
is stored in the leader. If these two conflict, the leader
is supposed to update it's arrays.
*/
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

//
// get the index of the last log entry.
// lock must be held before calling this.
//
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

//
// get the term of the last log entry.
// lock must be held before calling this.
//
func (rf *Raft) getLastTerm() int {
	return rf.logs[rf.getLastIndex()].Term
}

//
// get the randomized election timeout.
//
func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(240))
}

//
// send value to an un-buffered channel without blocking
//

/*
this function is used to prevent race conditions or blocking.
select{} helps us achieve our goal. without that, the code will get stuck.
*/
func (rf *Raft) sendValueToChannel(ch chan bool, val bool) {
	select {
	case ch <- val:
	default:
	}
}

//
// step down to follower when getting higher term,
// lock must be held before calling this.
//

/*
sometimes, a candidate or a leader finds out that its log
is stale and hence, it has to become a follower.
there is an if at the end of this function preventing the race condition.
*/
func (rf *Raft) stepDownToFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	// step down if not follower, this check is needed
	// to prevent race where state is already follower
	if state != Follower {
		// rf.stepDownCh <- true
		rf.sendValueToChannel(rf.stepDownCh, true)
	}
}

//
// check if the candidate's log is at least as up-to-date as ours
// lock must be held before calling this.
//

/*
a follower needs to be sure that a candidate is at least as update as itself.
this evaluation is done using the comparison of terms and last indices.
*/
func (rf *Raft) isUpToDate(cLastIndex int, cLastTerm int) bool {
	myLastIndex, myLastTerm := rf.getLastIndex(), rf.getLastTerm()

	if cLastTerm == myLastTerm {
		return cLastIndex >= myLastIndex
	}

	return cLastTerm > myLastTerm
}

//
// apply the committed logs.
//

/*
when an appendEntry request is accepted by the majority of the servers,
then it can be commited. the function below uses the fills in the log array
from lastApllied to commitIndex. the lastApplied variable is also updated each time.
*** the lastApllied variable is updated after each iteration (not after the completion of the
entire for loop) in order to keep the most updated amount at each step.
*/
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

//
// RequestVote RPC handler.
//

/*
this function is called by sendRequestVote function. it receives the args, compare it with
its variables and fills in the reply.
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		// rf.grantVoteCh <- true
		rf.sendValueToChannel(rf.grantVoteCh, true)
	}
}

//
// send a RequestVote RPC to a server.
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
//

/*
the point is that lock is used before accessing other variables. This prevents inconsistency
sendRequstVote calls the RequestVote function of one server via RPCs.
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.voteCount += 1
		// only send once when vote count just reaches majority
		if rf.voteCount == len(rf.peers)/2+1 {
			// rf.winElectCh <- true
			rf.sendValueToChannel(rf.winElectCh, true)
		}
	}
}

//
// broadcast RequestVote RPCs to all peers in parallel.
// lock must be held before calling this.
//

/*
for voting, args is set once and sendRequestVote RPCs are called
concurrently.
*/
func (rf *Raft) performRequestVote() {
	if rf.state != Candidate {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

//
// AppendEntries RPC handler.
//

/*
reply will be filled by the server. if the leader is not up to date, the
entries won't be appended and the wrong leader becomes a follower.
each appendEntry request from a valid leader is primarily considered as a
heartbeat message.
if the index or term of the server conflict with the corresponding
ones from the leader, the query gets declined and the correct index and
term are transferred as reply variables.
after conflicting indices being specified, logs will get truncated and correct
entry slice gets added to it.
after logs being updated, commit index is set to min(leaderCommit, lastIndex)
when all is ok, applyLogs is called concurrently.
*/

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	lastIndex := rf.getLastIndex()
	// rf.heartbeatCh <- true
	rf.sendValueToChannel(rf.heartbeatCh, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	// follower log is shorter than leader
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return
	}

	// log consistency check fails, i.e. different term at prevLogIndex
	cfTerm := rf.logs[args.PrevLogIndex].Term
	if cfTerm != args.PrevLogTerm {
		reply.ConflictTerm = cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.logs[i].Term == cfTerm; i-- {
			reply.ConflictIndex = i
		}
		reply.Success = false
		return
	}

	// only truncate log if an existing entry conflicts with a new one
	// it finds the first place that logs and entries conflict
	i, j := args.PrevLogIndex+1, 0
	for ; i < lastIndex+1 && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.logs[i].Term != args.Entries[j].Term {
			break
		}
	}
	// saves the logs without conflicts
	rf.logs = rf.logs[:i]
	args.Entries = args.Entries[j:]
	// adds new entries
	rf.logs = append(rf.logs, args.Entries...)

	reply.Success = true

	// update commit index to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = rf.getLastIndex()
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}
		go rf.applyLogs()
	}
}

//
// send a AppendEntries RPC to a server.
//

/*
this function can be called by a valid leader.
if the appendEntries query is successfull, matchIndex array of the
leader gets incremented by the length of the entries. otherwise, the
array is updated using the reply.ConflictIndex. If the mentioned is not set,
it iterates over the log and finds it.
*/
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// check if the leader is valid.

	if rf.state != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		return
	}

	// update matchIndex and nextIndex of the follower
	if reply.Success {
		// match index should not regress in case of stale rpc response
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.ConflictTerm < 0 {
		// follower's log shorter than leader's
		rf.nextIndex[server] = reply.ConflictIndex
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		// try to find the conflictTerm in log
		newNextIndex := rf.getLastIndex()
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.logs[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}
		// if not found, set nextIndex to conflictIndex
		if newNextIndex < 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = newNextIndex
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	// if there exists an N such that N > commitIndex, a majority of
	// matchIndex[i] >= N, and log[N].term == currentTerm, set commitIndex = N
	for n := rf.getLastIndex(); n >= rf.commitIndex; n-- {
		count := 1
		if rf.logs[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					count += 1
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogs()
			break
		}
	}
}

//
// broadcast AppendEntries RPCs to all peers in parallel.
// lock must be held before calling this.
//
func (rf *Raft) performAppendEntries() {
	if rf.state != Leader {
		return
	}

	for server := range rf.peers {
		if server != rf.me {
			// leader creates a unique args for each server
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[server] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.LeaderCommit = rf.commitIndex
			entries := rf.logs[rf.nextIndex[server]:] // just the new entries
			args.Entries = make([]LogEntry, len(entries))
			// make a deep copy of the entries to send
			copy(args.Entries, entries)
			go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	term := rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{term, command})
	rf.persist()

	return rf.getLastIndex(), term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// convert the raft state to leader.
//
func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != Candidate {
		return
	}

	rf.resetChannels()
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastIndex() + 1
	// initializing the nextIndex array
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}

	rf.performAppendEntries()
}

//
// convert the raft state to candidate.
//
func (rf *Raft) convertToCandidate(fromState string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// this check is needed to prevent race
	// while waiting on multiple channels
	if rf.state != fromState {
		return
	}

	rf.resetChannels()
	rf.state = Candidate
	// each time a server becomes Candidate, it's term gets incremented
	rf.currentTerm += 1
	// each candidate vote for itself
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	rf.performRequestVote()
}

//
// reset the channels, needed when converting server state.
// lock must be held before calling this.
//
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

//
// main server loop.
// this functions will be called concurrently
//
func (rf *Raft) runServer() {
	// if the server is killed, this functions ends immedeately
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-time.After(120 * time.Millisecond):
				rf.mu.Lock()
				rf.performAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Follower)
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
				// state should already be follower
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				// again converts to Candidate
				rf.convertToCandidate(Candidate)
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// at first each server is a follower
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.logs = append(rf.logs, LogEntry{Term: 0})

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	// start the background server loop
	go rf.runServer()

	return rf
}
