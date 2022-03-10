package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

type Config struct {
	// HeartbeatTimeout specifies the time in follower state without
	// a leader before we attempt an election.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
}

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
	currentTerm int

	voteFor int
	//voteCount int
	log []LogEntry

	commitIndex int

	lastApplied int

	nextIndex []int

	matchIndex []int

	state RaftState

	stateLock sync.Mutex

	lastLogIndex int

	lastLogTerm int

	lastLock sync.Mutex

	config Config
	// lastContact is the last time we had contact from the
	// leader node. This can be used to gauge staleness.
	lastContact     time.Time
	lastContactLock sync.RWMutex

	shutdownCh chan struct{}
}
type RaftState uint32

const (
	// Follower is the initial state of a Raft node.
	Follower RaftState = iota

	// Candidate is one of the valid states of a Raft node.
	Candidate

	// Leader is one of the valid states of a Raft node.
	Leader

	// Shutdown is the terminal state of a Raft node.
	Shutdown
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	//log.Printf("rf %d get state %d get voteFor %d  term=%d", rf.me, rf.state, rf.voteFor, rf.currentTerm)
	state := rf.getState()
	isleader = state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//log.Printf("rf %d request vote %d %d %d", rf.me, args.Term, args.CandidateId, args.LastLogIndex)
	// Your code here (2A, 2B).
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()
	//log.Printf("rf %d get vote request args.Term=%d  args.CandidateId=%d args.LastLogIndex=%d args.LastLogTerm=%d", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	//log.Printf("rf %d vote request rf me rf.currentTerm=%d rf.voteFor=%d rf.lastLogIndex=%d rf.lastLogTerm=%d", rf.me, rf.currentTerm, rf.voteFor, rf.lastLogIndex, rf.lastLogTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if args.LastLogTerm > rf.lastLogTerm ||
			(args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex) {
			rf.state = Follower
			rf.voteFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	}

	//// Your code here (2A, 2B).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//requestTerm := args.Term
	//if requestTerm < rf.currentTerm {
	//	reply.Term = rf.currentTerm
	//	reply.VoteGranted = false
	//	return
	//}
	//if requestTerm > rf.currentTerm {
	//	rf.setState(Follower)
	//	rf.currentTerm = requestTerm
	//	reply.Term = requestTerm
	//	rf.voteFor = -1
	//}
	//
	//voteFor := rf.voteFor
	//if rf.voteFor == -1 || voteFor == args.CandidateId {
	//	reply.Term = args.CandidateId
	//	reply.VoteGranted = true
	//	//return
	//}
	////rf.voteFor = args.CandidateId
	////reply.VoteGranted = true
	rf.persist()
	rf.setLastContact()
}

// setLastContact is used to set the last contact time to now
func (r *Raft) setLastContact() {
	r.lastContactLock.Lock()
	r.lastContact = time.Now()
	r.lastContactLock.Unlock()
}

func (r *Raft) LastContact() time.Time {
	r.lastContactLock.RLock()
	last := r.lastContact
	r.lastContactLock.RUnlock()
	return last
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
	}
	rf.shutdownCh <- struct{}{}
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
	//log.Printf("start Make %d", me)
	rf := &Raft{}
	rf.setState(Follower)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.voteFor = -1
	rf.config = Config{HeartbeatTimeout: 2000 * time.Millisecond, ElectionTimeout: 1000 * time.Millisecond, LeaderLeaseTimeout: 500 * time.Millisecond}
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.lastLogIndex = -1
	go func() {
		for {
			//log.Printf("me: %d: state: %d", rf.me, rf.getState())
			switch rf.getState() {
			case Follower:
				//log.Printf("%d:start Follower", rf.me)
				//println("start Follower me = ", rf.me)
				rf.runFollower()
			case Candidate:
				//log.Printf("%d:start Candidate", rf.me)
				//println("start Candidate me = ", rf.me)
				rf.runCandidate()
			case Leader:
				////log.Printf("%d: Leader", rf.me)
				//log.Printf("start Leader me = %d", rf.me)
				rf.runLeader()
			}

		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) runFollower() {
	heartbeatTimer := randomTimeout(rf.config.HeartbeatTimeout)
	//println("heartbeatTimer:", rf.config.HeartbeatTimeout)
	for rf.getState() == Follower {
		select {
		case <-heartbeatTimer:
			heartbeatTimer = randomTimeout(rf.config.HeartbeatTimeout)
			lastContact := rf.LastContact()
			if time.Now().Sub(lastContact) < rf.config.HeartbeatTimeout {
				continue
			}
			rf.setState(Candidate)
			return
		case <-rf.shutdownCh:
			return
		}

	}

}

func (rf *Raft) runCandidate() {
	//rf.setState(Candidate)
	rf.currentTerm++
	rf.voteFor = rf.me
	//rf.voteCount = 1
	rf.persist()
	rf.sendRequestVoted()
	state := rf.getState()
	//log.Printf("start Candidate me = %d state = %d", rf.me, state)
	electionTimeout := randomTimeout(rf.config.ElectionTimeout)
	for state == Candidate {
		select {
		case <-electionTimeout:
			electionTimeout = randomTimeout(rf.config.ElectionTimeout)
			return
			//if state != Candidate {
			//	return
			//}
			//rf.currentTerm++
			//rf.voteFor = rf.me
			//rf.sendRequestVoted()
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) runLeader() {
	rf.setState(Leader)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	heartbeatTimer := randomTimeout(rf.config.HeartbeatTimeout / 10)
	for rf.getState() == Leader {
		select {
		case <-heartbeatTimer:
			heartbeatTimer = randomTimeout(rf.config.HeartbeatTimeout / 10)
			if rf.getState() != Leader {
				return
			}
			rf.sendAppendEntries()
			rf.setLastContact()
		case <-rf.shutdownCh:
			return
		}
	}
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}
func (rf *Raft) sendRequestVoted() {
	var voteCount int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		//go func(i int) {
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.lastLogIndex
		args.LastLogTerm = rf.lastLogTerm
		reply := RequestVoteReply{}
		//log.Printf("%d: send RequestVote to %d", rf.me, i)
		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
			//count := 0
			//TAG:
			if ok {
				if reply.Term > rf.currentTerm {
					rf.setState(Follower)
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.persist()
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
					count := atomic.LoadInt32(&voteCount)
					//log.Printf("%d: voteCount = %d\n", rf.me, count)
					if count >= int32(len(rf.peers)>>1+1) {
						rf.setState(Leader)
						//log.Printf("%d: become Leader\n", rf.me)
						////log.Printf("%d: Leader\n", rf.state)
						rf.persist()
						return
					}
				}
			}
		}(i)
	}
	//}(i)

	//rf.setState(Follower)
	//rf.voteFor = -1
	rf.persist()
	rf.setLastContact()
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
}

func (rf *Raft) sendAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
			args.Entries = rf.log[rf.nextIndex[i]:]
			args.LeaderCommit = rf.commitIndex
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries0(i, &args, &reply)
			if ok {
				if reply.Success {
					rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[i] = rf.matchIndex[i] + 1
				} else {
					rf.nextIndex[i] = reply.ConflictIndex
				}
			}
		}(i)
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.lastLogIndex
}

func (rf *Raft) getLogTerm(index int) int {
	if index >= len(rf.log) || index < 0 {
		return -1
	}
	return rf.log[index].Term
}

func (rf *Raft) sendAppendEntries0(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.state != Follower {
		rf.setState(Follower)
		rf.currentTerm = args.Term
		reply.Term = args.Term
	}
	rf.voteFor = args.LeaderId
	if n := len(args.Entries); n > 0 {
		index := rf.lastLogIndex
		var newEntries []LogEntry
		newEntries = args.Entries[index:]
		if len(newEntries) > 0 {
			rf.log = append(rf.log, newEntries...)
		}
		last := newEntries[n-1]
		rf.setLastLog(len(rf.log)-1, last.Term)
	}

	reply.Success = true
	rf.setLastContact()
}

func (r *Raft) setLastLog(index, term int) {
	r.lastLock.Lock()
	r.lastLogIndex = index
	r.lastLogTerm = term
	defer r.lastLock.Unlock()
}
func (r *Raft) getState() RaftState {
	r.stateLock.Lock()
	defer r.stateLock.Unlock()
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *Raft) setState(s RaftState) {
	r.stateLock.Lock()
	defer r.stateLock.Unlock()
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

//func (r *Raft) getCurrentTerm() uint64 {
//	return atomic.LoadUint64(&r.currentTerm)
//}
//
//func (r *Raft) setCurrentTerm(term uint64) {
//	atomic.StoreInt32(&r.currentTerm, term)
//}
