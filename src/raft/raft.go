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
	CommitTimeout      time.Duration
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
	log map[int]LogEntry
	//log sync.Map

	logLock sync.Mutex
	//startLock sync.Mutex

	commitIndex int
	//
	lastApplied int

	nextIndex *myMap

	matchIndex *myMap

	state RaftState

	stateLock sync.Mutex

	lastLogIndex int

	lastLogTerm int

	lastLock sync.Mutex

	applyStartCh chan interface{}

	applyEndCh chan AppLyEndMsg

	commitCh chan ApplyMsg

	applyCh chan ApplyMsg

	config Config
	// lastContact is the last time we had contact from the
	// leader node. This can be used to gauge staleness.
	lastContact     time.Time
	lastContactLock sync.RWMutex

	shutdownCh chan struct{}

	leaderRun bool
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	////log.Printf("rf %d get state %d get voteFor %d  term=%d", rf.me, rf.state, rf.voteFor, rf.currentTerm)
	isleader = rf.getState() == Leader && rf.leaderRun
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
	////log.Printf("rf %d request vote %d %d %d", rf.me, args.Term, args.CandidateId, args.LastLogIndex)
	// Your code here (2A, 2B).
	//rf.lastLock.Lock()
	//defer rf.lastLock.Unlock()
	////log.Printf("rf %d get vote request args.Term=%d  args.CandidateId=%d args.LastLogIndex=%d args.LastLogTerm=%d", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	////log.Printf("rf %d vote request rf me rf.currentTerm=%d rf.voteFor=%d rf.lastLogIndex=%d rf.lastLogTerm=%d", rf.me, rf.currentTerm, rf.voteFor, rf.lastLogIndex, rf.lastLogTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.setState(Follower)
		rf.currentTerm = args.Term
		reply.Term = args.Term
	}

	lastIdx, lastTerm := rf.getLastLog()
	if lastTerm > args.LastLogTerm {
		return
	}
	if lastTerm == args.LastLogTerm && lastIdx > args.LastLogIndex {
		return
	}

	reply.VoteGranted = true
	//if args.Term > rf.currentTerm {
	//	rf.currentTerm = args.Term
	//	if args.LastLogTerm > rf.lastLogTerm ||
	//		(args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.getLastLogIndex()) {
	//		rf.state = Follower
	//		rf.voteFor = args.CandidateId
	//		reply.VoteGranted = true
	//	} else {
	//		reply.VoteGranted = false
	//		return
	//	}
	//}
	rf.persist()
	rf.setLastContact()
}

func (rf *Raft) sendRequestVoted() {
	var voteCount int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = rf.lastLogIndex
		args.LastLogTerm = rf.lastLogTerm
		reply := RequestVoteReply{}
		////log.Printf("%d: send RequestVote to %d", rf.me, i)
		go func(i int) {
			ok := rf.sendRequestVote(i, &args, &reply)
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
					////log.Printf("%d: voteCount = %d\n", rf.me, count)
					if count >= int32(len(rf.peers)>>1+1) {
						rf.setState(Leader)
						////log.Printf("%d: become Leader\n", rf.me)
						//////log.Printf("%d: Leader\n", rf.state)
						rf.persist()
						return
					}
				}
			}
		}(i)
	}
	rf.persist()
	//rf.setLastContact()
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
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	nowIndex := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	////log.Printf("rf %d Start %d get voteFor %d  term=%d", rf.me, rf.state, rf.voteFor, rf.currentTerm)
	if rf.getState() != Leader {
		isLeader = false
		return nowIndex, term, isLeader
	}
	timeOut := 10 * time.Second
	var timer <-chan time.Time
	if timeOut > 0 {
		timer = time.After(timeOut)
	}
	go func() {
		rf.applyStartCh <- command
		////log.Printf("%d: applyStartCh \n", rf.me)
		////log.Printf("rf %d applyStartCh command=%v log=%v nextIndex=%v matchIndex=%v nowIndex=%v", rf.me, command, rf.log, rf.nextIndex, rf.matchIndex, nowIndex)
	}()
	for {
		select {
		case <-timer:
			if rf.getState() != Leader {
				isLeader = false
				return nowIndex, term, isLeader
			}
			return nowIndex, term, isLeader
		case msg := <-rf.applyEndCh:
			////log.Printf("%d: applyEndCh %v\n", rf.me, msg)
			////log.Printf("rf %d applyEndCh command=%v log=%v nextIndex=%v matchIndex=%v nowIndex=%v", rf.me, command, rf.log, rf.nextIndex, rf.matchIndex, nowIndex)
			rf.persist()
			return msg.CommandIndex, msg.CommandTerm, isLeader
		}

	}

	//select {
	//case rf.applyStartCh <- command:
	//	//log.Printf("%d: applyStartCh\n", rf.me)
	////case <-timer:
	////	//log.Printf("%d: applyStartCh timeout\n", rf.me)
	////	return nowIndex, term, isLeader
	//case msg := <-rf.applyEndCh:
	//	//log.Printf("%d: applyEndCh %v\n", rf.me, msg)
	//	//log.Printf("rf %d start command=%v log=%v nextIndex=%v matchIndex=%v nowIndex=%v", rf.me, command, rf.log, rf.nextIndex, rf.matchIndex, nowIndex)
	//	return msg.CommandIndex, msg.CommandTerm, isLeader
	//}
	//
	////go func() {
	////	rf.applyStartCh <- true
	////}()
	//
	//return nowIndex, term, isLeader
}

type AppLyEndMsg struct {
	CommandIndex int
	CommandTerm  int
	IsLeader     bool
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
	////log.Printf("start Make %d", me)
	rf := &Raft{}
	rf.setState(Follower)
	rf.peers = peers
	rf.applyCh = applyCh
	rf.persister = persister
	rf.log = make(map[int]LogEntry)
	rf.me = me
	rf.voteFor = -1
	rf.config = Config{HeartbeatTimeout: 2000 * time.Millisecond, ElectionTimeout: 1000 * time.Millisecond, LeaderLeaseTimeout: 500 * time.Millisecond, CommitTimeout: 50 * time.Millisecond}
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.lastLogIndex = 0
	rf.nextIndex = &myMap{
		data: make(map[int]int, len(rf.peers)),
	}
	rf.matchIndex = &myMap{
		data: make(map[int]int, len(rf.peers)),
	}
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex.storage(i, 1)
		rf.matchIndex.storage(i, 0)
	}
	rf.applyStartCh = make(chan interface{})
	rf.applyEndCh = make(chan AppLyEndMsg)
	rf.commitCh = make(chan ApplyMsg)
	//for i := range rf.nextIndex {
	//	rf.nextIndex[i] = 1
	//}
	//for i := range rf.nextIndex {
	//	rf.nextIndex[i] = 1
	//}
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
				//log.Printf("%d: Leader", rf.me)
				////log.Printf("start Leader me = %d", rf.me)
				rf.leaderRun = true
				rf.runLeader()
				rf.leaderRun = false
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
			b := time.Now().Sub(lastContact) < rf.config.HeartbeatTimeout
			if b {
				continue
			}
			rf.setState(Candidate)
			return
		case <-rf.shutdownCh:
			return
		case <-rf.applyStartCh:
			go func() {
				rf.applyEndCh <- AppLyEndMsg{
					CommandIndex: -1,
					CommandTerm:  -1,
					IsLeader:     false,
				}
			}()
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
	////log.Printf("start Candidate me = %d state = %d", rf.me, state)
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
		case <-rf.applyStartCh:
			go func() {
				rf.applyEndCh <- AppLyEndMsg{
					CommandIndex: -1,
					CommandTerm:  -1,
					IsLeader:     false,
				}
			}()
		}
	}
}

func (rf *Raft) runLeader() {
	//rf.commitIndex = rf.getLastLogIndex() + 1
	//rf.matchIndex =
	//rf.matchIndex = &myMap{
	//	data: make(map[int]int, len(rf.peers)),
	//}
	for i := 0; i < len(rf.matchIndex.data); i++ {
		if i == rf.me {
			rf.matchIndex.storage(i, rf.getLastLogIndex())
		} else {
			rf.matchIndex.storage(i, 0)
		}
	}
	//rf.nextIndex = &myMap{
	//	data: make(map[int]int, len(rf.peers)),
	//}

	//next 下标对leader本身来说不需要维护，leader只需要维护提交的下标，next是leader对跟随者来说的下一个log copy startIndex
	for i := 0; i < len(rf.nextIndex.data); i++ {
		if i == rf.me {
			continue
		}
		rf.nextIndex.storage(i, rf.getLastLogIndex()+1)
	}
	//rf.nextIndex.storage(rf.me, rf.getLastLogIndex()+1)
	////log.Printf("runleader log=%v", rf.log)
	rf.setState(Leader)
	heartbeatTimer := randomTimeout(rf.config.HeartbeatTimeout / 10)
	//go func() {
	//
	//}()
	for rf.getState() == Leader {
		select {
		case command := <-rf.applyStartCh:
			////log.Printf("%d:applyStartCh", rf.me)
			go func() {
				rf.apply(command)
			}()
		//	这个是为了把commitIndex再次传给跟随者，否则没有地方刷新commitIndex
		case <-randomTimeout(rf.config.CommitTimeout):
			go func() {
				rf.sendAppendEntries(false)
			}()
		case <-heartbeatTimer:
			heartbeatTimer = randomTimeout(rf.config.HeartbeatTimeout / 10)
			if rf.getState() != Leader {
				return
			}
			go func() {
				rf.heart()
			}()
		case <-rf.shutdownCh:
			return
		}
	}
}

func (rf *Raft) heart() {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	rf.sendAppendEntries(true)
	//rf.setLastContact()
}

// randomTimeout returns a value that is between the minVal and 2x minVal.
func randomTimeout(minVal time.Duration) <-chan time.Time {
	if minVal == 0 {
		return nil
	}
	extra := (time.Duration(rand.Int63()) % minVal)
	return time.After(minVal + extra)
}

type LogEntry struct {
	Term    int
	Index   int
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
	Term    int
	LastLog int
	Success bool
	//ConflictIndex int
}

func (rf *Raft) sendAppendEntries(heartbeat bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastIndex := rf.getLastLogIndex()
	var appendCount int32 = 1
	copylog := copyLog(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			//rf.logLock.Lock()
			//defer rf.logLock.Unlock()
			//rf.startLock.Lock()
			//defer rf.startLock.Unlock()
		RE:
			//rf.log := rf.log

			if rf.getState() != Leader {
				return
			}
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me

			//args.PrevLogIndex = rf.getLastLogIndex()

			if !heartbeat {
				nextIndex := rf.nextIndex.load(i)
				preIndex := max(nextIndex, 0)
				args.PrevLogTerm = rf.getLogTerm(preIndex)
				if preIndex == 1 {
					preIndex = 0
					args.PrevLogTerm = 0
				} else {
					entry := copylog[nextIndex-1]
					preIndex = entry.Index
					args.PrevLogTerm = entry.Term
				}
				args.PrevLogIndex = preIndex
				args.Entries = make([]LogEntry, 0)

				for j := rf.nextIndex.load(i); j <= lastIndex; j++ {
					//args.Entries[j] = rf.log[j]
					args.Entries = append(args.Entries, copylog[j])
				}
				args.LeaderCommit = rf.commitIndex
			}
			////log.Printf("sendAppendEntries rf.me=%v send to server=%v args=%v rf.log=%v", rf.me, i, args, rf.log)
			reply := AppendEntriesReply{}
			////log.Printf("sendAppendEntries rf.me=%v send to server=%v args=%v needOk=%v rf.matchIndex=%v rf.nextIndex=%v ", rf.me, i, args, len(rf.peers)>>1+1, rf.matchIndex, rf.nextIndex)
			ok := rf.sendAppendEntries0(i, &args, &reply)
			if ok {
				if reply.Success {
					if len(args.Entries) <= 0 {
						return
					}
					last := args.Entries[len(args.Entries)-1]
					rf.matchIndex.storage(i, last.Index)
					rf.nextIndex.storage(i, last.Index+1)
					atomic.AddInt32(&appendCount, 1)
					count := atomic.LoadInt32(&appendCount)
					////log.Printf("sendAppendEntries rf.me=%v send to ok !! server=%v args=%v okCount=%v needOk=%v rf.matchIndex=%v rf.nextIndex=%v", rf.me, i, args, count, len(rf.peers)>>1+1, rf.matchIndex, rf.nextIndex)
					if count >= int32(len(rf.peers)>>1+1) {
						//rf.commitIndex = rf.matchIndex.load(rf.me)
						preCommit := max(rf.commitIndex, 1)
						//lastLogIndex := rf.getLastLogIndex()
						rf.commitIndex = lastIndex
						rf.lastLogTerm = rf.currentTerm
						for j := preCommit; j <= lastIndex; j++ {
							msg := ApplyMsg{
								CommandValid:  true,
								Command:       rf.log[j].Command,
								CommandIndex:  j,
								SnapshotValid: false,
								Snapshot:      nil,
								SnapshotTerm:  0,
								SnapshotIndex: 0,
							}
							//log.Printf("lader comit msg=%v", msg)
							rf.applyCh <- msg
						}

					}
				} else {
					if rf.getState() != Leader {
						return
					}
					if reply.Term > rf.currentTerm {
						//rf.Kill()
						//rf.startLock.Lock()
						//defer rf.startLock.Unlock()
						if rf.getState() == Leader {
							//log.Printf("me=%v reply.Term=%v  rf.currentTerm=%v come on server=%v  set state follower", rf.me, reply.Term, rf.currentTerm, i)
							rf.setState(Follower)
						}
						return
					}
					if heartbeat {
						return
					}
					//rf.matchIndex.storage(i, rf.matchIndex.load(i)-1)
					//rf.nextIndex.storage(i, rf.nextIndex.load(i)-1)
					rf.nextIndex.storage(i, max(1, min(rf.nextIndex.load(i)-1, reply.LastLog+1)))
					goto RE
				}
				//if heartbeat {
				//rf.setLastContact()
				//}
			}
		}(i)
	}
}

func copyLog(c map[int]LogEntry) map[int]LogEntry {
	m := make(map[int]LogEntry)
	for k, v := range c {
		m[k] = v
	}
	return m
}

func (m *myMap) storage(k int, v int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data[k] = v
}
func (m *myMap) load(k int) int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.data[k]
}
func (m *myMap) len() int {
	m.lock.Lock()
	defer m.lock.Unlock()
	return len(m.data)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	////log.Printf("me = %d  args=%v  rf.log=%v rf=%v", rf.me, args, rf.log)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	lastLogIndex := rf.getLastLogIndex()
	//reply.Term = lastLogIndex
	reply.Success = false
	////log.Printf("AppendEntries me=%v args=%v args.Entries=%v ,rf.log=%v ,rf.commit=%v rf.currentTerm=%v ,rf.lastLogIndex=%v", rf.me, args, args.Entries, rf.log, rf.commitIndex, rf.currentTerm, lastLogIndex)

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm || rf.getState() != Follower {
		rf.setState(Follower)
		rf.currentTerm = args.Term
		reply.Term = args.Term
	}
	rf.voteFor = args.LeaderId

	////log.Printf("AppendEntries me=%v args=%v args.Entries=%v ,rf.log=%v ,rf.commit=%v", rf.me, args, args.Entries, rf.log, rf.commitIndex)
	if args.PrevLogIndex > 0 {
		lastIdx, lastTerm := rf.getLastLog()
		var prevLogTerm int
		if args.PrevLogIndex == lastIdx {
			prevLogTerm = lastTerm

		} else {
			prevLog, ok := rf.log[args.PrevLogIndex]
			if !ok {
				return
			}
			prevLogTerm = prevLog.Term
		}

		if args.PrevLogTerm != prevLogTerm {
			return
		}
	}
	if n := len(args.Entries); n > 0 {
		var newEntries []LogEntry
		for i, entry := range args.Entries {
			if entry.Index > lastLogIndex {
				newEntries = args.Entries[i:]
				break
			}

			if storeEntry, ok := rf.log[entry.Index]; !ok {
				return
			} else if storeEntry.Term != entry.Term {
				for j := entry.Index; j <= lastLogIndex; j++ {
					delete(rf.log, j)
				}
				newEntries = args.Entries[i:]
				break
			}
		}

		if l := len(newEntries); l > 0 {
			for _, v := range newEntries {
				rf.log[v.Index] = v
			}
			last := args.Entries[n-1]
			rf.setLastLog(last.Index, last.Term)
		}

	}
	////log.Printf("AppendEntries me=%v args=%v args.Entries=%v ,rf.log=%v ,rf.commit=%v", rf.me, args, args.Entries, rf.log, rf.commitIndex)
	if args.LeaderCommit > 0 && args.LeaderCommit > rf.commitIndex {
		idx := min(args.LeaderCommit, rf.getLastLogIndex())
		rf.commitIndex = idx
		rf.processLogs(idx)
	}
	rf.setLastContact()
	reply.Success = true
}

func (rf *Raft) processLogs(idx int) {
	lastApplied := rf.lastApplied
	if idx <= lastApplied {
		return
	}
	for index := lastApplied + 1; index <= idx; index++ {
		//rf.commitIndex = min(rf.getLastLogIndex(), args.LeaderCommit)
		msg := ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[index].Command,
			CommandIndex:  index,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		////log.Printf("processLogs rf.me=%v rf.log=%v msg=%v", rf.me, rf.log, msg)
		//log.Printf("processLogs rf.lastApplied=%v rf.me=%v rf.log=%v msg=%v", lastApplied, rf.me, rf.log, msg)
		rf.applyCh <- msg
	}
	//rf.lastLogTerm = rf.log[rf.lastLogIndex].Term
	rf.lastApplied = idx

}

func min(m1 int, m2 int) int {
	if m1 > m2 {
		return m2
	}
	return m1
}
func (r *Raft) getLastLog() (int, int) {
	r.lastLock.Lock()
	defer r.lastLock.Unlock()
	return r.lastLogIndex, r.lastLogTerm
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

func (rf *Raft) apply(command interface{}) (term int, nextIndex int, isLeader bool) {
	rf.logLock.Lock()
	term = rf.currentTerm
	rf.lastLock.Lock()
	rf.lastLogIndex++
	rf.lastLock.Unlock()
	nextIndex = rf.getLastLogIndex()
	//nowIndex = rf.getLastLogIndex()
	rf.log[rf.lastLogIndex] = LogEntry{
		Term:    term,
		Index:   nextIndex,
		Command: command,
	}
	//rf.nextIndex.storage(rf.me, nextIndex+1)
	rf.matchIndex.storage(rf.me, nextIndex)
	//rf.nextIndex.storage(rf.me,)
	rf.setLastLog(nextIndex, term)
	//rf.lastLogIndex = nextIndex
	//rf.lastLogTerm = term
	//rf.lastApplied = nowIndex
	rf.logLock.Unlock()
	go func() {
		rf.applyEndCh <- AppLyEndMsg{
			CommandIndex: nextIndex,
			CommandTerm:  term,
			IsLeader:     true,
		}
	}()
	//go func() {
	rf.sendAppendEntries(false)
	//}()
	return term, nextIndex, true
}

func (rf *Raft) getLastLogIndex() int {
	rf.lastLock.Lock()
	defer rf.lastLock.Unlock()
	return rf.lastLogIndex
}

func (rf *Raft) getLogTerm(index int) int {
	rf.logLock.Lock()
	defer rf.logLock.Unlock()
	if index < 0 {
		return 0
	}
	return rf.log[index].Term
}
func (rf *Raft) sendAppendEntries0(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type myMap struct {
	data map[int]int
	lock sync.Mutex
}

func max(i, j int) int {
	if i < j {
		return j
	}
	return i
}

//func (r *Raft) getCurrentTerm() uint64 {
//	return atomic.LoadUint64(&r.currentTerm)
//}
//
//func (r *Raft) setCurrentTerm(term uint64) {
//	atomic.StoreInt32(&r.currentTerm, term)
//}
