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
	"sync"
)
import "labrpc"
import "math/rand"
import "time"
import "fmt"
import "bytes"
import "labgob"

const (
	Follower = iota
	Candidate
	Leader
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
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
}

type entries struct {
	Command interface{}
	Term    int
	Index   int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	//incs  	  bool
	//rpcnum	  int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state
	currentTerm int
	votedFor    int
	log         []entries
	timestamp   int64
	//volatile state
	state       int
	commitIndex int
	lastApplied int

	//leaders
	nextIndex  []int
	matchIndex []int

	//channel
	chanAppendEntries chan int
	chanvoteGranted   chan int
	chanLeader        chan int
	chanApplyMsg      chan ApplyMsg
	chanCommit        chan int
	chanNewLog		  chan int
	chanCanApply	  chan int

	maxraftstate	  int
	//chanCopy		  chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

func (rf *Raft) GetLen() int {
	return len(rf.log) - 1
}

func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()
	res := rf.commitIndex
	rf.mu.Unlock()
	return res
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log) //only three presistent state on all server
	e.Encode(rf.timestamp)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var curt int
	var vote int
	var llog []entries
	var tm   int64
	if d.Decode(&curt) != nil || d.Decode(&vote) != nil || d.Decode(&llog) != nil || d.Decode(&tm) != nil {
		fmt.Println("server ", rf.me, " readPersist wrong!")
	} else {
		rf.mu.Lock()
		rf.currentTerm = curt
		rf.ClearChange()
		rf.votedFor = vote
		rf.log = llog
		rf.timestamp = tm
		rf.mu.Unlock()
	}
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entries

	LeaderCommit int
	TimeStamp	 int64
}

type AppendEntriesReply struct {
	Term      int
	PrevIndex int
	Success   bool
	ErrTimeout bool
	LogTooLong bool
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte
	TimeStamp		 int64
}

type InstallSnapshotReply struct {
	Term int
	ErrTimeout	bool
}

//
// example RequestVote RPC handler.
//

func min(x int, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

func (rf *Raft) ClearChange() {
	rf.chanvoteGranted = make(chan int, 10000)
	rf.chanAppendEntries = make(chan int, 10000)
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ClearChange()
		rf.state = Follower
		rf.votedFor = -1

	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term >= rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if args.LastLogTerm > rf.log[rf.GetLen()].Term || (args.LastLogTerm == rf.log[rf.GetLen()].Term && args.LastLogIndex >= rf.log[rf.GetLen()].Index) {
				rf.chanvoteGranted <- 1
				reply.VoteGranted = true
				rf.state = Follower
				rf.votedFor = args.CandidateId
				//fmt.Println(rf.me, "voted for ", rf.votedFor)
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//have no entries
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ClearChange()
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.PrevIndex = args.PrevLogIndex
	reply.ErrTimeout = false
	reply.LogTooLong = false
	if args.Term >= rf.currentTerm {
		rf.chanAppendEntries <- 1
		if args.TimeStamp <= rf.timestamp {
			//this rpc is timeout, not in order
			fmt.Println(rf.me, "this append entries rpc is timeout")
			reply.ErrTimeout = true
			return
		}
		rf.timestamp = args.TimeStamp
		FirstIndex := rf.log[0].Index
		NowIndex := args.PrevLogIndex - FirstIndex
		if NowIndex < 0 {
			reply.PrevIndex = FirstIndex
			return
		}
		// TODO
		// add check raftstate
		if rf.maxraftstate != -1 && rf.GetStateSize() >= rf.maxraftstate {
			reply.LogTooLong = true
			return
		}

		if rf.GetLen() < NowIndex || rf.log[NowIndex].Term != args.PrevLogTerm {
			reply.Success = false
			if rf.GetLen() >= NowIndex {
				tmp := NowIndex
				for rf.log[tmp].Term == rf.log[NowIndex].Term && tmp > 0 {
					tmp--
				}
				reply.PrevIndex = rf.log[tmp].Index
			} else {
				reply.PrevIndex = rf.log[rf.GetLen()].Index
			}
		} else {
			reply.Success = true
			if rf.GetLen() >= NowIndex+1 {
				rf.log = rf.log[:NowIndex+1]
			}
			rf.log = append(rf.log, args.Entries...) //append a slice
			// if len(args.Entries) > 0{
			//  fmt.Println(rf.log)
			// }
			if rf.commitIndex < min(args.LeaderCommit, rf.log[rf.GetLen()].Index) { //rules for followers to increase commitIndex
				rf.commitIndex = min(args.LeaderCommit, rf.log[rf.GetLen()].Index)
				rf.chanCommit <- 1
				//fmt.Println("commitIndex: ", rf.me, rf.commitIndex)
			}
			fmt.Println(rf.me, "append entries, last commit index ", rf.commitIndex, "last apply index ", rf.lastApplied)
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.ClearChange()
		rf.votedFor = -1
		rf.state = Follower
	}
	reply.Term = rf.currentTerm
	reply.ErrTimeout = false

	rf.chanAppendEntries <- 1

	if args.TimeStamp <= rf.timestamp {
		//this rpc is timeout, not in order
		fmt.Println(rf.me, "this install snapshot rpc is timeout")
		reply.ErrTimeout = true
		rf.mu.Unlock()
		return
	}
	rf.timestamp = args.TimeStamp

	FirstIndex := rf.log[0].Index
	NowIndex := args.LastIncludeIndex - FirstIndex
	if NowIndex < 0 {
		rf.mu.Unlock()
		return
	}
	if rf.GetLen() >= NowIndex && rf.log[NowIndex].Term == args.LastIncludeTerm {
		rf.log = rf.log[NowIndex:]
	} else {
		rf.log = make([]entries, 0)
		rf.log = append(rf.log, entries{0, args.LastIncludeTerm, args.LastIncludeIndex})
	}
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Snapshot)
	msg := ApplyMsg{CommandValid: false, Snapshot: args.Snapshot}
	rf.lastApplied = args.LastIncludeIndex
	rf.commitIndex = rf.lastApplied
	fmt.Println(rf.me, "install snapshot, last commit index ", rf.commitIndex, "last apply index ", rf.lastApplied)
	rf.mu.Unlock()
	rf.chanCanApply <- 1
	rf.chanApplyMsg <- msg
	<- rf.chanCanApply
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	//newlog := false
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader = rf.GetState()
	if isLeader {
		index = rf.log[rf.GetLen()].Index + 1
		//fmt.Println("a log entry size is: ", unsafe.Sizeof(entries{command, term, index}))
		rf.log = append(rf.log, entries{command, term, index})
		if len(rf.chanNewLog) == 0 {
			rf.chanNewLog <- 1
		}
		//newlog = true
		rf.persist()
	}
	//if newlog {
	//	// a new log append, so need to send heartbeat
	//	go rf.allAppendEntries()
	//}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//fmt.Println(rf.me, " send rpc: ", rf.rpcnum)
}

func (rf *Raft) allRequestVote() {
	//fmt.Println(rf.me, "becomes candidate, term is", rf.currentTerm)
	count := 1
	rf.mu.Lock()
	args := &RequestVoteArgs{rf.currentTerm, rf.me, rf.log[rf.GetLen()].Index, rf.log[rf.GetLen()].Term}
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == Candidate { //must candidate can send requestvote
			go func(i int) {
				reply := &RequestVoteReply{}

				ok := rf.sendRequestVote(i, args, reply)
				rf.mu.Lock()
				//rf.rpcnum++
				defer rf.mu.Unlock()
				if ok == true && rf.state == Candidate {
					if args.Term != rf.currentTerm { //candidate timeout and start a new election
						return
					}
					if reply.Term > rf.currentTerm { //candidate's term is out of date
						rf.state = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.ClearChange()
						rf.persist()
						return
					}
					if reply.VoteGranted == true {
						count += 1
						if count == len(rf.peers)/2+1 {
							if rf.state == Candidate { //maybe rf hasn't been a candidate because of receiving leader's rpc
								//fmt.Println(rf.me, "become leader")
								rf.chanLeader <- 1
							}
						}
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) updateCommit() {
	N := rf.commitIndex
	FirstIndex := rf.log[0].Index
	for i := max(rf.commitIndex+1, FirstIndex+1); i <= rf.log[rf.GetLen()].Index; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me {
				if rf.matchIndex[j] >= i {
					if rf.log[i-FirstIndex].Term == rf.currentTerm {
						num++
					}
				}
			}
		}
		if num > len(rf.peers)/2 {
			N = i
		}
		//else {
		//	break
		//}
	}
	if N > rf.commitIndex && rf.state == Leader {
		rf.commitIndex = min(N, rf.log[rf.GetLen()].Index)
		fmt.Println("leader is: ", rf.me, "now commit index is: ", rf.commitIndex)
		rf.chanCommit <- 1
	}
}
//
//func (rf *Raft) setInCS() {
//	rf.incs = true
//}
//
//func (rf *Raft) setNotInCS() {
//	rf.incs = false
//}

func (rf *Raft) allAppendEntries() {
	//rules for leaders to increase the commitIndex
	//if(rf.incs == true) {
	//	return
	//}
	rf.mu.Lock()
	//rf.setInCS()
	defer rf.mu.Unlock()
	//defer rf.setNotInCS()
	FirstIndex := rf.log[0].Index
	rf.timestamp = time.Now().UnixNano()
	//N := rf.commitIndex
	//for i := max(rf.commitIndex+1, FirstIndex+1); i <= rf.log[rf.GetLen()].Index; i++ {
	//	num := 1
	//	for j := range rf.peers {
	//		if j != rf.me {
	//			if rf.matchIndex[j] >= i {
	//				if rf.log[i-FirstIndex].Term == rf.currentTerm {
	//					num++
	//				}
	//			}
	//		}
	//	}
	//	if num > len(rf.peers)/2 {
	//		N = i
	//	}
	//}
	//if N > rf.commitIndex && rf.state == Leader {
	//	rf.commitIndex = min(N, rf.log[rf.GetLen()].Index)
	//	rf.chanCommit <- 1
	//}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.state == Leader {
			if rf.nextIndex[i] > FirstIndex {
				etr := make([]entries, 0)
				//fmt.Println(i, etr)
				tmpIndex := min(max(0, rf.nextIndex[i]-1), rf.log[rf.GetLen()].Index)
				if tmpIndex < rf.log[rf.GetLen()].Index {
					etr = rf.log[tmpIndex+1-FirstIndex:]
				}
				args := &AppendEntriesArgs{rf.currentTerm, rf.me, tmpIndex, rf.log[tmpIndex-FirstIndex].Term, etr, rf.commitIndex, rf.timestamp}
				go func(args *AppendEntriesArgs, i int) {
					reply := &AppendEntriesReply{}
					//fmt.Println(rf.me)
					//fmt.Println(rf.me, i)

					ok := rf.peers[i].Call("Raft.AppendEntries", args, reply)
					rf.mu.Lock()
					//rf.rpcnum++
					defer rf.mu.Unlock()
					//defer rf.persist()
					if ok == true && rf.state == Leader {
						if args.Term != rf.currentTerm {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.ClearChange()
							rf.persist()
							return
						}
						if reply.ErrTimeout == true {
							//rf.persist()
							return
						}
						if reply.LogTooLong == true {
							// follow log to long, send snapshot
							rf.nextIndex[i] = 1
							return
						}
						if reply.Success == true && rf.state == Leader {
							if len(args.Entries) > 0 {
								rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
								rf.matchIndex[i] = rf.nextIndex[i] - 1
								if rf.matchIndex[i] > rf.commitIndex {
									rf.updateCommit()
								}
							}
						} else if rf.state == Leader {
							if rf.nextIndex[i] > reply.PrevIndex+1 {
								rf.nextIndex[i] = reply.PrevIndex + 1
							} else {
								rf.nextIndex[i] = max(rf.nextIndex[i]-1, 1)
							}
						}
					}
				}(args, i)
			} else {
				//snapshot
				fmt.Println(rf.me, "send snapshot to ", i, "firstindex is ", FirstIndex)
				args := &InstallSnapshotArgs{rf.currentTerm, rf.me, rf.log[0].Index, rf.log[0].Term, rf.persister.ReadSnapshot(), rf.timestamp}
				go func(args *InstallSnapshotArgs, i int) {
					reply := &InstallSnapshotReply{}

					ok := rf.peers[i].Call("Raft.InstallSnapshot", args, reply)
					rf.mu.Lock()
					//rf.rpcnum++
					defer rf.mu.Unlock()
					//defer rf.persist()
					if ok == true && rf.state == Leader {
						if args.Term != rf.currentTerm {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.state = Follower
							rf.votedFor = -1
							rf.currentTerm = reply.Term
							rf.ClearChange()
							rf.persist()
							return
						}
						if reply.ErrTimeout == true {
							//rf.persist()
							return
						}
						rf.nextIndex[i] = args.LastIncludeIndex + 1
						rf.matchIndex[i] = rf.nextIndex[i] - 1
					}
				}(args, i)
			}
		}
	}
}

func (rf *Raft) SaveSnapshot(index int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	FirstIndex := rf.log[0].Index
	if index < FirstIndex {
		return
	}
	fmt.Println(rf.me, "do snapshot, trucate index is: ", index, "current commit id is ", rf.commitIndex, "apply id is ", rf.lastApplied)
	rf.log = rf.log[index-FirstIndex:]
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), data)
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
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

func (rf *Raft) doStateChange() {
	for {
		rf.mu.Lock()
		st := rf.state
		rf.mu.Unlock()
		switch st {
		case Follower:
			select {
			case <-rf.chanvoteGranted: //receive requestVote rpc
				rf.mu.Lock()
				rf.ClearChange()
				rf.mu.Unlock()
			case <-rf.chanAppendEntries: //receive AppendEntries rpc
				rf.mu.Lock()
				rf.ClearChange()
				rf.mu.Unlock()
			case <-time.After(time.Millisecond * time.Duration(250+rand.Int31n(500))): //election timeout
				rf.mu.Lock()
				if len(rf.chanvoteGranted) == 0 && len(rf.chanAppendEntries) == 0 {
					rf.state = Candidate
					rf.currentTerm += 1 //every election start term += 1
					rf.votedFor = rf.me
					rf.persist()
				}
				rf.ClearChange()
				rf.mu.Unlock()
			}
		case Candidate:
			go rf.allRequestVote() //send rpc
			select {
			case <-time.After(time.Millisecond * time.Duration(250+rand.Int31n(500))): //election timeout and start a new election
				rf.mu.Lock()
				if len(rf.chanvoteGranted) == 0 && len(rf.chanAppendEntries) == 0 {
					rf.state = Candidate
					rf.currentTerm += 1 //every election start term += 1
					rf.votedFor = rf.me
					rf.persist()
				}
				rf.ClearChange()
				rf.mu.Unlock()
			case <-rf.chanAppendEntries: // receive leader rpc and convert to follower
				rf.mu.Lock()
				rf.state = Follower
				rf.ClearChange()
				rf.mu.Unlock()
			case <-rf.chanLeader: // receive more than majority vote
				rf.mu.Lock()
				rf.state = Leader
				//fmt.Println(rf.me, "become leader, term is", rf.currentTerm)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = rf.log[rf.GetLen()].Index + 1
					rf.matchIndex[i] = 0
				}
				rf.mu.Unlock()
			}
		case Leader:
			go rf.allAppendEntries()
			time.Sleep(time.Duration(5) * time.Millisecond)
			select {
			case <- rf.chanNewLog:
			case <-time.After(time.Duration(50) * time.Millisecond):
			}
		}
	}
}

//func (rf *Raft) doCommit() {
//	for {
//		select {
//		case <- rf.chanCopy:
//			rf.mu.Lock()
//			rf.updateCommit()
//			rf.chanCopy = make(chan int, 10000)
//			rf.mu.Unlock()
//		}
//	}
//}

func (rf *Raft) doApply() {
	for {
		select {
		case <-rf.chanCommit:
			//fmt.Println("raft apply: ", rf.me, rf.lastApplied)
			//for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log[rf.GetLen()].Index{
			for {
				rf.mu.Lock()
				if !(rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.log[rf.GetLen()].Index) {
					rf.mu.Unlock()
					break
				}
				FirstIndex := rf.log[0].Index
				if rf.lastApplied+1 >= FirstIndex {
					index := min(rf.lastApplied + 1 - FirstIndex, rf.GetLen())
					msg := ApplyMsg{CommandValid: true, Command: rf.log[index].Command, CommandIndex: rf.lastApplied + 1}
					fmt.Println(rf.me, " want to apply index is: ", rf.lastApplied+1, "raft commit index is: ", rf.commitIndex, "msg is: ", msg)
					rf.lastApplied++
					rf.mu.Unlock()
					//can't lock when send in channel, dead lock
					rf.chanCanApply <- 1
					rf.chanApplyMsg <- msg
					<- rf.chanCanApply
					rf.mu.Lock()
				}
				//rf.lastApplied = min(rf.lastApplied + 1, rf.commitIndex)
				rf.mu.Unlock()
				//fmt.Println(rf.me, "lastApplied", rf.lastApplied, rf.commitIndex)
			}
		}
	}
}

func (rf *Raft) SetMaxRaftState(raftstate int){
	rf.maxraftstate = raftstate
}

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
	rf.timestamp = 0
	rf.log = make([]entries, 0)
	rf.log = append(rf.log, entries{0, 0, 0})

	rf.chanvoteGranted = make(chan int, 10000)
	rf.chanAppendEntries = make(chan int, 10000)
	rf.chanLeader = make(chan int, 10000)
	rf.chanApplyMsg = applyCh
	rf.chanCommit = make(chan int, 10000)
	rf.chanNewLog = make(chan int, 1)
	rf.chanCanApply = make(chan int, 1)
	//rf.chanCopy = make(chan int, 10000)
	//rf.rpcnum = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index

	go rf.doStateChange()
	//go rf.doCommit()
	go rf.doApply()

	return rf
}
