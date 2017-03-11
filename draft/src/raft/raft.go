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

import "sync"
import "labrpc"
import "time"
import "math/rand"
// import "bytes"
// import "encoding/gob"


//const HeartInterval = time.Millisecond * 150
const HeartInterval = time.Millisecond * 150


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	dead bool					  // if raft
	lastTimeBeenReset int64 //be changed by heartbeats
	isLeader chan bool // is necessary??
	electeCancel chan bool //set true if other server to be leader

	voted bool // flag of if candidate voted for or not 
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int 
	voteFor int //candidateId or interface{} type
	voteCount int 
	leader int // leader id now

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	var isleader bool
	// Your code here (2A).
	if rf.leader == rf.me {
		isleader = true
	}
	return rf.currentTerm, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate`s term
	CandidateId int // candidate requesting vote

	// other field
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	
	// reset rf.lastTimeBeenReset, count timeout from now
	rf.lastTimeBeenReset = now()
	if !rf.voted {
		DPrintf("server %d now vote for %d\n", rf.me, args.CandidateId)
		rf.voted = true
		rf.voteFor = args.CandidateId
		//change curent term, todo for strong
		if rf.currentTerm < args.Term {
			// need mutex??
			rf.currentTerm = args.Term
			reply.VoteGranted = true
		}
			
	}
}

type AppendEntriesArgs struct {
	Term int // leader`s term 
	LeaderId int 

	//other field

} 

type AppendEntriesReply struct {
	ServerId int
	Term int //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

} 

//
// AppendEntries RPC handler.
// 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.lastTimeBeenReset = now()
	
	// check if leader itself or not
	if rf.currentTerm <= args.Term {
		rf.leader = args.LeaderId
		reply.Success = true
		select {
			case rf.electeCancel <- true:
				DPrintf("new leader selected")
			default:
		}
	}
	// set heartbeart time to zero
	reply.Term = rf.currentTerm
	reply.ServerId = rf.me 
	//do other things
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	// Your code here (2B).


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
	rf.dead = true
	// close clinet ends below
}

// Candidate status
func (rf *Raft) toCandidate() {
	
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// add current term
	rf.currentTerm++

	//vote itself
	rf.voteFor = rf.me
	rf.voted = true
	rf.voteCount++
	defer func(){
		rf.voteCount = 0
	}()

	// send RequestVote RPC to all servers, once in this certain term
	rva := &RequestVoteArgs{}
	rva.Term = rf.currentTerm
	rva.CandidateId = rf.me


	servnum := len(rf.peers)
	rvareplys := make([]*RequestVoteReply, servnum) // save RPC reply
	for i := 0; i < servnum; i++{
		rvareplys[i] = &RequestVoteReply{}
	}
	replys := make(chan bool, servnum-1) //for wait replays from Call()
	DPrintf("now server %d send RequestVotes....\n", rf.me)
	for i := 0; i < servnum; i++{
		if i != rf.me{
			//replys <- rf.sendRequestVote(i, rva, rvareplys[i])
			DPrintf("now server %d send RequestVote to server %d\n", rf.me, i)
			go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, replychans chan bool) {
				replys <- rf.sendRequestVote(server, args, reply)
			}(i, rva, rvareplys[i], replys)
		}
	}

	// wait for all response and check if other server turn to leader.
	replymun := 0
	LOOP:
	for {
		select{
			case ok := <- replys:
				//mutex?
				DPrintf("server %d recived vote request reply is %t \n", rf.me,  ok)
				replymun++
				if replymun >= servnum-1{
					// check if majar server voted
					for i := 0; i < servnum; i++{
						if i != rf.me{
							// check rvareplys[i].term, todo
							
							// check if win the vote from other nodes
							if rvareplys[i].VoteGranted {
								rf.voteCount++
							}
						}
					}
					DPrintf("server %d `s voteCount is %d\n", rf.me, rf.voteCount)
					if rf.voteCount >= servnum/2 {
						rf.isLeader <- true
						return
					}
					break LOOP
				} 
			case cancel := <- rf.electeCancel:
				//there is leader already in cluster
				DPrintf("cancel flag is %t \n", cancel)
        		break LOOP
			case <-time.After(5000 * time.Millisecond):
        		DPrintf("election timed out, no leader elected. return from candidate status\n")
        		break LOOP
		}
	}

	rf.isLeader <- false
}

//to leader status
func (rf *Raft) toLeader() {
	for rf.leader == rf.me {
		// send AppendEntries to followers periodicly
		// send RequestVote RPC to all servers, once in this certain term
		aea := &AppendEntriesArgs{}
		aea.Term = rf.currentTerm
		aea.LeaderId = rf.me


		servnum := len(rf.peers)
		aereplys := make([]*AppendEntriesReply, servnum) // save RPC reply
		for i := 0; i < servnum; i++{
			aereplys[i] = &AppendEntriesReply{}
		}
		replys := make(chan bool, servnum-1) //for wait replays from Call()

		for i := 0; i < servnum; i++{
			if i != rf.me{
				DPrintf("now leader %d send AppendEntries to server %d\n", rf.me, i)
				go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, replychans chan bool) {
					replys <- rf.sendAppendEntries(server, args, reply)
				}(i, aea, aereplys[i], replys)
			}
		}

		replymun := 0
		LOOP:
		for {
			select{
				case ok := <- replys:
					//mutex?
					DPrintf("leader %d receive reply  is %t \n",rf.me, ok)
					replymun++
					if replymun >= servnum-1{
						break LOOP
					} 
				case <-time.After(60 * time.Second): //HeartInterval and continue? 
	        		DPrintf("leader get reply timeout\n")
			}
		}

		// process reply
		for i := 0; i < servnum; i++{
			if i != rf.me{
				// check aereplys[i].term, todo
				if aereplys[i].Success {
					//DPrintf("leader %d heartbeat SUCCESS from server %d \n", rf.me, i)
				}
			}
		}		
		//do other things

		// return when find other leader in cluster, and the leader`s term is bigger.
		time.Sleep(HeartInterval)
	}
}

//A candidate continues in this state until one of 
//three things happens:
//(a) it wins the election,
//(b) another server establishes itself as leader,  
//(c) a period of time goes by with no winner
func (rf *Raft) electionTime() {

	//check heartbeats, if  kick off leader election periodically
	// by sending out RequestVote RPCs when it hasn't heard 
	// from another peer for a while.
	electetimeouts := randInt(300, 500)
	for {
		if now() - rf.lastTimeBeenReset > electetimeouts {
			//for {
				go rf.toCandidate()
				//block for chan or election timeout 
				select {
					case isleader := <- rf.isLeader:
						if isleader {
							DPrintf("server %d to leader status....\n", rf.me)
							// need go thread or block?
							rf.leader = rf.me
							rf.toLeader()
							//break
							//time.Sleep(30000)
							
						} 
					case <-time.After(5000 * time.Millisecond):
        				DPrintf("server %d: election timed out, no leader elected.change state to follower, reset timeouts \n", rf.me)
				}	

			//}
			// back to follower status, reset timeout.
			rf.voted = false
			rf.lastTimeBeenReset = now()
			electetimeouts = randInt(300, 500)
		}

	}

	//if timeout, switch to candidate, and send sendRequestVote RPC, 
	//util recieve AppendEntries.
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
	rf.dead = false
	rf.currentTerm = 0
	rf.voteFor = -1 

    rf.lastTimeBeenReset = now()
	rf.isLeader  = make(chan bool)
	rf.electeCancel = make(chan bool)

	rf.voted = false
	rf.voteCount = 0 
	rf.leader = -1

	go func(){
		for rf.dead == false {
			// get random timeout
      		rf.electionTime()
      	}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

//return now time with ms since January 1, 1970 UTC.
func now() int64 {
	return time.Now().UnixNano()/1e6
}

func randInt(start int64, end int64) int64 {
    if start >= end || start == 0 || end == 0{
        return int64(end)
    }
    rand.Seed(int64(time.Now().Nanosecond()))
    return rand.Int63n(end-start) + start

}
