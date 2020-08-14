package raft
import "time"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.lock("req_vote")
	defer rf.unlock("req_vote")
	defer func() {
		DPrintf("[%d] get request vote, args:%+v, reply:%+v", rf.me, args, reply)
	}()

	//lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.term 
	reply.VoteGranted = false 

	if args.Term < rf.term {
		return 
	}else if args.Term > rf.term {
		reply.VoteGranted = true 
		rf.changeRole(Follower)
	}else {
		//if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		reply.VoteGranted = true 
		rf.changeRole(Follower)
		//}
	}

	
	
	// else if args.Term == rf.term {
	// 	if rf.role == Leader {
	// 		return 
	// 	}
		
	// 	if rf.voteFor == args.CandidateId {
	// 		reply.VoteGranted = true
	// 		return 
	// 	}
	// 	//投给了其他server
	// 	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
	// 		return
	// 	}

	// }else {
	// 	rf.term = args.Term
	// 	rf.voteFor = -1
	// 	rf.changeRole(Follower)
	// }

	// if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
	// 	return 
	// }

	// rf.term = args.Term
	// rf.voteFor = args.CandidateId
	// reply.VoteGranted = true
	// rf.changeRole(Follower)


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
func (rf *Raft) sendRequestVoteToPeer(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()
	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}
		go func(){
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if ok == false {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <- t.C:
			return
		case <- rpcTimer.C:
			continue
		case ok := <- ch:
			if !ok {
				continue
			}else{
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return 
			}
			
		}
	}
}

func (rf *Raft) startElection(){
	rf.lock("start_election")
	rf.electionTimer.Reset(randElectionTimeout())
	if rf.role == Leader {
		rf.unlock("start_election")
		return 
	}

	
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term: rf.term,
		CandidateId: rf.me, 
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	DPrintf("[%d] start election. args:%v", rf.me, args)
	rf.unlock("start_election")
	
	grantedCount := 1
	chResCount := 1
	
	votesCh := make(chan bool, len(rf.peers))
	
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int){
			reply := RequestVoteReply{}
			DPrintf("[%d] sends vote request to [%d]",rf.me, index)
			rf.sendRequestVoteToPeer(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.lock("start_ele_change_term")
				if reply.Term > rf.term {
					rf.term = reply.Term 
					rf.changeRole(Follower)
					rf.resetElectionTimer()
				}
				rf.unlock("start_ele_change_term")	
			}	
		}(votesCh, index)
	}

	for {
		r := <- votesCh
		chResCount += 1
		if r == true {
			grantedCount += 1
		}
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || (chResCount - grantedCount) > len(rf.peers)/2 {
			break 
		}
	}
	if grantedCount <= len(rf.peers)/2 {
		DPrintf("[%d] grantedCount <= len/2:count:%d", rf.me, grantedCount)
		return 
	}

	rf.lock("start_ele2")
	DPrintf("[%d] Already collected enough votes. grantedCount:%d, args:%+v", rf.me, grantedCount, args)
	if rf.term == args.Term && rf.role == Candidate {
		rf.changeRole(Leader)
	}
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	rf.unlock("start_ele2")

}