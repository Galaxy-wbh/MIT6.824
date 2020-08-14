package raft

import "time"

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int 
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
}

func (rf *Raft) resetHeartBeatTimers(){
	for i, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetHeartBeatTimer(index int) {
	rf.appendEntriesTimers[index].Stop()
	rf.appendEntriesTimers[index].Reset(HeartBeatTimeout)
}

func (rf *Raft) getAppendLogs(index int) (prevLogIndex, prevLogTerm int, res []LogEntry) {
	//nextIndex := rf.nextIndex[index]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	prevLogIndex = lastLogIndex
	prevLogTerm = lastLogTerm
	res = append([]LogEntry{})
	return 
}

func (rf *Raft) getAppendEntriesArgs(index int) AppendEntriesArgs{
	prevLogIndex, prevLogTerm, logs := rf.getAppendLogs(index)
	args := AppendEntriesArgs{
		Term: rf.term,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.lock("append_entries")
	DPrintf("[%d] get append entries from [%d]", rf.me, args.LeaderId)
	reply.Term = rf.term 
	if rf.term > args.Term {
		rf.unlock("append_entries")
		return 
	}
	rf.term = args.Term 
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	reply.Success = true
	rf.unlock("append_entries")
	
}

func (rf *Raft) appendEntriesToPeer(index int){
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed(){
		rf.lock("appendtopeer1")
		if rf.role != Leader {
			rf.resetHeartBeatTimer(index)
			DPrintf("[%d] not leader, resetHeartBeatTimer", rf.me)
			rf.unlock("appendtopeer1")
			return 
		}
		args := rf.getAppendEntriesArgs(index)
		rf.resetHeartBeatTimer(index)
		rf.unlock("appendtopeer1")

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)
		
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply){
			DPrintf("[%d] send append entries to peer [%d]",rf.me, index)
			ok := rf.peers[index].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(&args, &reply)
		
		select {
		case <- rf.stopCh:
			return
		case <- RPCTimer.C:
			DPrintf("[%d] appendtopper, rpctimeout: peer:%d, args:%+v", rf.me, index, args)
			continue
		case ok :=  <- resCh:
			if !ok {
				DPrintf("[%d] append to peer not ok.", rf.me)
				continue
			}
		}
		DPrintf("[%d] appendtopeer peer:[%d], args:%+v, reply:%+v", rf.me, index, args, reply)
		rf.lock("appendtopeer2")
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term 
			rf.unlock("appendtopeer2")
			return 
		}

		if rf.role != Leader || rf.term != args.Term {
			rf.unlock("appendtopeer2")
			return 
		}
		rf.unlock("appendtopeer2")
		
		
	}
}