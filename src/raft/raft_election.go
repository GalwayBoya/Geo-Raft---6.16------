package raft

import (
	"consensus/web/events"
	"fmt"
	"math/rand"
	"time"
)

// 根据信用值获取不同优先级
func (rf *Raft) resetElectionTimerLocked() { //在收到心跳包或者投票的时候，重置选举计时器
	rf.electionStart = time.Now() //确定这一轮次的计时器点
	tmpmin, tmpmax := rf.getTimeout()
	randRange := int64(tmpmax - tmpmin)
	rf.electionTimeout = tmpmin + time.Duration(rand.Int63()%randRange) //随机选取一个min与max之间的时间作为超时时间
}

func (rf *Raft) isElectionTimeoutLocked() bool { //判断是否超时,超时检查
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) isMoreUpToDateLocked(candidateIndex, candidateTerm int) bool { //判断自己的日志是否比candidate新
	l := len(rf.log)
	lastIndex, lastTerm := l-1, rf.log[l-1].Term

	LOG(rf.me, rf.currentTerm, DVote, "Compare last log, Me: [%d]T%d, Candidate: [%d]T%d", lastIndex, lastTerm, candidateIndex, candidateTerm)
	if candidateTerm != lastTerm { //如果term不同，直接比较term
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex //如果term相同，比较index
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct { //请求投票（RequestVote）RPC 调用的参数
	// Your data here (PartA, PartB).
	Term        int //候选人的term
	CandidateId int //候选人的id

	LastLogIndex int
	LastLogTerm  int
	Credit       float64 //候选人的信用值
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last:[%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (PartA).
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("T%d, VoteGranted:%v", reply.Term, reply.VoteGranted)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error { //接收方处理投票请求
	// Your code here (PartA, PartB).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, VoteAsked,Args=%v", args.CandidateId, args.String())

	reply.Term = rf.currentTerm
	reply.VoteGranted = false //默认不投票
	//align term
	if args.Term < rf.currentTerm { //如果请求的term比自己的term小，直接拒绝
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Higher term, T%d>T%d", args.CandidateId, rf.currentTerm, args.Term)
		return nil
	}

	if args.Credit < rf.Credit { //如果请求的信用值比自己的信用值小，直接拒绝
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Lower credit, C%d<C%d", args.CandidateId, args.Credit, rf.Credit)
		return nil
	}

	if args.Term > rf.currentTerm { //如果请求的term比自己的term大，认可，变成follower
		rf.becomeFollowerLocked(args.Term)
	}

	// check for votedFor
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId { //如果已经投过票给其他人了，就不再投票
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Already voted to S%d", args.CandidateId, rf.votedFor)
		return nil
	}

	//check if candidate is more up-to-date
	if rf.isMoreUpToDateLocked(args.LastLogIndex, args.LastLogTerm) { //如果自己的日志比candidate新，就不投票
		LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Reject voted, Me is more up-to-date", args.CandidateId)
		return nil
	}

	// time.Sleep(200 * time.Millisecond)
	reply.VoteGranted = true       //只有条件都满足了，才会投票
	rf.votedFor = args.CandidateId //记下已经投票的人
	rf.persistLocked()             //持久化
	rf.resetElectionTimerLocked()  //重置选举计时器，向candidate保证暂时不会超时
	events.Log("election", "Node %d voted for Node %d in Term %d.", rf.me, args.CandidateId, rf.currentTerm)
	LOG(rf.me, rf.currentTerm, DVote, "<- S%d, Vote granted", args.CandidateId)

	return nil
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
// sendRequestVote 方法通过调用 ClientEnd 结构体的 Call 方法
// 向指定的服务器发送请求投票的 RPC 调用
// 并返回调用是否成功的结果
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) startElection(term int) { //需要term参数，表明这次选举是为了哪个term
	votes := 0 //投票数，用以计票

	//嵌套函数，用以向peer发起投票请求
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) { //向peer发起投票请求（方法内部定义的匿名函数可以访问外部变量，如rf）
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		//处理response
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok { //未获取选票
			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Ask Vote Failed,Lost or error", peer)
			return
		}

		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote Reply=%v", peer, reply.String())

		//同term
		if reply.Term > rf.currentTerm { //如果对方的term比自己大，说明自己的term过时了
			rf.becomeFollowerLocked(reply.Term)
			return
		}

		// check the context
		if rf.contextLostLocked(Candidate, term) { //如果在投票过程中，自己的term已经变了，就不再处理
			LOG(rf.me, rf.currentTerm, DVote, "Lost Context, abort RequestVoteReply for S%d,my term now:%v", peer, rf.currentTerm)
			return
		}

		if reply.VoteGranted { //对齐term且上下文没丢失
			votes++
			rf.votesReceived[peer] = true //统计该peer是否投了自己
			if votes > len(rf.peers)/2 {  //如果获得了大多数的选票
				rf.becomeLeaderLocked() //变为leader
				go rf.replicationTicker(term)
			} //当了Leader后，开始向所有的peer发送心跳包
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()                       //return之前解锁
	if rf.contextLostLocked(Candidate, term) { //如果在投票过程中，自己的term已经变了，就不再处理
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort RequestVot", rf.role)
		return
	}

	l := len(rf.log)
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			votes++
			continue
		}

		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: l - 1,
			LastLogTerm:  rf.log[l-1].Term,
			Credit:       rf.Credit,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, AskVote, Args=%v", peer, args.String())

		go askVoteFromPeer(peer, args)

	}

	// Artificially delay to make the candidate state visible in the UI.
	// This does not affect the correctness of the election.

}

func (rf *Raft) electionTicker() { //选举计时器

	for !rf.killed() {

		rf.mu.Lock()

		if (rf.Credit >= 0.6) && (rf.role != Leader) && (rf.isElectionTimeoutLocked)() { //如果不是leader并且超时了(新增，信誉值要大于0.6)
			// LOG(rf.me, rf.currentTerm, DInfo, "超时时间为：%v", rf.electionTimeout)
			// LOG(rf.me, rf.currentTerm, DInfo, "From Statr Time:%v", time.Since(rf.electionStart))
			rf.becomeCandidateLocked()          //立即变为候选人
			go rf.startElection(rf.currentTerm) //发起选举
		}
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)                  //随机等待50-350ms
		ms := 50 + (rand.Int63() % 300)                  //随机等待50-350ms
		time.Sleep(time.Duration(ms) * time.Millisecond) //即使同时检测到超时，也会有随机的时间差
	}
}
