package raft

import "time"

// leader主动卸任相关
// TimeoutNow RPC参数
type TimeoutNowArgs struct {
	Term        int // 发送者的任期
	Leaderindex int //leader的index
}

// TimeoutNow RPC回复
type TimeoutNowReply struct {
	Term           int // 接收者的任期
	NewLeaderindex int
	Succuess       bool
}

// TimeoutNow RPC处理函数
func (rf *Raft) TimeoutNow(args *TimeoutNowArgs, reply *TimeoutNowReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Succuess = false //默认不同意

	// 如果发送者任期小于自己的任期，忽略
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "Reject TimeoutNow, Higher term, T%d>T%d", rf.currentTerm, args.Term)
		return nil
	}

	// 如果发送者任期大于自己的任期，先更新自己的任期
	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	// 直接变为Candidate并开始选举
	rf.becomeCandidateLocked()
	go rf.startElection(rf.currentTerm) //有点问题

	//填充reply
	reply.Term = rf.currentTerm //变为candidate之后的Term
	reply.NewLeaderindex = rf.me
	reply.Succuess = true

	LOG(rf.me, rf.currentTerm, DVote, "Received TimeoutNow, becoming candidate")
	return nil
}

// 发送TimeoutNow RPC
func (rf *Raft) sendTimeoutNow(server int, args *TimeoutNowArgs, reply *TimeoutNowReply) bool {
	ok := rf.peers[server].Call("Raft.TimeoutNow", args, reply)
	return ok
}

// 查找最高信誉值节点的函数
func (rf *Raft) findHighestCreditNode() int {
	highestCredit := -1.0
	highestCreditNode := -1

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.creditview[i] > highestCredit {
			highestCredit = rf.creditview[i]
			highestCreditNode = i
		}
	}

	// 如果没有找到比当前节点信誉值更高的节点，则选择随机节点
	// if highestCreditNode == -1 {
	// 	candidates := make([]int, 0)
	// 	for i := 0; i < len(rf.peers); i++ {
	// 		if i != rf.me {
	// 			candidates = append(candidates, i)
	// 		}
	// 	}
	// 	if len(candidates) > 0 {
	// 		highestCreditNode = candidates[rand.Intn(len(candidates))]
	// 	}
	// }

	return highestCreditNode
}

// leader主动卸任计时器
func (rf *Raft) leaderTimeoutTicker(term int) {
	for !rf.killed() {
		time.Sleep(10 * time.Second) // 每10秒检查一次

		rf.mu.Lock()

		// 检查上下文
		if rf.contextLostLocked(Leader, term) {
			rf.mu.Unlock()
			return
		}

		// 检查是否已经到达leader超时时间
		if time.Since(rf.leaderStartTime) >= rf.leaderTimeout {
			LOG(rf.me, rf.currentTerm, DLeader, "Leader timeout after 24h, stepping down")

			// 查找最高信誉值的节点
			nextLeader := rf.findHighestCreditNode()

			if nextLeader != -1 { //找到了信誉值最高节点
				// 发送TimeoutNow RPC给选定的节点
				args := &TimeoutNowArgs{
					Term:        rf.currentTerm,
					Leaderindex: rf.me,
				}

				// 解锁以避免死锁
				rf.mu.Unlock()

				// 异步发送RPC
				go func(server int, args *TimeoutNowArgs) {
					LOG(rf.me, term, DLeader, "Sending TimeoutNow to S%d", server)
					reply := &TimeoutNowReply{}
					ok := rf.sendTimeoutNow(server, args, reply)

					if ok && reply.Succuess { //没有error并且reply.success返回true
						rf.mu.Lock()
						defer rf.mu.Unlock()

						// 如果收到更高任期的回复，变为follower
						if reply.Term > rf.currentTerm {
							rf.becomeFollowerLocked(reply.Term)
						}
						// } else {
						// 	// 不管回复如何，主动变为follower
						// 	rf.becomeFollowerLocked(rf.currentTerm)
						// }

						LOG(rf.me, rf.currentTerm, DLeader, "Stepped down after TimeoutNow response")
					} else { //有error
						rf.mu.Lock()
						defer rf.mu.Unlock()

						// 如果RPC失败，仍然主动变为follower，自然选举
						rf.becomeFollowerLocked(rf.currentTerm)
						LOG(rf.me, rf.currentTerm, DLeader, "Stepped down after TimeoutNow RPC failed")
					}
				}(nextLeader, args)

				return
			}
			// } else { //没找到信誉值最高节点，按理说肯定能找到
			// 	// 直接变为follower
			// 	rf.becomeFollowerLocked(rf.currentTerm)
			// 	rf.mu.Unlock()
			// 	LOG(rf.me, rf.currentTerm, DLeader, "Stepped down, no candidate found")
			// 	return
			// }
		}

		rf.mu.Unlock()
	}
}
