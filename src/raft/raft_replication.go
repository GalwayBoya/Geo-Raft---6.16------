package raft

import (
	"consensus/web/events"
	"fmt"
	"sort"
	"time"
)

type LogEntry struct {
	Term         int
	CommandValid bool        //说明这个command是不是需要apply
	Command      interface{} //raft是为了维护一个多机上一致的操作日志，command代指操作日志
	//签名相关
	Signature      []byte //消息签名
	SignatureValid bool   //签名是否有效，true表示验证通过
}

type AppendEntriesArgs struct { //附加日志（AppendEntries）RPC 调用的参数,日志或心跳
	Term     int //leader的term
	LeaderId int //leader的id

	//匹配点试探
	PrevLogIndex int        //紧邻新日志条目之前的那个日志条目的index和term
	PrevLogTerm  int        //一个index和一个term能够唯一确定一个LogEntry
	Entries      []LogEntry //需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）

	LeaderCommit int //领导人的已知已提交的最高的日志条目的索引

	Credit []float64 //在下一次心跳/复制日志时将集群的信用值同步给节点
}

func (args *AppendEntriesArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Prev:[%d]T%d, Entries:(%d,%d], CommitIdx:%d",
		args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm,
		args.PrevLogIndex, args.PrevLogIndex+len(args.Entries), args.LeaderCommit)
}

type AppendEntriesReply struct { //附加日志（AppendEntries）RPC 调用的回复
	Term    int  //当前term
	Success bool //如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true

	ConflictIndex int //如果失败，返回冲突的index
	ConflictTerm  int //如果失败，返回冲突的term

	SignatureValid bool //签名是否有效，true表示验证通过
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("T%d, Success:%v, Conflict:[%d]T%d", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error { //接收方处理附加日志请求

	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<- S%d, Appended, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	reply.Success = false //默认失败

	//对齐term
	if args.Term < rf.currentTerm { //  如果领导人的任期小于接收者的当前任期，返回假
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Higher term, T%d<T%d", args.LeaderId, args.Term, rf.currentTerm)
		return nil
	}
	if args.Term >= rf.currentTerm { //如果领导人的任期大于等于接收者的当前任期，接收者的任期更新为领导人的任期
		rf.becomeFollowerLocked(args.Term)
	}

	defer func() { //如果有panic，就返回失败
		rf.resetElectionTimerLocked()
		if !reply.Success {
			LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Follower Conflict: [%d]T%d", args.LeaderId, reply.ConflictIndex, reply.ConflictTerm)
			LOG(rf.me, rf.currentTerm, DDebug, "<- S%v, Follower Log= %v", args.LeaderId, rf.logString())
			// return
		}
	}()

	//如果prevLogIndex和prevLogTerm不匹配，说明日志不匹配，需要回退
	if args.PrevLogIndex >= len(rf.log) { //先判断长度，再判断term，避免数组越界
		reply.ConflictTerm = InvalidTerm  //返回一个无效的term
		reply.ConflictIndex = len(rf.log) //返回自己的日志长度
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Follower log too short,Len:%d < Prev:%d", args.LeaderId, len(rf.log), args.PrevLogIndex)
		return nil
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { //如果term不匹配，说明日志不匹配，需要回退
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term      //记录冲突term
		reply.ConflictIndex = rf.firstLogFor(reply.ConflictTerm) //返回冲突term的第一个log的index
		LOG(rf.me, rf.currentTerm, DLog2, "<- S%d, Reject log, Prev log not match, [%d]: T%d!=T%d", args.LeaderId, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return nil
	}

	//日志已经匹配成功，复制日志到本地

	// ************ 验证所有传入日志条目的签名 ****************
	// First, validate all entries in the batch. If any fails, reject the entire batch.
	for _, entry := range args.Entries {
		if entry.Command != nil { // 如果不是心跳信息，才进行验证
			valid, errMsg := rf.VerifyLogEntry(entry)
			if !valid {
				// 如果签名验证失败，拒绝整个批次
				LOG(rf.me, rf.currentTerm, DError, "签名验证失败，拒绝日志条目，原因: %s", errMsg)
				LOG(rf.me, rf.currentTerm, DError, "检测到领导人S%d可能篡改了日志,举报非法行为", args.LeaderId)
				// 可以在这里扣除leader信用
				reply.Success = false
				return nil
			}
		}
	}

	// All signatures are valid (or entries are heartbeats/empty).
	// Now, append the entries. This correctly handles overwriting conflicting logs from a specific index.
	if len(args.Entries) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
		rf.persistLocked()
	}
	// ************ 验证结束 ****************

	reply.Success = true //如果是心跳信息，或复制的日志全部验证通过后，才返回Success
	LOG(rf.me, rf.currentTerm, DLog2, "Follower accept log:(%d,%d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))
	rf.Credit = args.Credit[rf.me] //在这里接受leader发来的信用更新
	// update commit
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		events.Log("commit", "Leader %d committed logs up to index %d.", rf.me, rf.commitIndex)
		LOG(rf.me, rf.currentTerm, DApply, "Follower CI:%d", rf.commitIndex)
		rf.applyCond.Signal()
	}

	return nil
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool { //向指定的peer发送附加日志请求
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 获取大多数的matchIndex
func (rf *Raft) getMajorityIndexLocked() int {
	//获取大多数的matchIndex
	tmpIndexes := make([]int, len(rf.peers))
	copy(tmpIndexes, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmpIndexes))
	majorityIdx := (len(rf.peers) - 1) / 2
	LOG(rf.me, rf.currentTerm, DDebug, "Match Index after sort:%v, majority[%d]=%d", tmpIndexes, majorityIdx, tmpIndexes[majorityIdx])
	return tmpIndexes[majorityIdx]
}

// 只有在给定的term下开启复制计时器
func (rf *Raft) startReplication(term int) bool { //遍历所有的peer，向所有的peer发送心跳包（发送方）

	//嵌套函数，用以向peer发起复制请求
	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		//ok反映RPC调用本身是否成功

		// 创建一个通道用于接收RPC响应
		done := make(chan bool)

		//记录发送信息时间
		startTime := time.Now()

		go func() {
			ok := rf.sendAppendEntries(peer, args, reply)
			done <- ok
		}()

		// 等待响应或超时（原来的机制是一直等待，直到收到响应）
		// 现在使用一个通道来接收响应，如果超时，则增加未响应计数
		// 实现功能：若follower连续三次不应答/应答，则对其施加惩罚，减少信誉值
		select {
		case ok := <-done:
			//计算响应时间（毫秒）
			responseTime := time.Since(startTime).Milliseconds()

			rf.mu.Lock()
			// 将本次响应的延迟时间添加到对应节点的延迟数组中
			// 这些延迟数据将在每分钟由Leader调用updateCredit时被处理
			// 用于计算节点的平均延迟，作为信用值更新的依据
			rf.Delay[peer] = append(rf.Delay[peer], responseTime)
			rf.mu.Unlock()

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if !ok { //远程节点宕机或不可达、网络分区导致通信失败、远程节点拒绝处理请求、RPC超时
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed or refuse to respond", peer)
				// 超时，增加未响应计数
				rf.noResponse[peer]++

				if rf.noResponse[peer] >= 3 {
					// 连续三次未响应，惩罚
					rf.penalty[peer] += 0.15
					// rf.noResponse[peer] = 0 // 重置计数
					LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Credit reduced by 0.15 due to no response", peer)
				} else {
					rf.penalty[peer] += 0.05
				}
				return
			}

			// 收到响应，重置未响应计数
			rf.noResponse[peer] = 0

			LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Reply=%v", peer, reply.String())

			// align the term
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			// check the context
			if rf.contextLostLocked(Leader, term) { //如果在复制过程中，自己的term/role已经变了，就不再处理
				LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Contest Lost, T%d:Leader to T%d:%d", term, rf.currentTerm, rf.role)
				return
			}

			// leaded在收到每个peer的回复时，处理请求
			if !reply.Success { //如果失败，说明prevLogIndex和prevLogTerm不匹配，需要回退，探测更低的index
				//每次都回退一个term
				// idx, term := args.PrevLogIndex, args.PrevLogTerm
				// for idx > 0 && rf.log[idx].Term == term {
				//  idx--
				// }
				// rf.nextIndex[peer] = idx + 1
				// LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not match at %d,try next=%d", peer, args.PrevLogIndex, rf.nextIndex[peer])
				// return
				prevIndex := rf.nextIndex[peer]
				if reply.ConflictTerm == InvalidTerm { //如果返回的是无效Term
					rf.nextIndex[peer] = reply.ConflictIndex //直接回退到冲突的index
				} else {
					firstIndex := rf.firstLogFor(reply.ConflictTerm) //找到冲突term的第一个index
					if firstIndex != InvalidIndex {                  //如果找到了
						rf.nextIndex[peer] = firstIndex //回退到冲突term的第一个index
					} else {
						rf.nextIndex[peer] = reply.ConflictIndex //如果找不到，则直接回退到冲突的index
					}
				}
				// avoid unordered index
				if rf.nextIndex[peer] > prevIndex { //如果回退后的index比之前的index还大，说明有问题
					rf.nextIndex[peer] = prevIndex //直接回退到prevInde
				}

				// Add a boundary check to prevent panic on log statement
				//增加一个边界检查：当 nextIndex 被更新为 0 时，我们不应该再尝试访问 rf.log 中索引为 -1 的元素。
				if rf.nextIndex[peer] < 1 {
					LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Next index is %d, which is invalid. Leader log might be too short.", peer, rf.nextIndex[peer])
				} else {
					LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Not match at Prev=[%d]T%d, Try next Prev=[%d]T%d",
						peer, args.PrevLogIndex, args.PrevLogTerm, rf.nextIndex[peer]-1, rf.log[rf.nextIndex[peer]-1].Term)
				}
				LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Leader Log= %v", peer, rf.logString())
				return
			}

			//成功复制，更新nextIndex和matchIndex
			rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries) //使用请求的prevLogIndex+Entries的长度来更新matchIndex
			rf.nextIndex[peer] = rf.matchIndex[peer] + 1

			tmpPrtCount := make([]int64, len(rf.peers)) //临时的共识完成记录数组，因为这一次共识不一定能完成
			tmpPrtCount[peer]++                         //该peer完成了一次共识

			//更新commitIndex
			majorityMatched := rf.getMajorityIndexLocked()
			if majorityMatched > rf.commitIndex && rf.log[majorityMatched].Term == rf.currentTerm { //如果大多数的peer都复制了这个term的日志，就更新commitIndex
				events.Log("commit", "Leader %d committed logs up to index %d.", rf.me, majorityMatched)
				LOG(rf.me, rf.currentTerm, DApply, "Leader Update commitIndex from %d to %d", rf.commitIndex, majorityMatched)
				rf.totalConsensus++               //完成了一次共识
				rf.participateCount[rf.me]++      //leader自己
				rf.participateCount = tmpPrtCount //更新完成共识的peer

				rf.commitIndex = majorityMatched //在这里leader进行commit
				rf.applyCond.Signal()            //通知applyCond，可以进行apply了
			}

		case <-time.After(50 * time.Millisecond): // 如果50ms内未应答，计数+1
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 超时，增加未响应计数
			rf.noResponse[peer]++
			// if rf.noResponse[peer] >= 3 {
			// 	// 连续三次未响应，降低信用值
			// 	rf.creditview[peer] -= 0.15
			// 	// rf.noResponse[peer] = 0 // 重置计数
			// 	LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Credit reduced by 0.15 due to no response", peer)
			// }
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Timeout waiting for response", peer)
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock() //以确保在进行逻辑构造参数发送RPC时，全局的数据结构不会发生变化

	if rf.contextLostLocked(Leader, term) { //如果在复制过程中，自己的term已经变了，就不再处理。对于Leader来说，如果他不在这个term中，则他一定不是Leader了
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[%d] to %s[T%d]", term, rf.role, rf.currentTerm)
		return false
	}

	for peer := 0; peer < len(rf.peers); peer++ { //向所有的peer发送心跳包,如果是自己，就不用发
		if peer == rf.me {
			rf.matchIndex[peer] = len(rf.log) - 1 //从0开始
			rf.nextIndex[peer] = len(rf.log)

			continue
		}

		// Add a robust boundary check before accessing the log
		prevIdx := rf.nextIndex[peer] - 1
		if prevIdx < 0 {
			// This case should ideally not happen if logs are always initialized with a dummy entry.
			// However, as a safeguard, we prevent a panic.
			LOG(rf.me, rf.currentTerm, DError, "Invalid prevIdx %d for peer %d, nextIndex was %d. Resetting to 0.", prevIdx, peer, rf.nextIndex[peer])
			prevIdx = 0
		}

		prevTerm := rf.log[prevIdx].Term
		args := &AppendEntriesArgs{ //在loop中，每次对每一个peer都构造一个AppendEntriesArgs
			Term:         rf.currentTerm, //此处currentTerm也就是leader的term
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[prevIdx+1:], //从nextIndex开始复制
			LeaderCommit: rf.commitIndex,
			Credit:       rf.creditview,
		}
		LOG(rf.me, rf.currentTerm, DDebug, "-> S%d, Append, Args=%v", peer, args.String())
		go replicateToPeer(peer, args)

	}

	return true //返回一个成功的标志
}

// 只能在给定的term下开启复制计时器
func (rf *Raft) replicationTicker(term int) { //复制计时器
	for !rf.killed() { //每次时间一到，就向所有的peer发送心跳包
		ok := rf.startReplication(term)
		if !ok {
			break
		}

		time.Sleep(replicateInterval) //每隔一段时间发送一次心跳包

	}
}

// 由于较旧的Go版本没有内置的min函数，我们自己实现一个
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
