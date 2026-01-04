package raft

func (rf *Raft) applicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait() //wait的时候会释放锁，等到有signal的时候再重新获取锁，执行后面的代码

		entries := make([]LogEntry, 0)                          //需要应用的日志
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ { //从lastApplied+1开始，到commitIndex结束
			entries = append(entries, rf.log[i]) //将需要应用的日志加入到entries中
		}

		rf.mu.Unlock()

		for i, entry := range entries { //遍历entries
			rf.applyCh <- ApplyMsg{ //将entry发送到applyCh中
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + 1 + i,
			}
		}

		rf.mu.Lock()
		LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d,%d]", rf.lastApplied+1, rf.lastApplied+len(entries))
		rf.lastApplied += len(entries) //更新lastApplied
		rf.mu.Unlock()

	}

}
