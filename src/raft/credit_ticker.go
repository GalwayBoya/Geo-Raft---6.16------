package raft

import (
	"time"
)

// 信用值更新相关
// creditUpdateInterval 定义信用值更新的时间间隔，每分钟更新一次
const creditUpdateInterval = 1 * time.Minute

// startCreditUpdateTicker 启动一个定时器，每隔一分钟由leader调用updateCredit函数
// 对集群中节点的信用值进行更新。此函数会持续运行，直到节点被关闭。
//
// 更新过程中会：
// 1. 计算每个节点的平均延迟
// 2. 根据平均延迟、参与度和投票一致性更新信用值
// 3. 更新完成后重置统计数据，准备下一轮统计
func (rf *Raft) startCreditUpdateTicker() {
	for !rf.killed() {
		// 等待一分钟
		time.Sleep(creditUpdateInterval)

		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()

		// 只有leader才能更新信用值
		if isLeader {
			LOG(rf.me, rf.currentTerm, DLog, "Leader updating credit values for all nodes")

			rf.updateCredit()

			// 更新完信用值后，重置统计数据，准备下一轮统计
			rf.mu.Lock()
			rf.totalConsensus = 0
			for i := 0; i < len(rf.peers); i++ {
				rf.participateCount[i] = 0
				rf.validVoteCount[i] = 0
				rf.Delay[i] = make([]int64, 0)
			}

			rf.mu.Unlock()

			LOG(rf.me, rf.currentTerm, DLog, "Credit values updated successfully")
		}
	}
}

// Leader计算更新节点的信用值
// updateCredit 由Leader调用，用于更新集群中所有节点的信用值
// 信用值计算基于以下因素：
// 1. 硬件参数和地理优先级（静态部分）
// 2. 节点历史行为表现（动态部分）：
//   - 参与共识的比例
//   - 投票一致性
//   - 通信延迟（使用一分钟内收集的所有延迟数据的平均值）
//
// 每个节点的信用值更新后，会清空该节点的延迟记录数组，准备下一轮统计
func (rf *Raft) updateCredit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 计算各节点延迟的平均值，并确定全局最小和最大延迟（用于归一化）
	minDelay := int64(0)
	maxDelay := int64(0)
	avgDelays := make([]int64, len(rf.peers))

	// 计算每个节点的平均延迟
	for i, delays := range rf.Delay {
		if len(delays) == 0 { //如果节点i的延迟数组长度为0，那它平均延迟就为0
			avgDelays[i] = 0
			continue
		}

		// 计算平均延迟
		var sum int64 = 0
		var count int64 = 0
		for _, delay := range delays {
			if delay > 0 { //针对不为0的每一次进行计算
				sum += delay
				count++
			}
		}

		if count > 0 {
			avgDelays[i] = sum / count
		} else {
			avgDelays[i] = 0
		}

		// 初始化最小和最大延迟
		if i == 0 || (avgDelays[i] > 0 && avgDelays[i] < minDelay) {
			minDelay = avgDelays[i]
		}
		if avgDelays[i] > maxDelay {
			maxDelay = avgDelays[i]
		}
	}

	// 确保最大最小延迟不同，避免除以零
	if maxDelay == minDelay {
		maxDelay = minDelay + 1
	}

	avgDelays[rf.me] = (maxDelay + minDelay) / 2 //更新leader自己的延迟，取平均

	for i := 0; i < len(rf.peers); i++ {
		// 静态部分：HwParams 和 GeoPri 各占 50%
		static := 0.5*rf.HwParams + 0.5*rf.GeoPri

		// 动态部分 Bi(t)
		// 计算参与度：每一分钟内成功完成共识的次数占总共识次数的比例
		hi := float64(rf.participateCount[i]) / float64(rf.totalConsensus)
		if rf.totalConsensus == 0 {
			hi = 0
		}

		// 计算投票一致性：投票与最终结果一致的次数占总共识次数的比例
		vi := float64(rf.validVoteCount[i]) / float64(rf.totalConsensus)
		if rf.totalConsensus == 0 {
			vi = 0
		}

		// 使用平均延迟计算归一化延迟指标（值越大表示延迟越低）
		normalizedDelay := 0.0
		if avgDelays[i] > 0 {
			normalizedDelay = 1.0 - float64(avgDelays[i]-minDelay)/float64(maxDelay-minDelay)
		}

		// 计算行为分数，各指标加权计算
		bi := 0.35*rf.behaviorScore[i] + 0.2*hi + 0.1*vi + 0.35*normalizedDelay // 权重示例：α=0.3, β=0.2, γ=0.2, δ=0.3
		rf.behaviorScore[i] = bi

		// 总信用值 Ci(t) = w*static + (1-w)*σ*(Bi - Pi)
		w := 0.2     // 静态权重
		sigma := 0.8 // 衰减系数
		ci := w*static + (1-w)*sigma*(bi-rf.penalty[i])

		rf.creditview[i] = ci
	}

	// 更新自己的信用值后，重新计算选举超时时间
	if rf.role != Leader {
		oldTimeout := rf.electionTimeout
		rf.resetElectionTimerLocked()

		LOG(rf.me, rf.currentTerm, DLog, "Credit updated to %.3f, timeout changed from %v to %v",
			rf.creditview[rf.me], oldTimeout, rf.electionTimeout)
	}

}

// 定期对Pi进行衰减处理
func (rf *Raft) decayPenalty() {
	for !rf.killed() {
		rf.mu.Lock()
		for i := 0; i < len(rf.peers); i++ {
			rf.penalty[i] *= 0.8 // 每周期衰减 20%
		}
		rf.mu.Unlock()
		time.Sleep(20 * time.Second) // 每 20 秒衰减一次
	}
}

func (rf *Raft) getTimeout() (min time.Duration, max time.Duration) {
	if rf.Credit >= 0.8 {
		return FirstTier.MinTimeout, FirstTier.MaxTimeout
	} else if rf.Credit >= 0.6 && rf.Credit < 0.8 {
		return SecondTier.MinTimeout, SecondTier.MaxTimeout
	} else if rf.Credit >= 0.3 && rf.Credit < 0.6 {
		return ThirdTier.MinTimeout, ThirdTier.MaxTimeout
	}
	return 10000 * time.Millisecond, 10000 * time.Millisecond

}
