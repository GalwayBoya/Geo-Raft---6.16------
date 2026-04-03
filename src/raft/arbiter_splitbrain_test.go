package raft

// 该测试模拟了一个跨数据中心分区的场景，其中两个数据中心各有一个节点被分区成 Leader，仲裁节点通过日志新鲜度和 RTT 来选出最终的 Leader。
import (
	"consensus/labrpc"
	"fmt"
	"testing"
	"time"
)

func buildArbiterTestCluster(numServers int) (*labrpc.Network, [][]*labrpc.ClientEnd, []*Raft, []*Persister, []chan ApplyMsg) {
	net := labrpc.MakeNetwork()
	ends := make([][]*labrpc.ClientEnd, numServers)
	persisters := make([]*Persister, numServers)
	applyChs := make([]chan ApplyMsg, numServers)
	rafts := make([]*Raft, numServers)

	for i := 0; i < numServers; i++ {
		ends[i] = make([]*labrpc.ClientEnd, numServers)
		for j := 0; j < numServers; j++ {
			endName := fmt.Sprintf("end-%d-%d", i, j)
			ends[i][j] = net.MakeEnd(endName)
			net.Connect(endName, j)
			net.Enable(endName, true)
		}
	}

	for i := 0; i < numServers; i++ {
		persisters[i] = MakePersister()
		applyChs[i] = make(chan ApplyMsg, 100)
		rf := Make(ends[i], i, persisters[i], applyChs[i])
		rafts[i] = rf
		srv := labrpc.MakeServer()
		srv.AddServiceWithName(rf, "Raft")
		net.AddServer(i, srv)
	}
	return net, ends, rafts, persisters, applyChs
}

func buildArbiterEnds(net *labrpc.Network, numServers int) []*labrpc.ClientEnd {
	arbiterEnds := make([]*labrpc.ClientEnd, numServers)
	for i := 0; i < numServers; i++ {
		endName := fmt.Sprintf("arbiter-%d", i)
		arbiterEnds[i] = net.MakeEnd(endName)
		net.Connect(endName, i)
		net.Enable(endName, true)
	}
	return arbiterEnds
}

func partitionTwoDCs(net *labrpc.Network, nodesPerDC int, dcCount int) {
	total := nodesPerDC * dcCount
	for i := 0; i < total; i++ {
		dcI := i / nodesPerDC
		for j := 0; j < total; j++ {
			dcJ := j / nodesPerDC
			if dcI != dcJ {
				net.Enable(fmt.Sprintf("end-%d-%d", i, j), false)
			}
		}
	}
}

func forceLeaderForTest(rf *Raft, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func appendLogForTest(rf *Raft, term int, n int, prefix string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < n; i++ {
		rf.log = append(rf.log, LogEntry{
			Term:         term,
			CommandValid: true,
			Command:      fmt.Sprintf("%s-%d", prefix, i),
		})
	}
	rf.commitIndex = len(rf.log) - 1
	rf.lastApplied = rf.commitIndex
	rf.persistLocked()
}

// 该测试不是为了证明当前 GRaft 会自然产生双主，
// 而是为了在“特定故障注入场景”下验证仲裁节点的裁决逻辑是否正确：
// 1. 先人为制造双主；
// 2. 仲裁节点旁路探测；
// 3. 仲裁节点根据日志新鲜度 + RTT 选出赢家；
// 4. 败者收到裁决后退位。
func TestArbiterResolveSplitBrain(t *testing.T) {
	const numServers = 6
	const nodesPerDC = 3

	net, _, rafts, _, _ := buildArbiterTestCluster(numServers)
	defer func() {
		for i := 0; i < numServers; i++ {
			if rafts[i] != nil {
				rafts[i].Kill()
			}
		}
	}()

	arbiterEnds := buildArbiterEnds(net, numServers)
	arbiter := NewArbiter(arbiterEnds)

	// 构造一个跨 DC 分区，但仲裁节点仍然通过单独链路可见所有节点。
	partitionTwoDCs(net, nodesPerDC, 2)

	// 人工制造双主：DC0 的节点0、DC1 的节点3 同时视为 Leader。
	forceLeaderForTest(rafts[0], 10)
	forceLeaderForTest(rafts[3], 10)

	// 让两个主节点拥有不同的新鲜度：节点0日志更长、更完整。
	appendLogForTest(rafts[0], 10, 5, "dc0")
	appendLogForTest(rafts[3], 10, 3, "dc1")

	// 构造一个更贴近“工程场景”的仲裁观测：
	// 节点3到仲裁节点的 RTT 更低，但节点0日志更新更全。
	arbiter.SetSyntheticLatency(0, 15*time.Millisecond)
	arbiter.SetSyntheticLatency(3, 5*time.Millisecond)

	winner, leaders, err := arbiter.ResolveSplitBrain()
	if err != nil {
		t.Fatalf("arbiter resolve failed: %v", err)
	}
	if len(leaders) != 2 {
		t.Fatalf("expected 2 leaders before arbitration, got %d", len(leaders))
	}
	if winner.NodeID != 0 {
		t.Fatalf("expected node 0 to win by fresher log, got node %d", winner.NodeID)
	}

	time.Sleep(100 * time.Millisecond)

	_, leader0 := rafts[0].GetState()
	_, leader3 := rafts[3].GetState()
	if !leader0 {
		t.Fatalf("winner node 0 should remain leader")
	}
	if leader3 {
		t.Fatalf("loser node 3 should step down after arbitration")
	}
}
