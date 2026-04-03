package raft

// Arbiter 是一个“外部第三方仲裁节点”的代码实现。
import (
	"consensus/labgob"
	"consensus/labrpc"
	"consensus/network"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ArbiterProbeArgs / Reply 用于仲裁节点周期探测各节点状态。
type ArbiterProbeArgs struct{}

type ArbiterProbeReply struct {
	NodeID       int
	Term         int
	Role         string
	IsLeader     bool
	CommitIndex  int
	LastLogIndex int
	LastLogTerm  int
	Credit       float64
}

// ArbiterDecisionArgs / Reply 用于仲裁节点对双主进行裁决。
type ArbiterDecisionArgs struct {
	WinnerID int
	Reason   string
}

type ArbiterDecisionReply struct {
	Term      int
	WasLeader bool
	Accepted  bool
}

func init() {
	labgob.Register(ArbiterProbeArgs{})
	labgob.Register(ArbiterProbeReply{})
	labgob.Register(ArbiterDecisionArgs{})
	labgob.Register(ArbiterDecisionReply{})
}

// ArbiterProbe 是一个只读 RPC，供外部仲裁节点采样节点运行状态。
func (rf *Raft) ArbiterProbe(args *ArbiterProbeArgs, reply *ArbiterProbeReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastIndex := len(rf.log) - 1
	lastTerm := InvalidTerm
	if lastIndex >= 0 {
		lastTerm = rf.log[lastIndex].Term
	}

	reply.NodeID = rf.me
	reply.Term = rf.currentTerm
	reply.Role = string(rf.role)
	reply.IsLeader = rf.role == Leader
	reply.CommitIndex = rf.commitIndex
	reply.LastLogIndex = lastIndex
	reply.LastLogTerm = lastTerm
	if rf.me < len(rf.creditview) {
		reply.Credit = rf.creditview[rf.me]
	} else {
		reply.Credit = rf.Credit
	}
	return nil
}

// ApplyArbiterDecision 接收仲裁节点的裁决结果。
// 若本节点是领导者但不是赢家，则立即退位并进入一个短冷静期，避免再次抢主。
func (rf *Raft) ApplyArbiterDecision(args *ArbiterDecisionArgs, reply *ArbiterDecisionReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.WasLeader = rf.role == Leader
	reply.Accepted = true

	if rf.me != args.WinnerID && rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DLeader, "Lose arbitration, step down. Winner=S%d, reason=%s", args.WinnerID, args.Reason)
		rf.becomeFollowerLocked(rf.currentTerm)
		rf.electionTimeout = electionTimeoutMax + 200*time.Millisecond
		rf.electionStart = time.Now()
	}
	return nil
}

// ArbitrationSample 记录一次仲裁采样结果。
type ArbitrationSample struct {
	NodeID       int
	RTT          time.Duration
	Reachable    bool
	Term         int
	IsLeader     bool
	CommitIndex  int
	LastLogIndex int
	LastLogTerm  int
	Credit       float64
}

// Arbiter 是“外部第三方仲裁节点”的代码实现。
// 它不是 Raft 成员，不参与日志复制；仅用于旁路监测和在测试场景下执行裁决。
type Arbiter struct {
	mu               sync.Mutex
	peers            []ClientEnd
	history          map[int][]time.Duration
	syntheticLatency map[int]time.Duration
}

// NewArbiter 支持 []*labrpc.ClientEnd、[]*network.ClientEnd 或 []ClientEnd 三种输入。
func NewArbiter(peers interface{}) *Arbiter {
	arb := &Arbiter{
		history:          make(map[int][]time.Duration),
		syntheticLatency: make(map[int]time.Duration),
	}

	switch p := peers.(type) {
	case []ClientEnd:
		arb.peers = p
	case []*labrpc.ClientEnd:
		clientEnds := make([]ClientEnd, len(p))
		for i, client := range p {
			clientEnds[i] = client
		}
		arb.peers = clientEnds
	case []*network.ClientEnd:
		clientEnds := make([]ClientEnd, len(p))
		for i, client := range p {
			clientEnds[i] = client
		}
		arb.peers = clientEnds
	default:
		panic("unsupported arbiter peers type")
	}

	return arb
}

// SetSyntheticLatency 为测试场景注入一条“仲裁节点到目标节点”的附加时延。
// 当前实验网络不天然支持按链路精细建模时，可通过该方法在测试中模拟仲裁观测差异。
func (a *Arbiter) SetSyntheticLatency(nodeID int, d time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.syntheticLatency[nodeID] = d
}

func (a *Arbiter) getSyntheticLatency(nodeID int) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.syntheticLatency[nodeID]
}

func (a *Arbiter) record(nodeID int, rtt time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.history[nodeID] = append(a.history[nodeID], rtt)
}

func (a *Arbiter) ProbeNode(nodeID int) (ArbitrationSample, bool) {
	if nodeID < 0 || nodeID >= len(a.peers) {
		return ArbitrationSample{}, false
	}
	args := &ArbiterProbeArgs{}
	reply := &ArbiterProbeReply{}
	start := time.Now()
	ok := a.peers[nodeID].Call("Raft.ArbiterProbe", args, reply)
	rtt := time.Since(start)
	if extra := a.getSyntheticLatency(nodeID); extra > 0 {
		rtt += extra
	}
	if !ok {
		return ArbitrationSample{NodeID: nodeID, RTT: rtt, Reachable: false}, false
	}
	a.record(nodeID, rtt)
	return ArbitrationSample{
		NodeID:       reply.NodeID,
		RTT:          rtt,
		Reachable:    true,
		Term:         reply.Term,
		IsLeader:     reply.IsLeader,
		CommitIndex:  reply.CommitIndex,
		LastLogIndex: reply.LastLogIndex,
		LastLogTerm:  reply.LastLogTerm,
		Credit:       reply.Credit,
	}, true
}

func (a *Arbiter) ProbeAll() []ArbitrationSample {
	results := make([]ArbitrationSample, 0, len(a.peers))
	for i := 0; i < len(a.peers); i++ {
		sample, _ := a.ProbeNode(i)
		results = append(results, sample)
	}
	return results
}

func (a *Arbiter) DetectLeaders() []ArbitrationSample {
	all := a.ProbeAll()
	leaders := make([]ArbitrationSample, 0)
	for _, s := range all {
		if s.Reachable && s.IsLeader {
			leaders = append(leaders, s)
		}
	}
	return leaders
}

// compareFreshness 返回：
// 1  -> a 比 b 更新
// -1 -> b 比 a 更新
// 0  -> 两者相同
func compareFreshness(a, b ArbitrationSample) int {
	if a.LastLogTerm != b.LastLogTerm {
		if a.LastLogTerm > b.LastLogTerm {
			return 1
		}
		return -1
	}
	if a.LastLogIndex != b.LastLogIndex {
		if a.LastLogIndex > b.LastLogIndex {
			return 1
		}
		return -1
	}
	if a.CommitIndex != b.CommitIndex {
		if a.CommitIndex > b.CommitIndex {
			return 1
		}
		return -1
	}
	return 0
}

// SelectWinner 选择裁决赢家。
// 说明：虽然需求中提到了“通信延迟”和“日志更新更全”两个条件，
// 这里代码采取“日志新鲜度优先、通信时延次之”的顺序，原因是仲裁首先需要保证状态一致性，
// 在日志新鲜度相同或接近时，再使用时延作为性能侧裁决条件。
func (a *Arbiter) SelectWinner(candidates []ArbitrationSample) (ArbitrationSample, string, bool) {
	if len(candidates) == 0 {
		return ArbitrationSample{}, "", false
	}

	sorted := make([]ArbitrationSample, len(candidates))
	copy(sorted, candidates)
	sort.Slice(sorted, func(i, j int) bool {
		cmp := compareFreshness(sorted[i], sorted[j])
		if cmp != 0 {
			return cmp > 0
		}
		if sorted[i].RTT != sorted[j].RTT {
			return sorted[i].RTT < sorted[j].RTT
		}
		if sorted[i].Credit != sorted[j].Credit {
			return sorted[i].Credit > sorted[j].Credit
		}
		return sorted[i].NodeID < sorted[j].NodeID
	})

	winner := sorted[0]
	reason := fmt.Sprintf("winner=S%d, fresherLog=[%d]T%d, commit=%d, rtt=%v, credit=%.3f",
		winner.NodeID, winner.LastLogIndex, winner.LastLogTerm, winner.CommitIndex, winner.RTT, winner.Credit)
	return winner, reason, true
}

func (a *Arbiter) sendDecision(nodeID int, args *ArbiterDecisionArgs, reply *ArbiterDecisionReply) bool {
	if nodeID < 0 || nodeID >= len(a.peers) {
		return false
	}
	return a.peers[nodeID].Call("Raft.ApplyArbiterDecision", args, reply)
}

// ResolveSplitBrain 探测双主并对败者发送裁决。
// 返回赢家、当前检测到的所有 leader 采样及错误信息。
func (a *Arbiter) ResolveSplitBrain() (ArbitrationSample, []ArbitrationSample, error) {
	leaders := a.DetectLeaders()
	if len(leaders) == 0 {
		return ArbitrationSample{}, nil, fmt.Errorf("arbiter found no reachable leader")
	}
	if len(leaders) == 1 {
		return leaders[0], leaders, nil
	}

	winner, reason, ok := a.SelectWinner(leaders)
	if !ok {
		return ArbitrationSample{}, leaders, fmt.Errorf("arbiter failed to choose winner")
	}

	args := &ArbiterDecisionArgs{WinnerID: winner.NodeID, Reason: reason}
	for _, leader := range leaders {
		if leader.NodeID == winner.NodeID {
			continue
		}
		reply := &ArbiterDecisionReply{}
		_ = a.sendDecision(leader.NodeID, args, reply)
	}
	return winner, leaders, nil
}
