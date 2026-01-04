package raft //公用的放在raft.go里面

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

import (
	//	"bytes"

	"fmt"
	"sync"
	"sync/atomic"
	"time"

	//	"course/labgob"
	"consensus/labrpc"
	"consensus/network"
	"consensus/web/events"
)

// ClientEnd 接口定义了RPC客户端的通用接口
type ClientEnd interface {
	Call(svcMeth string, args interface{}, reply interface{}) bool
}

type TimeoutTier struct { //超时时间梯度
	MinTimeout time.Duration
	MaxTimeout time.Duration
}

var ( //不同的优先超时档位获得不同的超时时间
	FirstTier  = TimeoutTier{MinTimeout: 150 * time.Millisecond, MaxTimeout: 233 * time.Millisecond}
	SecondTier = TimeoutTier{MinTimeout: 233 * time.Millisecond, MaxTimeout: 316 * time.Millisecond}
	ThirdTier  = TimeoutTier{MinTimeout: 317 * time.Millisecond, MaxTimeout: 400 * time.Millisecond}
)

// var ( //不同的优先超时档位获得不同的超时时间（增加candidate停留时间）
// 	FirstTier  = TimeoutTier{MinTimeout: 250 * time.Millisecond, MaxTimeout: 350 * time.Millisecond}
// 	SecondTier = TimeoutTier{MinTimeout: 350 * time.Millisecond, MaxTimeout: 450 * time.Millisecond}
// 	ThirdTier  = TimeoutTier{MinTimeout: 450 * time.Millisecond, MaxTimeout: 550 * time.Millisecond}
// )

const (
	electionTimeoutMin time.Duration = 250 * time.Millisecond //定义选举超时的上下限 250ms
	electionTimeoutMax time.Duration = 400 * time.Millisecond //定义选举超时的上下限
	replicateInterval  time.Duration = 50 * time.Millisecond  //日志复制/心跳的间隔，需要比选举超时的最小值小

)

const (
	InvalidTerm  = 0
	InvalidIndex = 0
)

type Role string

const (
	Follower  Role = "Follower"  // 跟随者
	Candidate Role = "Candidate" // 候选人
	Leader    Role = "Leader"    // 领导者
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part PartD you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct { //应用消息
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For PartD Snapshot:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex  // Lock to protect shared access to this peer's state 锁，保护节点状态
	peers     []ClientEnd // RPC end points of all peers 所有对等节点的RPC端点，包含所有节点的列表
	persister *Persister  // Object to hold this peer's persisted state  保存对等节点持久化状态的对象
	me        int         // this peer's index into peers[] 这个对等节点在peers[]中的索引
	dead      int32       // set by Kill() 通过Kill()设置

	// Your data here (PartA, PartB, PartC).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role        Role // 当前角色
	currentTerm int  // 当前任期
	votedFor    int  // 在currentTerm是否投票，投过谁。-1表示没有投票

	Credit     float64 // 信用值
	GeoPri     float64 // 地理优先级
	HwParams   float64 // 硬件参数指标
	twooo2flag bool    //二取二标识

	//每个peer本地的日志
	log []LogEntry // 日志

	//仅leader使用，相当于每个peer的视图，包括匹配点和试探点
	nextIndex        []int     // 对于每一个peer，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	matchIndex       []int     // 对于每一个peer，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	creditview       []float64 // 记录每个peer的信用值 Ci
	Delay            [][]int64 // 记录每个peer的通信延迟,每一行对应一个节点在一次更新时间内的一系列延迟
	delayIndex       []int     // 当前Delay数组的索引位置
	validVoteCount   []int64   //记录节点投票一致次数 Vi
	participateCount []int64   //记录节点历史共识占比，即完整共识次数 Hi
	penalty          []float64 //动态惩罚值，根据违规行为计算 Pi
	behaviorScore    []float64 //记录节点历史表现 Bi
	votesReceived    []bool    // 记录在当前任期哪些节点投票给了自己，以便计算Vi
	totalConsensus   int64     //记录在未更新的这段时间内，总共进行了多少次共识
	noResponse       []int64   // 记录每个peer的未响应次数

	//fields for apply loop

	commitIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	applyCh   chan ApplyMsg // 应用消息通道
	applyCond *sync.Cond    // 应用消息条件变量

	electionStart   time.Time     // 选举开始时间（选取时钟起始点）
	electionTimeout time.Duration // 选举超时时间（随机）

	//leader主动卸任
	leaderStartTime time.Time     // leader任期开始时间
	leaderTimeout   time.Duration // leader任期时长（24小时）
}

// 再定义了基本的数据结构之后，需要表征状态机的三个函数，使peer可以在不同的角色之间进行转换
func (rf *Raft) becomeFollowerLocked(term int) { //只有加锁之后才能调用这个函数，传入的term表示我们要变为这个term的follower
	if term < rf.currentTerm { //如果请求的term比自己的term小，直接拒绝
		LOG(rf.me, rf.currentTerm, DError, "Can't become Follower, lower term: T%d", term)
		return
	}

	if rf.role == Leader {
		events.Log("election", "Leader %d is stepping down.", rf.me)
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s->Follower, For T%v->T%v", rf.role, rf.currentTerm, term)

	rf.role = Follower
	shouldPersist := rf.currentTerm != term //如果当前term和请求的term不一样，需要持久化
	if term > rf.currentTerm {
		rf.votedFor = -1 // 重置投票
	}
	rf.currentTerm = term
	if shouldPersist {
		rf.persistLocked()
	}

}

func (rf *Raft) becomeCandidateLocked() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DError, "Leader can't become Candidate")
		return
	}

	events.Log("election", "Node %d is starting a new election for Term %d.", rf.me, rf.currentTerm+1)
	LOG(rf.me, rf.currentTerm, DVote, "%s->Candidate, For T%d", rf.role, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me //投票给自己
	rf.votesReceived[rf.me] = true
	rf.persistLocked()
}

func (rf *Raft) becomeLeaderLocked() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DError, "Only Candidate can become Leader")
		return
	}

	events.Log("election", "Node %d has become the new Leader for Term %d.", rf.me, rf.currentTerm)
	LOG(rf.me, rf.currentTerm, DLeader, "Become Leader in T%d", rf.currentTerm)

	rf.role = Leader
	for peer := 0; peer < len(rf.peers); peer++ { //初始化视图
		rf.nextIndex[peer] = len(rf.log) //所有人的nextIndex都是leader的最后
		rf.matchIndex[peer] = 0
		if rf.votesReceived[peer] { //如果该peer投的就是自己
			rf.validVoteCount[peer] += 1 //Vi的计数
		}
	}

	// 设置leader24h倒计时开始时间
	rf.leaderStartTime = time.Now()

	// 启动leader卸任计时器
	go rf.leaderTimeoutTicker(rf.currentTerm)

	//惩罚衰减函数
	go rf.decayPenalty()
}

func (rf *Raft) firstLogFor(term int) int {
	for idx, entry := range rf.log {
		if entry.Term == term {
			return idx
		} else if entry.Term > term {
			break
		}

	}
	return InvalidIndex
}

func (rf *Raft) logString() string { //返回哪一段日志属于哪一个任期，如[0, 2]T1,[3, 7]T2,[8, 10]T3
	var terms string
	prevTerm := rf.log[0].Term
	prevStart := 0
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term != prevTerm { //从头开始找，当遇到任期变化的地方时，记录这一任期内的日志，以[%d, %d]T%d形式表示
			terms += fmt.Sprintf("[%d, %d]T%d,", prevStart, i-1, prevTerm)
			prevTerm = rf.log[i].Term //这一任期找完之后再找下一任期
			prevStart = i
		}
	}
	terms += fmt.Sprintf("[%d, %d]T%d", prevStart, len(rf.log)-1, prevTerm)
	return terms
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) { //raft对外提供的接口，返回当前的term和是否是leader

	// Your code here (PartA)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) GetID() int {
	return rf.me
}

// GetPeerLatencies returns the average latency to each peer from the leader's perspective.
// The result is a map of peer ID to average latency in milliseconds.
// 获取每个peer的平均延迟
func (rf *Raft) GetPeerLatencies() map[int]int64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return nil
	}

	latencies := make(map[int]int64)
	for peerID, delays := range rf.Delay {
		if peerID == rf.me {
			continue
		}
		if len(delays) == 0 {
			latencies[peerID] = -1 // Indicates no data yet
			continue
		}
		var total int64
		for _, d := range delays {
			total += d
		}
		latencies[peerID] = total / int64(len(delays))
	}
	return latencies
}

// StepDown forces the current node to become a follower.
// This is useful for testing, e.g., to simulate a leader failure.
func (rf *Raft) StepDown() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	events.Log("fault", "Fault injected: Forcing Leader %d to step down.", rf.me)

	// Increment term to force other nodes to update their state
	rf.becomeFollowerLocked(rf.currentTerm + 1)

	// Give other nodes a better chance to get elected.冷静期
	// We reset our own election timer to a value greater than the normal max timeout.
	rf.electionTimeout = electionTimeoutMax + 100*time.Millisecond //500ms的冷静期
	rf.electionStart = time.Now()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (PartD).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 返回值是日志的索引，当前的term，是否是leader

// 获取当前已提交的最高日志索引
// 线程安全（通过互斥锁保护）
func (rf *Raft) GetCommitIndex() int {
	rf.mu.Lock()         // 加锁防止数据竞争
	defer rf.mu.Unlock() // 确保函数返回时解锁

	return rf.commitIndex // 返回当前提交索引
}

func (rf *Raft) Start(command interface{}) (int, int, bool) { //raft对外提供的接口，开始一个新的日志条目
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}
	events.Log("commit", "Leader %d received a new command.", rf.me)
	entry := LogEntry{
		Command:      command,
		Term:         rf.currentTerm,
		CommandValid: true,
	}
	rf.log = append(rf.log, entry)

	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept Log [%d] T%d", len(rf.log)-1, rf.currentTerm)
	// Your code here (PartB).
	rf.persistLocked() //持久化日志

	return len(rf.log) - 1, rf.currentTerm, true
}

// 带签名的Start
func (rf *Raft) StartSigned(command interface{}, signature []byte) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果不是leader，直接返回false
	if rf.role != Leader {
		return 0, 0, false
	}

	// 验证签名
	valid := VerifyClientMessage(command, signature)

	//如果消息不合法，early return
	if !valid {
		return 0, 0, false
	}

	// 添加日志条目，包含签名和验证结果
	rf.log = append(rf.log, LogEntry{
		CommandValid:   true,
		Command:        command,
		Term:           rf.currentTerm,
		Signature:      signature,
		SignatureValid: valid,
	})

	LOG(rf.me, rf.currentTerm, DLeader, "Leader accept signed Log [%d] T%d, SignatureValid: %v", len(rf.log)-1, rf.currentTerm, valid)

	// 持久化日志
	rf.persistLocked()

	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() { //关闭节点
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool { //判断节点是否关闭
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) contextLostLocked(role Role, term int) bool { //判断上下文是否丢失
	return !(rf.currentTerm == term && rf.role == role)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers interface{}, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	// 根据传入的类型设置peers
	switch p := peers.(type) {
	case []*labrpc.ClientEnd:
		// 原始labrpc方式
		clientEnds := make([]ClientEnd, len(p))
		for i, client := range p {
			clientEnds[i] = client
		}
		rf.peers = clientEnds
	case []*network.ClientEnd:
		// 新的network方式
		clientEnds := make([]ClientEnd, len(p))
		for i, client := range p {
			clientEnds[i] = client
		}
		rf.peers = clientEnds
	default:
		panic("不支持的peers类型")
	}

	rf.persister = persister
	rf.me = me

	// Your initialization code here (PartA, PartB, PartC). 初始化代码（一个peer第一次起来）
	rf.role = Follower
	rf.currentTerm = 1
	rf.votedFor = -1

	// 空日志，避免一些边界条件的判断
	rf.log = append(rf.log, LogEntry{Term: InvalidTerm})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	//初始化Raft结构信用值相关字段
	rf.resetPerPeerLocked()

	//首次超时时间
	rf.electionStart = time.Now()
	//根据信用值获得不同梯度的超时时间
	rf.electionTimeout, _ = rf.getTimeout()
	// rf.electionTimeout = time.Duration(rand.Intn(1000)) * time.Millisecond
	// rf.electionTimeout = time.Duration(10) * time.Millisecond

	// 初始化apply loop
	rf.applyCh = applyCh

	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash 宕机重启之后会从之前保留的状态里读出来，覆盖前面的字段
	rf.readPersist(persister.ReadRaftState())

	// 启动选举ticker
	go rf.electionTicker()

	// 启动应用ticker
	go rf.applicationTicker() //选举延迟测试先不启用

	// 启动信用更新ticker
	go rf.startCreditUpdateTicker()

	// 初始化leader任期时长（24小时）
	rf.leaderTimeout = 24 * time.Hour

	return rf
}

// 导出RPC方法及其参数类型
func init() {
	// 这里为空，但确保了RequestVoteArgs等类型的可见性

}

// var quorum int
// var

// func DetectByzantineLeader() { //检测拜占庭领导者,若集群中除leader外有超过一半的节点认为存在拜占庭领导者，则认为存在拜占庭领导者
//
//		quorum = len(peers)/2 + 1
//	}

// 获取信用值
func getCredit(rf *Raft) float64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.Credit
}

// resetPerPeerLocked 为peer信用值相关的所有字段重新分配内存，是初始化函数的一部分
func (rf *Raft) resetPerPeerLocked() {

	rf.creditview = make([]float64, len(rf.peers)) // 记录每个peer的信用值 Ci

	// 初始化二维延迟数组
	rf.Delay = make([][]int64, len(rf.peers)) //纪律每个节点的通信延迟 Di
	for i := range rf.Delay {
		rf.Delay[i] = make([]int64, 0) // 每个peer的延迟记录初始为空切片
	}

	rf.validVoteCount = make([]int64, len(rf.peers))   //记录每个peer的投票一致 Vi
	rf.participateCount = make([]int64, len(rf.peers)) //记录每个peer的完成共识 Hi
	rf.penalty = make([]float64, len(rf.peers))        //Pi
	rf.behaviorScore = make([]float64, len(rf.peers))  //Bi
	rf.noResponse = make([]int64, len(rf.peers))
	rf.votesReceived = make([]bool, len(rf.peers))
	rf.totalConsensus = 0 //记录T内总共进行了多少次共识

	// initial credit
	rf.Credit = 0.6 + 0.1*rf.GeoPri + 0.1*rf.HwParams
	for peer := 0; peer < len(rf.peers); peer++ {
		rf.nextIndex[peer] = 1
		rf.matchIndex[peer] = 0
		rf.creditview[peer] = 0.6 // 初始信用值
		rf.penalty[peer] = 0.0
		rf.behaviorScore[peer] = 0.5 // 初始表现分数
	}
}
