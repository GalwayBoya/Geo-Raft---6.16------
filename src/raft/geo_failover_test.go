package raft

import (
	"consensus/labgob"
	"consensus/labrpc"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 故障转移测试结果
type FailoverResult struct {
	延迟   int     // 跨数据中心延迟(ms)
	丢包率  float64 // 跨数据中心丢包率
	切换时间 float64 // 切换时间(秒)
	成功   bool    // 是否成功切换到期望的数据中心
	原DC  int     // 原Leader数据中心
	新DC  int     // 新Leader数据中心
}

// 主测试函数
func TestGeoFailover(t *testing.T) {
	// 创建数据目录
	dataDir := "data/geo_failover"
	os.MkdirAll(dataDir, 0755)

	// 配置参数
	delays := []int{50, 100, 200}
	lossRates := []float64{0.0, 0.01, 0.05}

	// 初始化随机数
	rand.Seed(time.Now().UnixNano())

	// 存储结果
	var results []FailoverResult

	// 对每种配置运行测试
	for _, delay := range delays {
		for _, lossRate := range lossRates {
			t.Logf("测试配置: 延迟=%dms, 丢包率=%.2f", delay, lossRate)

			// 每种配置测试3次(正式测试可改为10-100次)
			for i := 0; i < 100; i++ {
				result := runFailoverTest(t, delay, lossRate)
				results = append(results, result)

				// 报告结果
				t.Logf("  测试%d: 切换时间=%.2f秒, 成功=%v, %d→%d",
					i+1, result.切换时间, result.成功, result.原DC, result.新DC)

				// 给系统休息时间
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	// 保存结果到CSV
	saveResults(results, filepath.Join(dataDir, "results-100.csv"))
}

// 单次故障转移测试
func runFailoverTest(t *testing.T, delay int, lossRate float64) FailoverResult {
	// 配置
	nodesPerDC := 4 // 每个数据中心4个节点
	numDCs := 2     // 2个数据中心
	totalNodes := nodesPerDC * numDCs
	expectedDC := 1 // 期望故障转移到第二个数据中心

	// 创建网络
	net := labrpc.MakeNetwork()

	// 创建节点
	servers := make([]*labrpc.Server, totalNodes)
	ends := make([][]*labrpc.ClientEnd, totalNodes)
	persisters := make([]*Persister, totalNodes)
	applyChs := make([]chan ApplyMsg, totalNodes)
	rafts := make([]*Raft, totalNodes)

	// 初始化网络连接
	for i := 0; i < totalNodes; i++ {
		servers[i] = labrpc.MakeServer()
		persisters[i] = MakePersister()
		applyChs[i] = make(chan ApplyMsg, 100)
		ends[i] = make([]*labrpc.ClientEnd, totalNodes)

		for j := 0; j < totalNodes; j++ {
			ends[i][j] = net.MakeEnd(fmt.Sprintf("end-%d-%d", i, j))
			net.Connect(fmt.Sprintf("end-%d-%d", i, j), j)
			net.Enable(fmt.Sprintf("end-%d-%d", i, j), true)
		}
	}

	// 初始化为可靠网络
	net.Reliable(true)

	// 创建Raft节点
	for i := 0; i < totalNodes; i++ {
		// 先创建Raft节点
		rafts[i] = Make(ends[i], i, persisters[i], applyChs[i])

		// 设置期望节点的优先级 (第二个DC的第一个节点)
		if i == nodesPerDC {
			rafts[i].GeoPri = 1
			rafts[i].HwParams = 1
		}

		// 注册RPC服务
		servers[i].AddServiceWithName(rafts[i], "Raft")
		net.AddServer(i, servers[i])
	}

	// 等待初始Leader选举
	time.Sleep(2 * time.Second)

	// 查找当前Leader
	initialLeaderID := -1
	initialLeaderDC := -1
	initialTerm := 0

	for i := 0; i < totalNodes; i++ {
		if rafts[i].killed() {
			continue
		}

		term, isLeader := rafts[i].GetState()
		if isLeader {
			initialLeaderID = i
			initialLeaderDC = i / nodesPerDC
			initialTerm = term
			t.Logf("初始Leader: 节点%d (DC%d), Term=%d", i, initialLeaderDC, term)
			break
		}
	}

	// // 如果没有找到Leader，随机选择一个
	// if initialLeaderID == -1 {
	// 	initialLeaderID = 0
	// 	initialLeaderDC = 0
	// 	t.Logf("未找到Leader，默认使用节点0")
	// }

	// 设置跨数据中心网络延迟和丢包
	setNetworkConditions(net, delay, lossRate, nodesPerDC, numDCs)

	// 注入命令让系统稳定，注意这里要使用带签名的start
	for i := 0; i < 3; i++ {
		if initialLeaderID != -1 {

			command := fmt.Sprintf("cmd-%d", i)
			signature, _ := SignCommand(command)
			rafts[initialLeaderID].StartSigned(command, signature)
			time.Sleep(100 * time.Millisecond)
		}
	}

	// 注入故障
	t.Logf("杀死Leader节点%d (DC%d)", initialLeaderID, initialLeaderDC)
	startTime := time.Now()

	// 杀死Leader
	rafts[initialLeaderID].Kill()

	// 断开Leader连接
	for j := 0; j < totalNodes; j++ {
		if j != initialLeaderID {
			net.Enable(fmt.Sprintf("end-%d-%d", initialLeaderID, j), false)
			net.Enable(fmt.Sprintf("end-%d-%d", j, initialLeaderID), false)
		}
	}

	// 等待新Leader选举
	newLeaderID := -1
	newLeaderDC := -1
	for i := 0; i < 50; i++ { // 最多等待5秒
		for n := 0; n < totalNodes; n++ {
			if n == initialLeaderID || rafts[n].killed() {
				continue
			}

			term, isLeader := rafts[n].GetState()
			if isLeader && term > initialTerm {
				newLeaderID = n
				newLeaderDC = n / nodesPerDC
				t.Logf("新Leader: 节点%d (DC%d), Term=%d", n, newLeaderDC, term)
				break
			}
		}

		if newLeaderID != -1 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	// 计算故障转移时间
	switchTime := time.Since(startTime).Seconds()

	// 清理资源
	for i := 0; i < totalNodes; i++ {
		if rafts[i] != nil && !rafts[i].killed() {
			rafts[i].Kill()
		}
	}

	// 返回结果
	return FailoverResult{
		延迟:   delay,
		丢包率:  lossRate,
		切换时间: switchTime,
		成功:   newLeaderDC == expectedDC,
		原DC:  initialLeaderDC,
		新DC:  newLeaderDC,
	}
}

// 设置网络条件
func setNetworkConditions(net *labrpc.Network, delay int, lossRate float64, nodesPerDC, numDCs int) {
	totalNodes := nodesPerDC * numDCs

	// 设置不可靠网络
	net.Reliable(false)

	// 根据延迟设置
	net.LongDelays(delay > 150)

	// 对跨数据中心连接设置丢包
	for i := 0; i < totalNodes; i++ {
		dcI := i / nodesPerDC

		for j := 0; j < totalNodes; j++ {
			dcJ := j / nodesPerDC

			// 跨数据中心连接
			if dcI != dcJ {
				// 根据丢包率随机禁用连接
				if rand.Float64() < lossRate {
					net.Enable(fmt.Sprintf("end-%d-%d", i, j), false)
				}
			}
		}
	}
}

// 保存结果到CSV
func saveResults(results []FailoverResult, filepath string) {
	file, err := os.Create(filepath)
	if err != nil {
		fmt.Printf("创建CSV文件失败: %v\n", err)
		return
	}
	defer file.Close()

	file.WriteString("延迟(ms),丢包率,切换时间(秒),成功,原DC,新DC\n")

	for _, r := range results {
		successInt := 0
		if r.成功 {
			successInt = 1
		}

		line := fmt.Sprintf("%d,%.2f,%.2f,%d,%d,%d\n",
			r.延迟, r.丢包率, r.切换时间, successInt, r.原DC, r.新DC)

		file.WriteString(line)
	}
}

// 初始化
func init() {
	labgob.Register([]interface{}{})
	labgob.Register(ApplyMsg{})
	labgob.Register(RequestVoteArgs{})
	labgob.Register(RequestVoteReply{})
	labgob.Register(AppendEntriesArgs{})
	labgob.Register(AppendEntriesReply{})
}
