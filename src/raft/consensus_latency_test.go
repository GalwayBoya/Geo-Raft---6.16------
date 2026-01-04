package raft

import (
	"consensus/labrpc"
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// 测试不同节点数量下的共识延迟
// go test -timeout 60m -v -run TestConsensusLatency
func TestConsensusLatency(t *testing.T) {
	// 跳过耗时测试的条件判断
	if testing.Short() {
		t.Skip("跳过耗时测试")
	}

	// 创建输出目录
	err := os.MkdirAll("data/consensus_latency", 0755)
	if err != nil {
		t.Fatalf("创建输出目录失败: %v", err)
	}

	// 设置要测试的节点数量
	nodeCounts := []int{4, 8, 12, 16, 20, 24}
	// 每个配置的测试次数
	iterations := 100

	t.Logf("开始共识延迟测试：节点配置 %v，每配置迭代 %d 次", nodeCounts, iterations)

	// 创建CSV文件并写入表头
	file, err := os.Create("data/consensus_latency/consensus_latency_results2 4-24 100.csv")
	if err != nil {
		t.Fatalf("无法创建CSV文件: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV表头
	writer.Write([]string{"NodeCount", "Iteration", "ConsensusLatency_ms"})

	// 对每个节点数量进行测试
	for _, nodeCount := range nodeCounts {
		t.Logf("开始测试 %d 个节点的共识延迟", nodeCount)

		for i := 1; i <= iterations; i++ {
			// 测试间短暂休息
			time.Sleep(100 * time.Millisecond)

			// 测量共识延迟
			latency, success := measureConsensusLatency(t, nodeCount)

			if !success {
				// 共识失败，记录并继续，这一次不会记录
				t.Logf("节点数 %d: 第 %d 次测试超时或失败", nodeCount, i)
				continue
			}

			// 写入测试结果到CSV
			writer.Write([]string{
				fmt.Sprintf("%d", nodeCount),
				fmt.Sprintf("%d", i),
				fmt.Sprintf("%.2f", latency.Seconds()*1000), // 转换为毫秒
			})

			// 每20次测试输出进度并刷新
			if i%20 == 0 {
				t.Logf("节点数 %d: 已完成 %d/%d 次测试", nodeCount, i, iterations)
				writer.Flush()
			}
		}

		// 完成一组测试
		writer.Flush()
		t.Logf("节点数 %d 测试完成", nodeCount)
	}

	t.Logf("测试完成，结果已保存到 data/consensus_latency/consensus_latency_results2 4-24 100.csv")
}

// 测量单次共识延迟的函数
func measureConsensusLatency(t *testing.T, numServers int) (time.Duration, bool) {
	// 创建模拟网络
	net := labrpc.MakeNetwork()

	// 创建网络连接
	ends := make([][]*labrpc.ClientEnd, numServers)
	for i := 0; i < numServers; i++ {
		ends[i] = make([]*labrpc.ClientEnd, numServers)
		for j := 0; j < numServers; j++ {
			endName := fmt.Sprintf("end-%d-%d", i, j)
			ends[i][j] = net.MakeEnd(endName)
			net.Connect(endName, j)
			net.Enable(endName, true)
		}
	}

	// 创建持久化器和应用通道
	persisters := make([]*Persister, numServers)
	applyChs := make([]chan ApplyMsg, numServers)
	for i := 0; i < numServers; i++ {
		persisters[i] = MakePersister()
		applyChs[i] = make(chan ApplyMsg, 100)
	}

	// 创建Raft节点
	rafts := make([]*Raft, numServers)
	for i := 0; i < numServers; i++ {
		// 创建Raft实例
		rf := Make(ends[i], i, persisters[i], applyChs[i])
		rafts[i] = rf

		// 注册RPC服务
		srv := labrpc.MakeServer()
		srv.AddServiceWithName(rf, "Raft")
		net.AddServer(i, srv)
	}

	// 确保清理资源全部kill
	defer func() {
		for i := 0; i < numServers; i++ {
			if rafts[i] != nil {
				rafts[i].Kill()
			}
		}
	}()

	// 等待选出Leader
	leader := -1
	startTime := time.Now()
	timeout := 2 * time.Second

	//循环检测：已超时且leader还未选出
	for time.Since(startTime) < timeout && leader == -1 {
		for i := 0; i < numServers; i++ {
			if rafts[i] == nil {
				continue
			}
			_, isLeader := rafts[i].GetState()
			if isLeader {
				leader = i
				break
			}
		}
		if leader == -1 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	if leader == -1 {
		t.Logf("选举Leader超时")
		return 0, false
	}

	// 使用一个通道来接收应用消息的通知
	commandApplied := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	// 监听应用通道，等待命令被应用
	go func() {
		defer wg.Done()
		// 我们只需要一条成功提交的消息
		for msg := range applyChs[leader] {
			if msg.CommandValid && msg.Command == "test-command" {
				close(commandApplied)
				return
			}
		}
	}()

	command := "test-command"
	signature, _ := SignCommand(command)
	rafts[leader].StartSigned(command, signature)

	consensusTimeout := 3 * time.Second //共识延迟超时时间

	// Leader提交一个新的命令，并记录开始时间
	consensusStartTime := time.Now()

	// index, term, isLeader := rafts[leader].Start("test-command")

	// if !isLeader {
	// 	t.Logf("开始共识时Leader已改变")
	// 	return 0, false
	// }

	// t.Logf("命令已提交给Leader(节点%d)处理: 索引=%d, 任期=%d", leader, index, term)

	// consensusTimeout := 3 * time.Second

	// 等待命令被应用或超时
	select {
	case <-commandApplied:
		// 命令已被应用
		return time.Since(consensusStartTime), true
	case <-time.After(consensusTimeout):
		// 超时
		t.Logf("共识超时，命令未在%v内完成", consensusTimeout)
		return 0, false
	}
}
