package raft

import (
	"consensus/labrpc"
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"
)

// 测试不同节点数量下的选举延迟
func TestElectionLatency(t *testing.T) {
	//go test -timeout 60m -v -run TestElectionLatency
	// 跳过耗时测试的条件判断
	if testing.Short() {
		t.Skip("跳过耗时测试")
	}

	// 设置要测试的节点数量
	nodeCounts := []int{4, 8, 12, 16, 20, 24}
	// 每个配置的测试次数
	iterations := 100

	t.Logf("开始选举延迟测试：节点配置 %v，每配置迭代 %d 次", nodeCounts, iterations)

	// 创建CSV文件并写入表头
	file, err := os.Create("data/election_latency/election_latency_results2 4-24 100.csv")
	if err != nil {
		t.Fatalf("无法创建CSV文件: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// 写入CSV表头
	writer.Write([]string{"NodeCount", "Iteration", "ElectionLatency_ms"})

	// 对每个节点数量进行测试
	for _, nodeCount := range nodeCounts {
		t.Logf("开始测试 %d 个节点的选举延迟", nodeCount)

		for i := 1; i <= iterations; i++ {
			// 测试间短暂休息
			time.Sleep(100 * time.Millisecond)

			// 测量选举延迟
			latency, success := measureElectionLatency(t, nodeCount)

			if !success {
				// 选举失败，记录并继续
				t.Logf("节点数 %d: 第 %d 次测试超时", nodeCount, i)
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

	t.Logf("测试完成，结果已保存到 election_latency_results2.csv")
}

// 测量单次选举延迟的函数
func measureElectionLatency(t *testing.T, numServers int) (time.Duration, bool) {
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

	// 记录选举开始时间
	startTime := time.Now()

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

	// 确保清理资源
	defer func() {
		for i := 0; i < numServers; i++ {
			if rafts[i] != nil {
				rafts[i].Kill()
			}
		}
	}()

	// 等待选出Leader
	timeout := 1 * time.Second
	checkInterval := 10 * time.Millisecond
	endTime := time.Now().Add(timeout)

	for time.Now().Before(endTime) {
		// 检查是否有节点成为Leader
		for i := 0; i < numServers; i++ {
			if rafts[i] == nil {
				continue
			}
			_, isLeader := rafts[i].GetState()
			if isLeader {
				return time.Since(startTime), true
			}
		}
		time.Sleep(checkInterval)
	}

	// 超时未选出Leader
	return timeout, false
}
