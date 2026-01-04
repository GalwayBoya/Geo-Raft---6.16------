package main

import (
	"consensus/labrpc"
	"consensus/raft"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

const (
	nodeCount    = 4    // 集群节点数量
	baseRPCPort  = 5001 // RPC基础端口号
	baseHTTPPort = 8080 // HTTP监控端口（可选）
	raftTimeout  = 2000 // Raft选举超时基准（毫秒）
)

func main1() {
	// 解析节点ID参数
	nodeID := flag.Int("id", 0, "Node ID (0-3)")
	flag.Parse()

	if *nodeID < 0 || *nodeID >= nodeCount {
		log.Fatal("Invalid node ID, must be 0-3")
	}

	// 初始化网络和节点
	network := labrpc.MakeNetwork()
	nodes := make([]*raft.Raft, nodeCount)
	peers := make([]*labrpc.ClientEnd, nodeCount)

	// 创建所有节点的RPC端点
	for i := 0; i < nodeCount; i++ {
		peers[i] = network.MakeEnd(fmt.Sprintf("node-%d", i))
	}

	// 初始化Raft节点
	wg := sync.WaitGroup{}
	for i := 0; i < nodeCount; i++ {
		wg.Add(1)
		go func(nodeId int) {
			defer wg.Done()

			// 节点配置
			persister := raft.MakePersister()
			applyCh := make(chan raft.ApplyMsg)

			// 创建Raft实例
			rf := raft.Make(
				peers,     // 所有节点的ClientEnd
				nodeId,    // 当前节点ID
				persister, // 持久化存储
				applyCh,   // 应用通道
			)

			// 启动RPC服务
			svc := labrpc.MakeService(rf)
			srv := labrpc.MakeServer()
			srv.AddService(svc)
			network.AddServer(
				fmt.Sprintf("node-%d", nodeId),
				srv,
			)

			nodes[nodeId] = rf
			log.Printf("Node %d started at :%d", nodeId, baseRPCPort+nodeId)
		}(i)
	}

	// 等待所有节点初始化完成
	wg.Wait()

	// 连接所有节点形成集群
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				network.Connect(
					fmt.Sprintf("node-%d", i),
					fmt.Sprintf("node-%d", j),
				)
				network.Enable(
					fmt.Sprintf("node-%d", i),
					true,
				)
			}
		}
	}

	// 启动监控协程（可选）
	// go monitorCluster(nodes)

	// 处理终止信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 清理资源
	log.Println("\nShutting down cluster...")
	network.Cleanup()
	for _, rf := range nodes {
		rf.Kill()
	}
}

// 监控集群状态（每2秒打印一次）
// func monitorCluster(nodes []*raft.Raft) {
// 	for {
// 		time.Sleep(2 * time.Second)
// 		fmt.Println("\n=== Cluster Status ===")
// 		for i, rf := range nodes {
// 			term, isLeader := rf.GetState()
// 			status := "Follower"
// 			if isLeader {
// 				status = "Leader"
// 			}
// 			fmt.Printf("Node %d | Term: %d | Role: %-7s | CommitIdx: %d\n",
// 				i, term, status, rf.GetCommitIndex())
// 		}
// 		fmt.Println("======================")
// 	}
// }
