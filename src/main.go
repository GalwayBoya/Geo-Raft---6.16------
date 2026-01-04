package main

import (
	"consensus/labgob"
	"consensus/labrpc"
	"consensus/network"
	"consensus/raft"
	"consensus/web/server"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// 在 init() 中注册需要用到的结构，以便 gob 序列化/反序列化。
func init() {
	labgob.Register(raft.ApplyMsg{})

	// 注册Raft RPC参数类型
	labgob.Register(raft.RequestVoteArgs{})
	labgob.Register(raft.RequestVoteReply{})
	labgob.Register(raft.AppendEntriesArgs{})
	labgob.Register(raft.AppendEntriesReply{})

	// 如果有其他需要在网络中传输的结构，也要在这里注册
}

var (
	peerAddrs  = flag.String("addrs", "localhost:8000,localhost:8001,localhost:8002,localhost:8003,localhost:8004", "所有节点的地址，以逗号分隔")
	listenAddr = flag.String("listen", "", "本节点监听地址(如为空则使用addrs中对应id的地址)")
	nodeId     = flag.Int("id", 0, "当前节点ID")
	single     = flag.Bool("single", true, "是否单机模拟运行")
)

func main() {
	flag.Parse()

	addresses := strings.Split(*peerAddrs, ",")
	if *nodeId >= len(addresses) {
		log.Fatalf("节点ID %d 超出范围 (总节点数: %d)", *nodeId, len(addresses))
	}

	if *single {
		// 单机模拟模式（保持原始行为）
		singleNodeSimulation()
	} else {
		// 多机部署模式
		multiNodeDeployment(addresses, *nodeId)
	}
}

// 多机部署模式
func multiNodeDeployment(addresses []string, nodeId int) {
	// 确定监听地址
	listenAddress := *listenAddr
	if listenAddress == "" {
		listenAddress = addresses[nodeId]
	}

	fmt.Printf("启动节点 %d，地址: %s\n", nodeId, addresses[nodeId])
	if listenAddress != addresses[nodeId] {
		fmt.Printf("监听地址: %s (与节点地址不同)\n", listenAddress)
	}

	// 创建网络适配器
	net := network.MakeNetwork()

	// 创建ClientEnd列表（连接到其他节点）
	peers := make([]*network.ClientEnd, len(addresses))
	for i, addr := range addresses {
		peers[i] = net.MakeEnd(addr)
	}

	// 创建和配置RPC服务器
	server := network.MakeServer()

	// 创建Raft节点
	persister := raft.MakePersister()
	applyCh := make(chan raft.ApplyMsg, 100)
	rf := raft.Make(peers, nodeId, persister, applyCh)

	// 注册Raft服务
	svc := network.MakeService(rf)
	svc.AddToServer(server)

	// 将服务器添加到网络
	net.AddServer(listenAddress, server)

	// 启动RPC服务器
	err := net.StartServer(listenAddress)
	if err != nil {
		log.Fatalf("启动RPC服务器失败: %v", err)
	}

	// 处理应用消息
	go func() {
		for msg := range applyCh {
			if msg.CommandValid {
				fmt.Printf("[节点 %d] 应用日志: 命令=%v, 索引=%d\n",
					nodeId, msg.Command, msg.CommandIndex)
			}
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 清理资源
	fmt.Println("正在关闭节点...")
	rf.Kill()
	net.StopServer()
}

// 单机模拟模式（保持原有代码）
func singleNodeSimulation() {
	// 创建一个模拟网络
	net := labrpc.MakeNetwork()

	const numServers = 8
	// 每个节点都有一组 ClientEnd，大小也是 4，代表与其他节点通信的"端"
	ends := make([][]*labrpc.ClientEnd, numServers)
	for i := 0; i < numServers; i++ {
		ends[i] = make([]*labrpc.ClientEnd, numServers)
	}

	// 为了让所有节点都能互联，我们给每个 (i,j) 生成一个唯一的 endName
	// 并将它连接到网络上
	for i := 0; i < numServers; i++ {
		for j := 0; j < numServers; j++ {
			endName := fmt.Sprintf("end-%d-%d", i, j)
			ends[i][j] = net.MakeEnd(endName)
			// 这里把 endName 连接到第 j 个服务器
			net.Connect(endName, j)
			// 先都启用
			net.Enable(endName, true)
		}
	}

	// 每个节点都需要一个持久化器 Persister，用来存储 Raft 状态
	persisters := make([]*raft.Persister, numServers)
	for i := 0; i < numServers; i++ {
		persisters[i] = raft.MakePersister()
	}

	// 为每个节点创建一个应用通道 applyCh，用于接收 Raft 日志提交的消息
	applyChs := make([]chan raft.ApplyMsg, numServers)
	for i := 0; i < numServers; i++ {
		applyChs[i] = make(chan raft.ApplyMsg)
	}

	// 创建并启动 24 个 Raft 节点
	rafts := make([]*raft.Raft, numServers)
	for i := 0; i < numServers; i++ {
		// 为每个节点创建一个 Raft 实例
		rf := raft.Make(ends[i], i, persisters[i], applyChs[i])
		rafts[i] = rf

		// 为当前节点创建一个 RPC Server，并将其加入到网络中
		srv := labrpc.MakeServer()
		// 使用新增的AddServiceWithName方法，显式指定服务名称为"Raft"
		srv.AddServiceWithName(rf, "Raft")
		net.AddServer(i, srv)
	}

	// 移除旧的监控协程，用新的Web界面代替
	// go monitor.MonitorCluster(rafts)

	// 启动一个简单的协程，消费各节点的 applyCh，打印日志提交结果
	for i := 0; i < numServers; i++ {
		go func(id int) {
			for msg := range applyChs[id] {
				if msg.CommandValid {
					// 可以在这里保留或移除日志，Web界面会显示提交索引
					// fmt.Printf("[Node %d] applyCh received: Command=%v, Index=%d\n",
					// 	id, msg.Command, msg.CommandIndex)
				}
			}
		}(i)
	}

	//************************************************************
	//WEB界面部分
	//************************************************************

	// 启动 Web 服务器
	server.RunServer(rafts, net)

	// 定期广播状态更新
	go func() {
		for {
			// 这里可以调整广播频率
			time.Sleep(250 * time.Millisecond)
			server.BroadcastStatus()
		}
	}()

	// 主线程阻塞，等待用户中断
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// 清理资源
	fmt.Println("正在关闭节点...")
	for i := 0; i < numServers; i++ {
		rafts[i].Kill()
	}
}
