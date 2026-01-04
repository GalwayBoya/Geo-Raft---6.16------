package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"raft"
)

func main() {
	// 解析命令行参数
	serverAddr := flag.String("addr", "localhost:1234", "服务器地址")
	nodeID := flag.Int("id", 0, "节点ID")
	flag.Parse()

	// 创建Raft实例
	rf := raft.Make([]raft.PeerInfo{
		{ID: 0, Addr: "localhost:1234"},
		{ID: 1, Addr: "localhost:1235"},
		{ID: 2, Addr: "localhost:1236"},
	}, *nodeID, nil, make(chan raft.ApplyMsg))

	// 注册RPC服务
	rpc.Register(rf)
	rpc.HandleHTTP()

	// 启动服务器
	l, err := net.Listen("tcp", *serverAddr)
	if err != nil {
		log.Fatal("监听失败:", err)
	}
	log.Printf("服务器启动在 %s", *serverAddr)
	http.Serve(l, nil)
}
