package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"raft"
)

type Command struct {
	Operation string
	Key       string
	Value     string
}

func main() {
	// 解析命令行参数
	serverAddr := flag.String("addr", "localhost:1234", "Raft server address")
	flag.Parse()

	// 连接到Raft服务器
	client, err := rpc.Dial("tcp", *serverAddr)
	if err != nil {
		log.Fatal("连接服务器失败:", err)
	}
	defer client.Close()

	// 创建命令
	cmd := Command{
		Operation: "SET",
		Key:       "test_key",
		Value:     "test_value",
	}

	// 对命令进行签名
	signature, err := raft.SignCommand(cmd)
	if err != nil {
		log.Fatal("签名失败:", err)
	}

	// 调用StartSigned发送命令
	var reply raft.StartSignedReply
	err = client.Call("Raft.StartSigned", raft.StartSignedArgs{
		Command:   cmd,
		Signature: signature,
	}, &reply)

	if err != nil {
		log.Fatal("RPC调用失败:", err)
	}

	if !reply.IsLeader {
		log.Fatal("当前节点不是leader")
	}

	fmt.Printf("命令已提交，日志索引: %d, 任期: %d\n", reply.Index, reply.Term)
}
