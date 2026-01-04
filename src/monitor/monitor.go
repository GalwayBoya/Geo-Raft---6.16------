package monitor

import (
	"consensus/raft"
	"fmt"
	"time"
)

// 监控集群状态（每2秒打印一次）
func MonitorCluster(nodes []*raft.Raft) {
	for {
		time.Sleep(2 * time.Second)
		fmt.Println("\n=== Cluster Status ===")
		for i, rf := range nodes {
			term, isLeader := rf.GetState()
			status := "Follower"
			if isLeader {
				status = "Leader"
			}
			fmt.Printf("Node %d | Term: %d | Role: %-7s | CommitIdx: %d\n",
				i, term, status, rf.GetCommitIndex())
		}
		fmt.Println("======================")
	}
}
