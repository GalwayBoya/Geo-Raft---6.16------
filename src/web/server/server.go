package server

import (
	"consensus/labrpc"
	"consensus/raft"
	"consensus/web/events"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type NodeStatus struct {
	ID          int           `json:"id"`
	Term        int           `json:"term"`
	Role        string        `json:"role"`
	CommitIndex int           `json:"commitIndex"`
	Latencies   map[int]int64 `json:"latencies,omitempty"`
	IsIsolated  bool          `json:"isIsolated"`
}

type ClusterStatus struct {
	Nodes          []NodeStatus   `json:"nodes"`
	Events         []events.Event `json:"events"`
	IsolatedNodeId int            `json:"isolatedNodeId"`
}

var (
	clients        = make(map[*websocket.Conn]bool)
	clientsMu      sync.Mutex
	raftNodes      []*raft.Raft
	network        *labrpc.Network
	isolatedNodeId = -1 // -1 means no node is isolated
)

func getClusterFullStatus() ClusterStatus {
	nodeStatuses := make([]NodeStatus, len(raftNodes))
	for i, rf := range raftNodes {
		term, _ := rf.GetState()
		role := rf.GetRole()

		nodeStatuses[i] = NodeStatus{
			ID:          i,
			Term:        term,
			Role:        string(role),
			CommitIndex: rf.GetCommitIndex(),
			Latencies:   rf.GetPeerLatencies(),
			IsIsolated:  i == isolatedNodeId,
		}
	}

	return ClusterStatus{
		Nodes:          nodeStatuses,
		Events:         events.GetAll(),
		IsolatedNodeId: isolatedNodeId,
	}
}

func BroadcastStatus() {
	status := getClusterFullStatus()
	data, err := json.Marshal(status)
	if err != nil {
		log.Println("Error marshalling status:", err)
		return
	}

	clientsMu.Lock()
	defer clientsMu.Unlock()
	for client := range clients {
		err := client.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.Printf("Error writing to client: %v", err)
			client.Close()
			delete(clients, client)
		}
	}
}

func handleConnections(c *gin.Context) {
	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println("Failed to upgrade connection:", err)
		return
	}
	defer ws.Close()

	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	log.Println("Client connected")

	// Send initial status
	status := getClusterFullStatus()
	data, _ := json.Marshal(status)
	ws.WriteMessage(websocket.TextMessage, data)

	for {
		// Keep connection alive
		_, _, err := ws.ReadMessage()
		if err != nil {
			log.Printf("Client disconnected: %v", err)
			clientsMu.Lock()
			delete(clients, ws)
			clientsMu.Unlock()
			break
		}
	}
}

func RunServer(nodes []*raft.Raft, net *labrpc.Network) {
	raftNodes = nodes
	network = net
	rand.Seed(time.Now().UnixNano())

	router := gin.Default()

	router.Static("/static", "web/static")

	router.GET("/", func(c *gin.Context) {
		c.File("web/templates/index.html")
	})

	router.GET("/ws", func(c *gin.Context) {
		handleConnections(c)
	})

	router.POST("/inject-fault", func(c *gin.Context) {
		var leaderNode *raft.Raft
		for _, node := range raftNodes {
			if _, isLeader := node.GetState(); isLeader {
				leaderNode = node
				break
			}
		}

		if leaderNode != nil {
			log.Println("Injecting fault: Forcing leader to step down.")
			leaderNode.StepDown()
			c.JSON(http.StatusOK, gin.H{"message": "Fault injected successfully. Leader is stepping down."})
		} else {
			c.JSON(http.StatusNotFound, gin.H{"message": "No leader found to inject fault."})
		}
	})

	router.POST("/send-command", func(c *gin.Context) {
		var leaderNode *raft.Raft
		for _, node := range raftNodes {
			if _, isLeader := node.GetState(); isLeader {
				leaderNode = node
				break
			}
		}

		if leaderNode != nil {
			command := "hello world"
			signedcommand, _ := raft.SignCommand(command)
			index, term, isLeader := leaderNode.StartSigned(command, signedcommand)
			if !isLeader {
				c.JSON(http.StatusConflict, gin.H{"message": "Failed to send command, node is no longer leader."})
				return
			}
			log.Printf("Command '%s' sent to leader. Index: %d, Term: %d", command, index, term)
			c.JSON(http.StatusOK, gin.H{
				"message": "Command sent to leader successfully.",
				"index":   index,
				"term":    term,
			})
		} else {
			c.JSON(http.StatusNotFound, gin.H{"message": "No leader found to send command to."})
		}
	})

	router.POST("/toggle-isolation", func(c *gin.Context) {
		if network == nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"message": "Network control is not available in this mode."})
			return
		}

		if isolatedNodeId != -1 {
			// Heal the currently isolated node
			network.Heal(isolatedNodeId)
			events.Log("partition", "Node %d reconnected to the network.", isolatedNodeId)
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Node %d reconnected.", isolatedNodeId)})
			isolatedNodeId = -1
		} else {
			// Isolate a new random node that is a follower
			var followers []int
			for _, node := range raftNodes {
				if _, isLeader := node.GetState(); !isLeader {
					followers = append(followers, node.GetID())
				}
			}

			if len(followers) == 0 {
				c.JSON(http.StatusConflict, gin.H{"message": "No follower nodes available to isolate."})
				return
			}
			nodeToIsolate := followers[rand.Intn(len(followers))]

			network.Isolate(nodeToIsolate)
			isolatedNodeId = nodeToIsolate
			events.Log("partition", "Node %d has been isolated from the network.", isolatedNodeId)
			c.JSON(http.StatusOK, gin.H{"message": fmt.Sprintf("Node %d isolated.", isolatedNodeId)})
		}
	})

	go func() {
		if err := router.Run(":8080"); err != nil {
			log.Fatalf("Failed to run server: %v", err)
		}
	}()
}
