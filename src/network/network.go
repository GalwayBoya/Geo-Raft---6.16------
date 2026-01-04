package network

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"
)

// ClientEnd 是RPC客户端的接口，模拟labrpc.ClientEnd
type ClientEnd struct {
	addr    string      // 服务器地址
	client  *rpc.Client // RPC客户端
	mu      sync.Mutex  // 保护连接的互斥锁
	enabled bool        // 连接是否启用
}

// Call 发送RPC请求，与labrpc的Call方法签名保持一致
func (ce *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	ce.mu.Lock()
	if !ce.enabled {
		ce.mu.Unlock()
		return false
	}

	// 如果客户端未连接，尝试连接
	if ce.client == nil {
		client, err := rpc.Dial("tcp", ce.addr)
		if err != nil {
			fmt.Printf("连接到 %s 失败: %v\n", ce.addr, err)
			ce.mu.Unlock()
			return false
		}
		ce.client = client
	}
	ce.mu.Unlock()

	// 发送RPC请求
	err := ce.client.Call(svcMeth, args, reply)
	if err != nil {
		fmt.Printf("RPC调用失败 %s: %v\n", svcMeth, err)
		ce.mu.Lock()
		if ce.client != nil {
			ce.client.Close()
			ce.client = nil
		}
		ce.mu.Unlock()
		return false
	}

	return true
}

// Close 关闭连接
func (ce *ClientEnd) Close() {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	if ce.client != nil {
		ce.client.Close()
		ce.client = nil
	}
}

// Enable 启用或禁用连接
func (ce *ClientEnd) Enable(enabled bool) {
	ce.mu.Lock()
	defer ce.mu.Unlock()
	ce.enabled = enabled
	if !enabled && ce.client != nil {
		ce.client.Close()
		ce.client = nil
	}
}

// NetworkAdapter 负责创建多机环境下的网络连接
type NetworkAdapter struct {
	mu       sync.Mutex
	servers  map[string]*rpc.Server
	listener net.Listener
}

// MakeNetwork 创建一个新的网络适配器
func MakeNetwork() *NetworkAdapter {
	na := &NetworkAdapter{
		servers: make(map[string]*rpc.Server),
	}
	return na
}

// MakeEnd 创建一个ClientEnd，模拟labrpc的MakeEnd
func (na *NetworkAdapter) MakeEnd(addr string) *ClientEnd {
	ce := &ClientEnd{
		addr:    addr,
		enabled: true,
	}
	return ce
}

// AddServer 添加一个RPC服务器到网络，模拟labrpc的AddServer
func (na *NetworkAdapter) AddServer(addr string, server *rpc.Server) {
	na.mu.Lock()
	defer na.mu.Unlock()
	na.servers[addr] = server
}

// StartServer 启动RPC服务器
func (na *NetworkAdapter) StartServer(addr string) error {
	na.mu.Lock()
	server, exists := na.servers[addr]
	na.mu.Unlock()

	if !exists {
		return fmt.Errorf("服务器 %s 不存在", addr)
	}

	// 不需要在这里注册gob类型，它们应该已经在init()中注册过了

	// 监听TCP端口
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("解析地址失败 %s: %v", addr, err)
	}

	// 如果是0.0.0.0或localhost，使用原始地址
	if host == "0.0.0.0" || host == "localhost" {
		listener, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("监听端口失败 %s: %v", addr, err)
		}
		na.listener = listener
	} else {
		// 尝试解析IP地址
		ips, err := net.LookupIP(host)
		if err != nil || len(ips) == 0 {
			// 如果解析失败，尝试使用0.0.0.0代替
			parts := strings.Split(addr, ":")
			if len(parts) == 2 {
				newAddr := "0.0.0.0:" + parts[1]
				fmt.Printf("无法解析主机 %s，尝试使用 %s 代替\n", host, newAddr)
				listener, err := net.Listen("tcp", newAddr)
				if err != nil {
					return fmt.Errorf("监听端口失败 %s: %v", newAddr, err)
				}
				na.listener = listener
			} else {
				return fmt.Errorf("无效的地址格式 %s", addr)
			}
		} else {
			// 使用解析出的第一个IP地址
			listener, err := net.Listen("tcp", addr)
			if err != nil {
				return fmt.Errorf("监听端口失败 %s: %v", addr, err)
			}
			na.listener = listener
		}
	}

	// 处理连接
	go func() {
		for {
			conn, err := na.listener.Accept()
			if err != nil {
				// 如果监听器被关闭，就退出循环
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				fmt.Printf("接受连接失败: %v\n", err)
				continue
			}
			go server.ServeConn(conn)
		}
	}()

	fmt.Printf("RPC服务器已启动，监听地址: %s\n", na.listener.Addr())
	return nil
}

// StopServer 停止RPC服务器
func (na *NetworkAdapter) StopServer() {
	if na.listener != nil {
		na.listener.Close()
	}
}

// MakeServer 创建一个新的RPC服务器，模拟labrpc的MakeServer
func MakeServer() *rpc.Server {
	return rpc.NewServer()
}

// MakeService 将对象的方法注册为RPC服务，模拟labrpc的MakeService
func MakeService(rcvr interface{}) *ServiceRegistry {
	return &ServiceRegistry{rcvr: rcvr}
}

// ServiceRegistry 封装RPC服务注册
type ServiceRegistry struct {
	rcvr interface{}
}

// AddToServer 将服务添加到服务器
func (sr *ServiceRegistry) AddToServer(server *rpc.Server) {
	// 注册Raft对象及其所有导出方法
	err := server.Register(sr.rcvr)
	if err != nil {
		fmt.Printf("注册RPC服务失败: %v\n", err)
	}
}

// 初始化注册所需的gob类型
func init() {
	// 注册基本类型
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})

	// 注意：这里不需要重复注册Raft类型，因为它们已经在main.go中通过labgob注册了
	// 我们只需在启动服务器前确保已注册
}
