# Geo-Raft：支持地理分布与可视化监控的Raft共识算法实现

Geo-Raft是一个基于Golang的高级Raft共识算法实现，专为地理分布式的多数据中心环境设计。本项目不仅实现了核心的Raft算法，还引入了基于信用值的选举机制、RSA命令签名验证，并提供了一个功能强大的Web可视化监控界面，支持实时状态查看和故障注入。

## 核心特性

### 1. 地理分布式优化 (Geo-Distributed)
- **基于信用的选举机制 (Credit-Based Election)**：
  - 节点根据参与度、投票一致性和网络延迟维护一个"信用值"。
  - 信用值高的节点拥有更短的选举超时时间（分为FirstTier, SecondTier, ThirdTier三档），从而更有可能成为Leader。
  - 优化了跨地域部署时的Leader选举效率，倾向于选择网络质量好、稳定性高的节点。

### 2. 安全性增强
- **命令签名验证 (Command Signing)**：
  - 客户端命令使用RSA私钥签名。
  - Raft节点在处理命令前使用公钥验证签名，防止恶意命令注入。

### 3. 可视化监控与控制 (Web Interface)
- **实时状态监控**：通过WebSocket实时推送集群状态，包括Term、Role、CommitIndex、网络延迟等。
- **故障注入 (Fault Injection)**：
  - **强制Leader下线**：通过Web界面强制当前Leader退位。
  - **网络隔离**：模拟网络分区，隔离特定节点。
- **交互式操作**：直接在Web界面发送测试命令。

### 4. 灵活的部署模式
- **单机模拟模式**：在单机上模拟多节点集群，配合Web界面进行开发和调试。
- **多机部署模式**：支持在真实网络环境下的多台机器上部署。

## 项目结构

```
Geo-Raft/
├── src/
│   ├── main.go               # 主程序入口
│   ├── go.mod                # Go模块定义
│   ├── raft/                 # Raft核心实现
│   │   ├── raft.go           # 核心逻辑
│   │   ├── credit_ticker.go  # 信用值更新机制
│   │   ├── signature.go      # RSA签名验证
│   │   ├── raft_election.go  # 选举逻辑
│   │   └── ...
│   ├── web/                  # Web服务器与前端
│   │   ├── server/           # Gin Web服务器
│   │   ├── templates/        # HTML模板
│   │   └── static/           # CSS/JS静态资源
│   ├── tools/                # Python分析与绘图工具
│   │   ├── plot_election_latency.py
│   │   └── ...
│   ├── network/              # 多机网络通信层
│   └── labrpc/               # 单机模拟RPC层
├── Makefile                  # 构建工具
└── README.md                 # 项目说明文档
```

## 快速开始

### 前提条件
- Go 1.13+
- Python 3.x (用于运行分析脚本)

### 1. 单机模拟模式 (推荐用于开发与演示)

此模式会在本地启动8个Raft节点，并开启Web服务器。

```bash
cd src
go run main.go -single
```

启动后，访问浏览器：`http://localhost:8080`

**Web界面功能：**
- 查看所有节点的状态（Leader/Follower/Candidate）。
- 点击 "Inject Fault" 强制Leader下线。
- 点击 "Toggle Isolation" 随机隔离一个Follower节点。
- 点击 "Send Command" 发送带有签名的 "hello world" 命令。

### 2. 多机部署模式（真实网络环境）

此模式使用 TCP/IP 网络通信，既支持跨机器部署，也支持在单机上通过不同端口模拟多节点集群。

**编译：**
```bash
cd src
go build -o raft-node main.go
```

**运行示例：单机多端口模拟 (3节点集群)**

在同一台机器上打开 3 个终端窗口，分别运行以下命令：

**终端 1 (节点 0):**
```bash
./raft-node -single=false -id=0 -addrs="localhost:8000,localhost:8001,localhost:8002" -listen="localhost:8000"
```

**终端 2 (节点 1):**
```bash
./raft-node -single=false -id=1 -addrs="localhost:8000,localhost:8001,localhost:8002" -listen="localhost:8001"
```

**终端 3 (节点 2):**
```bash
./raft-node -single=false -id=2 -addrs="localhost:8000,localhost:8001,localhost:8002" -listen="localhost:8002"
```

**参数说明：**
- `-single=false`: 关闭单机模拟模式，启用真实网络通信。
- `-id`: 当前节点ID (0, 1, 2...)，必须与 `-addrs` 列表中的顺序对应。
- `-addrs`: 集群所有节点的地址列表，逗号分隔。所有节点必须使用相同的列表。
- `-listen`: 本地监听地址。

## 性能测试与分析

`src/tools/` 目录下提供了Python脚本，用于分析实验数据并生成图表。实验数据通常存储在 `src/raft/data/` 目录下。

- `plot_election_latency.py`: 绘制选举延迟图表。
- `plot_consensus_latency.py`: 绘制共识延迟图表。
- `dslogs.py`: 日志分析工具。

**运行绘图脚本：**
```bash
cd src/tools
python plot_election_latency.py
```

## 协议
MIT License
