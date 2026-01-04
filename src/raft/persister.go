package raft

import "sync"

// Persister结构体，用于持久化Raft的状态和快照
type Persister struct {
	mu        sync.Mutex
	raftstate []byte //保存Raft的状态数据currentTerm、votedFor、log
	snapshot  []byte //保存快照数据
}

// MakePersister函数，用于创建一个Persister对象
func MakePersister() *Persister {
	return &Persister{}
}

// clone函数，用于复制一个byte数组
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

// Copy函数，用于复制一个Persister对象
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

// 返回一个Raft状态的拷贝（字节数组Raftstate的复制）
func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

// 返回Raftstate的大小
func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
// 将输入参数raftstate和snapshot保存到Persister中
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

// 读取快照数据（返回字节数组Snapshot的复制）
func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

// 返回快照数据大小
func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
