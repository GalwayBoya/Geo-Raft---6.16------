package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
)

// 序列化函数
func GetBytes(key interface{}) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// 使用 SHA256 哈希函数对数据进行哈希处理
func SHA256(elem interface{}) (string, error) {
	buf := GetBytes(elem)

	sum := sha256.Sum256(buf)
	ret := fmt.Sprintf("%x", sum)
	return ret, nil
}
