// Copyright 2016 Chao Wang <hit9@icloud.com>

// Package ketama implements a consistent hashing ring.
package ketama

import (
	"crypto/md5"
	"fmt"
	"sort"
)

// Node is the hashing ring node.
type Node struct {
	NodeLable string
	data      interface{}
	weight    uint
	hash      uint32
}

// NewNode creates a new Node.
func NewNode(NodeLable string, data interface{}, weight uint) *Node {
	return &Node{NodeLable: NodeLable, data: data, weight: weight}
}

// Key returns the Node NodeLable.
func (n *Node) Key() string {
	return n.NodeLable
}

// Data returns the Node data.
func (n *Node) Data() interface{} {
	return n.data
}

// Weight returns the Node weight.
func (n *Node) Weight() uint {
	return n.weight
}

// ByHash implements sort.Interface.
type ByHash []*Node

func (s ByHash) Len() int           { return len(s) }
func (s ByHash) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByHash) Less(i, j int) bool { return s[i].hash < s[j].hash }

// Ring is the ketama hashing ring.
type Ring struct {
	virtualNodes []*Node
}

// alignHash returns hash value with aligment.
func alignHash(NodeLable string, align int) uint32 {
	b := md5.Sum([]byte(NodeLable)) //16字节,4个字节一组
	return ((uint32(b[3+align*4]&0xff) << 24) |
		(uint32(b[2+align*4]&0xff) << 16) |
		(uint32(b[1+align*4]&0xff) << 8) |
		(uint32(b[0+align*4] & 0xff)))
}

// NewRing creates a new Ring.
func NewRing(realsNodes []*Node) *Ring {
	// Create ring and init its virtualNodes.
	hashRing := &Ring{} //哈希环
	length := 0
	for i := 0; i < len(realsNodes); i++ { //物理节点
		length += int(realsNodes[i].weight) * 4 * 40
	}
	hashRing.virtualNodes = make([]*Node, length) //虚拟节点
	// Init each ring node.
	k := 0
	for i := 0; i < len(realsNodes); i++ {
		node := realsNodes[i]
		for j := 0; j < int(node.weight)*40; j++ {
			NodeLable := fmt.Sprintf("%s-%d", node.NodeLable, j)
			for n := 0; n < 4; n++ {
				hashRing.virtualNodes[k] = &Node{}
				hashRing.virtualNodes[k].NodeLable = node.NodeLable
				hashRing.virtualNodes[k].weight = node.weight
				hashRing.virtualNodes[k].data = node.data
				hashRing.virtualNodes[k].hash = alignHash(NodeLable, n)
				k++
			}
		}
	}
	sort.Sort(ByHash(hashRing.virtualNodes))
	return hashRing
}

// Get node by NodeLable from ring.
// Returns nil if the ring is empty.
func (r *Ring) Get(NodeLable string) *Node {
	if len(r.virtualNodes) == 0 {
		return nil
	}
	if len(r.virtualNodes) == 1 {
		return r.virtualNodes[0]
	}
	left := 0
	right := len(r.virtualNodes)
	hash := alignHash(NodeLable, 0)
	for {
		mid := (left + right) / 2
		if mid == len(r.virtualNodes) {
			return r.virtualNodes[0]
		}
		var p uint32
		m := r.virtualNodes[mid].hash
		if mid == 0 {
			p = 0
		} else {
			p = r.virtualNodes[mid-1].hash
		}
		if hash < m && hash > p { //查找到
			return r.virtualNodes[mid]
		}
		if m < hash {
			left = mid + 1
		} else {
			right = mid - 1
		}
		if left > right { //哈希环跳圈
			return r.virtualNodes[0]
		}
	}
}
