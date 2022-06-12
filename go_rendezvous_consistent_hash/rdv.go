package rendezvous

type Rendezvous struct {
	nodes         map[string]int
	nodeStr       []string
	nodeHashValue []uint64
	hash          Hasher
}

type Hasher func(s string) uint64

func New(nodes []string, hash Hasher) *Rendezvous {
	r := &Rendezvous{
		nodes:         make(map[string]int, len(nodes)),
		nodeStr:       make([]string, len(nodes)),
		nodeHashValue: make([]uint64, len(nodes)),
		hash:          hash,
	}

	for i, n := range nodes {
		r.nodes[n] = i
		r.nodeStr[i] = n
		r.nodeHashValue[i] = hash(n)
	}

	return r
}

// Lookup 查找 key 匹配的 node
func (r *Rendezvous) Lookup(k string) string {
	// 首先计算 hash(key)
	khash := r.hash(k)

	// 先计算 keyHash 和 nodeHash[0] 的 hash 作为初始值
	var midx int
	var mhash = xorshiftMult64(khash ^ r.nodeHashValue[0])

	// 遍历所有的 nodeHash，计算 hash(keyHash + nodeHash)
	// 寻找计算结果最大的 node 的 idx
	// 这里，已经预先算好的每一个 nodeHash，存储顺序和 nodes 列表一致
	for i, nodeHashValue := range r.nodeHashValue[1:] {
		if h := xorshiftMult64(khash ^ nodeHashValue); h > mhash {
			midx = i + 1
			mhash = h
		}
	}

	// 根据 idx 返回匹配的 node
	return r.nodeStr[midx]
}

func (r *Rendezvous) Add(node string) {
	r.nodes[node] = len(r.nodeStr)
	r.nodeStr = append(r.nodeStr, node)
	r.nodeHashValue = append(r.nodeHashValue, r.hash(node))
}

func (r *Rendezvous) Remove(node string) {
	// find index of node to remove
	nidx := r.nodes[node]

	// remove from the slices
	l := len(r.nodeStr)
	r.nodeStr[nidx] = r.nodeStr[l]
	r.nodeStr = r.nodeStr[:l]

	r.nodeHashValue[nidx] = r.nodeHashValue[l]
	r.nodeHashValue = r.nodeHashValue[:l]

	// update the map
	delete(r.nodes, node)
	moved := r.nodeStr[nidx]
	r.nodes[moved] = nidx
}

func xorshiftMult64(x uint64) uint64 {
	x ^= x >> 12 // a
	x ^= x << 25 // b
	x ^= x >> 27 // c
	return x * 2685821657736338717
}
