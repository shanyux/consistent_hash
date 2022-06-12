/*
 * @Author: Yuxiang Shan
 * @Mail: Yuxiang.Shan@shopee.com
 * @Date: 2022-06-06 23:40:38
 * @FilePath: /consistent_hash/all_test.go
 */
package main

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	jump "github.com/shanyux/consistent_hash/go_jump_consistent_hash"
	ketama "github.com/shanyux/consistent_hash/go_ketama_consistent_hash"
	rendezvous "github.com/shanyux/consistent_hash/go_rendezvous_consistent_hash"
)

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func getServerNodes(nodenum int) []string {
	nodes := make([]string, 0, nodenum)
	for i := 0; i < nodenum; i++ {
		n := fmt.Sprintf("127.0.0.1:800%d", i)
		nodes = append(nodes, n)
	}
	return nodes

}

func RendezvousConsistentHash(nodeNum, testCount int) {
	distributeMap := make(map[string]int64)
	nodes := getServerNodes(nodeNum)
	for _, s := range nodes {
		distributeMap[s] = 0
	}
	rdz := rendezvous.NewRendezvous(nodes, hashString)
	start := time.Now()

	for i := 0; i < testCount; i++ {
		testName := "testName"
		node := rdz.Lookup(testName + strconv.Itoa(i))
		distributeMap[node] = distributeMap[node] + 1
	}

	var values []float64

	fmt.Printf("$$$$RendezvousConsistentHash 测试%d个结点,%d条测试数据\n", nodeNum, testCount)
	for k, v := range distributeMap {
		values = append(values, float64(v))
		// fmt.Printf("服务器地址:%s 分布数据数:%d\n", k, distributeMap[k])
		_ = k
	}
	fmt.Printf("标准差:%f, run time:%f\n", getStandardDeviation(values), time.Since(start).Seconds())
}

func getKatamaNodes(nodenum, virtualnum uint) []*ketama.Node {
	nodes := make([]*ketama.Node, 0, nodenum)
	var i uint = 0
	for ; i < nodenum; i++ {
		n := ketama.NewNode(fmt.Sprintf("127.0.0.1:800%d", i), nil, virtualnum)
		nodes = append(nodes, n)
	}

	return nodes
}
func KatamaConsistentHash(nodeNum, virtualNode, testCount int) {

	nodes := getKatamaNodes(uint(nodeNum), uint(virtualNode))
	hashRing := ketama.NewRing(nodes)
	distributeMap := make(map[string]int64)
	for _, node := range nodes {
		distributeMap[node.NodeLable] = 0
	}
	start := time.Now()

	// consistentHash := &Consistent{}
	// distributeMap := make(map[string]int64)
	// for i := 1; i <= nodeNum; i++ {
	// 	serverName := "172.17.0." + strconv.Itoa(i)
	// 	consistentHash.Add(serverName, virtualNode)
	// 	distributeMap[serverName] = 0
	// }
	//测试100W个数据分布
	for i := 0; i < testCount; i++ {
		testName := "testName"
		node := hashRing.Get(testName + strconv.Itoa(i))
		distributeMap[node.NodeLable] = distributeMap[node.NodeLable] + 1
	}

	var keys []string
	var values []float64
	for k, v := range distributeMap {
		keys = append(keys, k)
		values = append(values, float64(v))
	}
	sort.Strings(keys)
	fmt.Printf("KatamaConsistentHash 测试%d个结点,一个结点有%d个虚拟结点,%d条测试数据\n", nodeNum, virtualNode, testCount)
	// for _, k := range keys {
	// 	fmt.Printf("服务器地址:%s 分布数据数:%d\n", k, distributeMap[k])
	// }
	fmt.Printf("####标准差:%f, run time:%f\n", getStandardDeviation(values), time.Since(start).Seconds())

}

//获取标准差
func getStandardDeviation(list []float64) float64 {
	var total float64
	for _, item := range list {
		total += item
	}
	//平均值
	avg := total / float64(len(list))

	var dTotal float64
	for _, value := range list {
		dValue := value - avg
		dTotal += dValue * dValue
	}

	return math.Sqrt(dTotal / avg)
}

func JumpConsistentHash(nodeNum, testCount int) {
	distributeMap := make(map[int32]int64)
	var i int32
	for i = 0; i < int32(nodeNum); i++ {
		distributeMap[i] = 0
	}

	//测试100W个数据分布
	start := time.Now()
	rand.Seed(time.Now().Unix())
	for i := 0; i < testCount; i++ {
		key := rand.Int63n(int64(testCount))
		b := jump.JumpHash(uint64(key), int32(nodeNum))
		distributeMap[b] = distributeMap[b] + 1
	}

	var values []float64
	for _, v := range distributeMap {
		values = append(values, float64(v))
	}
	// sort.Strings(keys)
	fmt.Printf("&&&&JumpConsistentHash 测试%d个结点,%d条测试数据\n", nodeNum, testCount)
	// for i, k := range distributeMap {
	// 	fmt.Printf("服务器:%d 分布数据数:%d\n", i, k)
	// }
	fmt.Printf("标准差:%f, run time:%f\n", getStandardDeviation(values), time.Since(start).Seconds())
}

func Test_All_ConsistentHash(t *testing.T) {
	nodeNumList := []int{50, 100, 200, 300, 400, 500, 600, 700, 800, 900}
	//测试10台服务器
	// var nodeNum uint
	//测试数据量100W
	testCount := 1000000
	for _, nodeNum := range nodeNumList {
		VirtualNodeNumList := []int{1, 10, 50, 100}
		for _, v := range VirtualNodeNumList {
			KatamaConsistentHash(nodeNum, v, testCount)
		}
		RendezvousConsistentHash(nodeNum, testCount)
		JumpConsistentHash(nodeNum, testCount)
		fmt.Println()
	}
}
