// Copyright 2016 Chao Wang <hit9@icloud.com>

package ketama

import (
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"testing"
)

// Must asserts the given value is True for testing.
func Must(t *testing.T, v bool) {
	if !v {
		_, fileName, line, _ := runtime.Caller(1)
		t.Errorf("\n unexcepted: %s:%d", fileName, line)
	}
}

const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

// RandString returns a random string with the fixed length.
func RandString(length int) string {
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		j := rand.Intn(len(letters))
		b[i] = letters[j]
	}
	return string(b)
}

func TestBalance(t *testing.T) {
	nodes := []*Node{
		NewNode("127.0.0.1:8000", nil, 1),
		NewNode("127.0.0.1:8001", nil, 1),
		NewNode("127.0.0.1:8002", nil, 1),
		NewNode("127.0.0.1:8003", nil, 1),
		NewNode("127.0.0.1:8004", nil, 1),
		NewNode("127.0.0.1:8005", nil, 1),
		NewNode("127.0.0.1:8006", nil, 1),
		NewNode("127.0.0.1:8007", nil, 1),
		NewNode("127.0.0.1:8008", nil, 1),
		NewNode("127.0.0.1:8009", nil, 1),
		NewNode("127.0.0.1:8010", nil, 1),
		NewNode("127.0.0.1:8011", nil, 1),
		NewNode("127.0.0.1:8012", nil, 1),
	}
	ring := NewRing(nodes)
	Must(t, len(ring.virtualNodes) == len(nodes)*160)
	N := 4096 * len(nodes)
	m := make(map[string]int, 0)
	for i := 0; i < N; i++ {
		key := RandString(128)
		n := ring.Get(key)
		m[n.Key()]++
	}
	for _, v := range m {
		// rate 0.8 ~ 1.2
		Must(t, float64(v) > float64(N/len(nodes))*0.8)
		Must(t, float64(v) < float64(N/len(nodes))*1.2)
	}
}

func TestConsistence(t *testing.T) {
	nodes := []*Node{
		NewNode("192.168.0.1:9527", nil, 1),
		NewNode("192.168.0.2:9527", nil, 1),
		NewNode("192.168.0.3:9527", nil, 2),
		NewNode("192.168.0.4:9527", nil, 2),
		NewNode("192.168.0.5:9527", nil, 4),
	}
	ring := NewRing(nodes)
	Must(t, len(ring.virtualNodes) == (1+1+2+2+4)*160)
	for i := 0; i < 1024; i++ {
		key := RandString(128)
		n1 := ring.Get(key)
		n2 := ring.Get(key)
		Must(t, n1.Key() == n2.Key())
	}
}

func getServerNodes(nodenum, virtualnum uint) []*Node {
	nodes := make([]*Node, 0, nodenum)
	var i uint = 0
	for ; i < nodenum; i++ {
		n := NewNode(fmt.Sprintf("127.0.0.1:800%d", i), nil, virtualnum)
		nodes = append(nodes, n)
	}

	return nodes
}
func Test_Katama_ConsistentHash(t *testing.T) {
	virtualNodeList := []uint{1, 50, 100, 150, 200, 300, 400, 500}
	//测试10台服务器
	var nodeNum uint = 10
	//测试数据量100W
	testCount := 1000000
	for nodeNum <= 30 {
		nodeNum += 10
		for _, virtualNode := range virtualNodeList {
			nodes := getServerNodes(nodeNum, virtualNode)
			hashRing := NewRing(nodes)
			distributeMap := make(map[string]int64)
			for _, node := range nodes {
				distributeMap[node.NodeLable] = 0
			}
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
			fmt.Printf("####测试%d个结点,一个结点有%d个虚拟结点,%d条测试数据\n", nodeNum, virtualNode, testCount)
			// for _, k := range keys {
			// 	fmt.Printf("服务器地址:%s 分布数据数:%d\n", k, distributeMap[k])
			// }
			fmt.Printf("标准差:%f\n\n", getStandardDeviation(values))
		}
	}
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
