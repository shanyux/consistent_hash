/*
 * @Author: Yuxiang Shan
 * @Mail: Yuxiang.Shan@shopee.com
 * @Date: 2022-06-06 23:40:38
 * @FilePath: /consistent_hash/go_rendezvous_consistent_hash/rdv_test.go
 */
package rendezvous

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"testing"
)

func hashString(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func TestEmpty(t *testing.T) {
	r := NewRendezvous([]string{}, hashString)
	r.Lookup("hello")

}

func getServerNodes(nodenum int) []string {
	nodes := make([]string, 0, nodenum)
	for i := 0; i < nodenum; i++ {
		n := fmt.Sprintf("127.0.0.1:800%d", i)
		nodes = append(nodes, n)
	}
	return nodes

}
func Test_Rendezvous_ConsistentHash(t *testing.T) {
	nodeNumList := []int{100, 200, 300, 400, 500, 600, 700, 800}
	//测试10台服务器
	// var nodeNum uint
	//测试数据量100W
	testCount := 10000000

	for _, nodeNum := range nodeNumList {
		distributeMap := make(map[string]int64)
		nodes := getServerNodes(nodeNum)
		for _, s := range nodes {
			distributeMap[s] = 0
		}
		rdz := NewRendezvous(nodes, hashString)

		for i := 0; i < testCount; i++ {
			testName := "testName"
			node := rdz.Lookup(testName + strconv.Itoa(i))
			distributeMap[node] = distributeMap[node] + 1
		}

		var values []float64

		fmt.Printf("####测试%d个结点,%d条测试数据\n", nodeNum, testCount)
		for k, v := range distributeMap {
			values = append(values, float64(v))
			// fmt.Printf("服务器地址:%s 分布数据数:%d\n", k, distributeMap[k])
			_ = k
		}
		fmt.Printf("标准差:%f\n\n", getStandardDeviation(values))
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
