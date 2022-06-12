package jump

import (
	"fmt"
	"hash"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var jumpTestVectors = []struct {
	key      uint64
	buckets  int32
	expected int32
}{
	{1, 1, 0},
	{42, 57, 43},
	{0xDEAD10CC, 1, 0},
	{0xDEAD10CC, 666, 361},
	{256, 1024, 520},
	// Test negative values
	{0, -10, 0},
	{0xDEAD10CC, -666, 0},
}

func TestJumpHash(t *testing.T) {
	for _, v := range jumpTestVectors {
		h := JumpHash(v.key, v.buckets)
		if h != v.expected {
			t.Errorf("expected bucket for key=%d to be %d, got %d",
				v.key, v.expected, h)
		}
	}
}

var jumpStringTestVectors = []struct {
	key      string
	buckets  int32
	hasher   func() hash.Hash64
	expected int32
}{
	{"localhost", 10, NewCRC32, 9},
	{"ёлка", 10, NewCRC64, 6},
	{"ветер", 10, NewFNV1, 3},
	{"中国", 10, NewFNV1a, 5},
	{"日本", 10, NewCRC64, 6},
}

func TestJumpHashString(t *testing.T) {
	for _, v := range jumpStringTestVectors {
		h := HashString(v.key, v.buckets, v.hasher())
		if h != v.expected {
			t.Errorf("expected bucket for key=%s to be %d, got %d",
				strconv.Quote(v.key), v.expected, h)
		}
	}
}

func TestHasher(t *testing.T) {
	for _, v := range jumpStringTestVectors {
		hasher := New(int(v.buckets), v.hasher())
		h := hasher.Hash(v.key)
		if int32(h) != v.expected {
			t.Errorf("expected bucket for key=%s to be %d, got %d",
				strconv.Quote(v.key), v.expected, h)
		}
	}
}

func ExampleHash() {
	fmt.Print(JumpHash(256, 1024))
	// Output: 520
}

func ExampleHashString() {
	fmt.Print(HashString("127.0.0.1", 8, NewCRC64()))
	// Output: 7
}

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		JumpHash(uint64(i), int32(i))
	}
}

func BenchmarkHashStringCRC32(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), NewCRC32())
	}
}

func BenchmarkHashStringCRC64(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), NewCRC64())
	}
}

func BenchmarkHashStringFNV1(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), NewFNV1())
	}
}

func BenchmarkHashStringFNV1a(b *testing.B) {
	s := "Lorem ipsum dolor sit amet, consectetuer adipiscing elit, sed diam nonummy nibh euismod tincidunt ut laoreet dolore magna aliquam erat volutpat."
	for i := 0; i < b.N; i++ {
		HashString(s, int32(i), NewFNV1a())
	}
}

func Test_Jump_ConsistentHash(t *testing.T) {
	nodeNumList := []int32{1, 100, 200, 300, 400, 500, 600, 700, 800, 900}
	//测试10台服务器
	// var nodeNum uint
	//测试数据量100W
	testCount := 10000000

	for _, nodeNum := range nodeNumList {
		distributeMap := make(map[int32]int64)
		var i int32
		for i = 0; i < nodeNum; i++ {
			distributeMap[i] = 0
		}

		//测试100W个数据分布
		rand.Seed(time.Now().Unix())
		for i := 0; i < testCount; i++ {
			key := rand.Int63n(int64(testCount))
			b := JumpHash(uint64(key), int32(nodeNum))
			distributeMap[b] = distributeMap[b] + 1
		}

		var values []float64
		for _, v := range distributeMap {
			values = append(values, float64(v))
		}
		// sort.Strings(keys)
		fmt.Printf("####测试%d个结点,%d条测试数据\n", nodeNum, testCount)
		// for i, k := range distributeMap {
		// 	fmt.Printf("服务器:%d 分布数据数:%d\n", i, k)
		// }
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
