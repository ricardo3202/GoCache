package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Hash maps bytes to uint32
type Hash func(data []byte) uint32

// Map constains all hashed keys
type Map struct {
	hash     Hash           // 哈希函数
	replicas int            // 虚拟节点倍数
	ring     []int          // 哈希环
	hashMap  map[int]string // 虚拟节点的hash到真实节点的映射
}

// New 创建一个map实例
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 向哈希环中添加节点
func (m *Map) Add(keys ...string) {
	for _, key := range keys { // 一次可能传入多个节点
		for i := 0; i < m.replicas; i++ { // 每一个节点要对应几个虚拟节点
			hash := int(m.hash([]byte(strconv.Itoa(i) + key))) // 虚拟节点的值映射出hash
			m.ring = append(m.ring, hash)                      // 把虚拟节点添加进哈希环
			m.hashMap[hash] = key                              // 虚拟节点的hash对应真实的节点
		}
	}
	sort.Ints(m.ring)
}

// Get 对于传入的数据该分到哪个节点？
func (m *Map) Get(key string) string {
	if len(m.ring) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key))) // 先取数据key的hash
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.ring), func(i int) bool { // 拿到顺时针最近的虚拟节点
		return m.ring[i] >= hash
	})
	// 返回真实节点的key,是一个string类型的数据
	return m.hashMap[m.ring[idx%len(m.ring)]] // 用来处理idx == len(.keys),本身返回的idx就已经是虚拟节点的hash了
}
