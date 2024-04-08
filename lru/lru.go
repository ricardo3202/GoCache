package lru

import (
	"container/list"
	"time"
)

/*
LRUCache 定义了一个结构体，用来实现lru缓存淘汰算法
maxBytes：最大存储容量
nBytes：已占用的容量
ll：直接使用 Go 语言标准库实现的双向链表list.List，双向链表常用于维护缓存中各个数据的访问顺序，以便在淘汰数据时能够方便地找到最近最少使用的数据。
cache：map,键是字符串，值是双向链表中对应节点的指针
OnEvicted：是某条记录被移除时的回调函数，可以为 nil
Now：用于计算过期值的当前时间,默认为 time.Now()
*/

type NowFunc func() time.Time

// LRUCache is a LRU cache. It is not safe for concurrent access.
type LRUCache struct {
	maxCapacity int64
	curCapacity int64
	ll          *list.List
	cache       map[string]*list.Element
	OnEvicted   func(key string, value Value)
	Now         NowFunc
}

// 缓存中存储的数据类型,仍然保存key的好处是在删除队首节点时方便，这里的key就是cache里的key
type entry struct {
	key    string
	value  Value
	expire time.Time //节点的过期时间
}

// Value use Len to count how many bytes it takes
type Value interface {
	Len() int
}

// New is the Constructor of LRUCache
func New(maxCapacity int64, onEvicted func(string, Value)) *LRUCache {
	return &LRUCache{
		maxCapacity: maxCapacity,
		ll:          list.New(),
		cache:       make(map[string]*list.Element),
		OnEvicted:   onEvicted,
		Now:         time.Now,
	}
}

// Add adds a value to the cache. 向缓存中的添加或者更新数据
func (c *LRUCache) Add(key string, value Value, expire time.Time) {
	if c.cache == nil {
		c.cache = make(map[string]*list.Element)
		c.ll = list.New()
	}
	// 如果键已存在于缓存中，则更新其值和过期时间，并将该条目移到链表头部（表示最近访问）
	if node, ok := c.cache[key]; ok {
		c.ll.MoveToFront(node)                                      // 移至队尾
		kv := node.Value.(*entry)                                   // 断言取值
		c.curCapacity += int64(value.Len()) - int64(kv.value.Len()) // 更新大小
		kv.value = value                                            // 更新值
		kv.expire = expire                                          // 更新过期时间
	} else {
		node := c.ll.PushFront(&entry{key, value, expire})    //不存在那就创建节点放在队尾
		c.cache[key] = node                                   // 插入map
		c.curCapacity += int64(len(key)) + int64(value.Len()) //更新占用缓存
	}
	for c.maxCapacity != 0 && c.maxCapacity < c.curCapacity { // 内存超过最大内存了，就删一个
		c.RemoveOldest()
	}
}

// Get look ups a key's value，找到该节点，然后放到队尾去
func (c *LRUCache) Get(key string) (value Value, ok bool) {
	if c.cache == nil {
		return
	}
	if node, ok := c.cache[key]; ok {
		kv := node.Value.(*entry)
		if !kv.expire.IsZero() && kv.expire.Before(c.Now()) { // Before 传入一个时间，在这个时间之前返回ture，表示过期
			c.removeElement(node)
			return nil, false
		}
		c.ll.MoveToFront(node)
		return kv.value, true
	}
	return
}

// RemoveOldest removes the oldest item
func (c *LRUCache) RemoveOldest() {
	if c.cache == nil {
		return
	}
	node := c.ll.Back()
	if node != nil {
		c.removeElement(node)
	}
}

// Len the number of cache entries
func (c *LRUCache) Len() int {
	return c.ll.Len()
}

func (c *LRUCache) removeElement(node *list.Element) {
	c.ll.Remove(node)
	kv := node.Value.(*entry)
	delete(c.cache, kv.key)
	c.curCapacity -= int64(len(kv.key)) + int64(kv.value.Len())
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}
