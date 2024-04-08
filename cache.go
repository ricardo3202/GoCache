package gocache

import (
	"gocache/lfu"
	"gocache/lru"
	"sync"
)

// BaseCache 是一个接口，定义了基本的缓存操作方法。它包含了两个方法：add 和 get，用于向缓存中添加数据和从缓存中获取数据。
type BaseCache interface {
	add(key string, value ByteView)
	get(key string) (value ByteView, ok bool)
}

// LRUcache 对lru算法的封装,加锁实现并发缓存
type LRUcache struct {
	mu         sync.RWMutex
	lru        *lru.LRUCache
	cacheBytes int64 // 最大内存容量
}

// add 用于向缓存中添加数据
func (c *LRUcache) add(key string, value ByteView) {
	c.mu.Lock() // 写锁
	defer c.mu.Unlock()
	/*
		延迟初始化，一个对象的创建会延迟到第一次使用该对象时，可以减少开销，提高性能
	*/
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, value, value.Expire())
}

// get 用于从缓存中获取数据
func (c *LRUcache) get(key string) (value ByteView, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lru == nil {
		return
	}

	if v, ok := c.lru.Get(key); ok {
		return v.(ByteView), ok
	}
	return
}

// LFUcache 对lfu算法的封装,加锁实现并发缓存
type LFUcache struct {
	mu         sync.RWMutex
	lfu        *lfu.LFUCache
	cacheBytes int64 // 最大内存容量
}

// add 用于向缓存中添加数据
func (c *LFUcache) add(key string, value ByteView) {
	c.mu.Lock() // 写锁
	defer c.mu.Unlock()
	/*
		延迟初始化，一个对象的创建会延迟到第一次使用该对象时，可以减少开销，提高性能
	*/
	if c.lfu == nil {
		c.lfu = lfu.New(c.cacheBytes, nil)
	}
	c.lfu.Add(key, value, value.Expire())
}

// get 用于从缓存中获取数据
func (c *LFUcache) get(key string) (value ByteView, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.lfu == nil {
		return
	}

	if v, ok := c.lfu.Get(key); ok {
		return v.(ByteView), ok
	}
	return
}
