package gocache

import (
	"fmt"
	pb "gocache/gocachepb"
	"gocache/singleflight"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

/*
	负责与外部交互，控制缓存存储和获取的主流程，提供了比cache模块更高一层抽象的能力
*/

var (
	maxMinuteRemoteQPS = 10                      //最大QPS
	mu                 sync.RWMutex              //读写锁
	groups             = make(map[string]*Group) //map,根据键缓存组的名字，获取对应的缓存组
)

// Getter 接口
type Getter interface {
	Get(key string) ([]byte, error)
}

// GetterFunc 函数类型
type GetterFunc func(key string) ([]byte, error)

// Get GetterFunc 实现了Getter 接口
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// KeyStats Key的统计信息
type KeyStats struct {
	firstGetTime time.Time //第一次请求的时间
	remoteCnt    AtomicInt //请求的次数（利用atomic包封装的原子类）
}

// Group 缓存的命名空间
type Group struct {
	name      string
	getter    Getter               // 回调函数，用于从数据源获取数据
	mainCache BaseCache            // 主缓存，是一个 BaseCache 接口的实例，用于存储本地节点作为主节点所拥有的数据
	hotCache  BaseCache            // hotCache 则是为了存储热门数据的缓存。
	peers     PeerPicker           //实现了 PeerPicker 接口的对象，用于根据键选择相应的缓存节点
	loader    *singleflight.Group  //确保相同的请求只被执行一次
	keys      map[string]*KeyStats //根据键key获取对应key的统计信息
}

type AtomicInt int64 // 封装一个原子类，用于进行原子操作，保证并发安全.

// Add 方法用于对 AtomicInt 中的值进行原子自增
func (i *AtomicInt) Add(n int64) { //原子自增
	atomic.AddInt64((*int64)(i), n)
}

// Get 方法用于获取 AtomicInt 中的值。
func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

// NewGroup create a new instance of Group
func NewGroup(name string, cacheBytes int64, CacheType string, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:   name,
		getter: getter,
		loader: &singleflight.Group{},
		keys:   map[string]*KeyStats{},
	}
	if CacheType == "lru" {
		g.mainCache = &LRUcache{cacheBytes: cacheBytes}
		g.hotCache = &LRUcache{cacheBytes: cacheBytes}
	} else if CacheType == "lfu" {
		g.mainCache = &LFUcache{cacheBytes: cacheBytes}
		g.hotCache = &LFUcache{cacheBytes: cacheBytes}
	}
	groups[name] = g // 存入全局变量
	return g
}

// GetGroup 根据缓存组的名字获取缓存组
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// GetCacheData 获取缓存数据 热点缓存—>主缓存—>数据源
func (g *Group) GetCacheData(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	if v, ok := g.hotCache.get(key); ok {
		log.Println("[GeeCache] hit hotCache")
		return v, nil
	}

	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}

	return g.load(key) // 查不到执行回调函数,获取值并添加进缓存
}

// 缓存未命中—>尝试从远程节点获取—>若获取失败则从本地获取
func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok { // 如果是本地节点就返回nil，如果不是就返回对应节点的地址
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GoCache] Failed to get from peer", err)
			}
		}
		// 该key的哈希值在哈希环中所对应的就是当前节点，因此调用回调方法，去本地的数据源拿值
		return g.getLocally(key)
	})
	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

// getLocally 从本地获取数据 并添加到本地缓存 与 热点缓存中
func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err

	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	g.populateHotCache(key, value)
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}

func (g *Group) populateHotCache(key string, value ByteView) {
	g.hotCache.add(key, value)
}

// RegisterPeers registers a PeerPicker for choosing remote peer
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	//远程获取cnt++
	if stat, ok := g.keys[key]; ok {
		stat.remoteCnt.Add(1)
		//计算QPS
		interval := float64(time.Now().Unix()-stat.firstGetTime.Unix()) / 60
		qps := stat.remoteCnt.Get() / int64(math.Max(1, math.Round(interval)))
		if qps >= int64(maxMinuteRemoteQPS) {
			//存入hotCache
			g.populateHotCache(key, ByteView{b: res.Value})
			//删除映射关系,节省内存
			mu.Lock()
			delete(g.keys, key)
			mu.Unlock()
		}
	} else {
		//第一次获取
		g.keys[key] = &KeyStats{
			firstGetTime: time.Now(),
			remoteCnt:    1,
		}
	}

	return ByteView{b: res.Value}, nil
}
