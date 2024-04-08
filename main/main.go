package main

import (
	"fmt"
	"gocache"
	"log"
	"sync"
)

/*
用户通过 API 服务器（例如 http://localhost:9999）访问 /api?key=XXX 的形式来获取缓存数据。
API 服务器会调用对应缓存组的 Get 方法。
Get 方法首先尝试从本地缓存中的热点缓存（hotCache）中查找数据。
如果数据不在热点缓存中，它将尝试从主缓存（mainCache）中查找数据。
如果主缓存中也没有数据，缓存系统将选择一个远程节点（可能是本地节点，也可能是其他节点）。
如果选中的远程节点是本地节点，缓存系统会直接从数据源获取数据。
如果选中的远程节点不是本地节点，API 服务器将发送 gRPC 请求给对应的远程节点，要求其提供数据。

注意：节点刚开始都会注册到etcd。
*/

func main() {
	// db 是伪造的数据源
	db := map[string]string{
		"Tom":  "630",
		"Jack": "589",
		"Sam":  "567",
	}

	group := gocache.NewGroup("scores", 2<<10, "lru", gocache.GetterFunc( //lru算法做测试
		func(key string) ([]byte, error) {
			log.Println("[SlowDB] Search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	// New一个服务实例
	var addr string = "localhost:9999"
	svr, err := gocache.NewServer(addr)

	if err != nil {
		log.Fatal(err)
	}
	// 设置同伴节点IP(包括自己)
	svr.Set(addr)            // 将addr地址添加到svr服务中
	group.RegisterPeers(svr) // 把服务中的地址给了group
	log.Println("gocache is running at", addr)
	// 启动服务(注册服务至etcd/计算一致性哈希...)
	go func() {
		// Start将不会return 除非服务stop或者抛出error
		err = svr.Start()
		if err != nil {
			log.Fatal(err)
		}
	}()
	// 发出几个Get请求
	var wg sync.WaitGroup
	wg.Add(4)
	go GetTomScore(group, &wg)
	go GetTomScore(group, &wg)
	go GetTomScore(group, &wg)
	go GetTomScore(group, &wg)
	wg.Wait()

}

func GetTomScore(group *gocache.Group, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("get Jack...")
	view, err := group.GetCacheData("Sam")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(view.String())
}
