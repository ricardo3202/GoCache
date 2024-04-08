package gocache

import (
	"context"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	pb "gocache/gocachepb"
	"gocache/registry"
	"google.golang.org/protobuf/proto"
	"time"
)

// Client 实现gocache访问其他远程节点获取缓存的能力
type Client struct {
	baseURL string // 服务名称 gocache/ip:addr
}

var (
	//这个变量通常用于创建etcd客户端的配置，当你不需要定制化的配置时，可以直接使用 defaultEtcdConfig 这个预定义的配置。
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"localhost:2379"}, // etcd服务器的地址，这里使用本地地址和默认端口
		DialTimeout: 5 * time.Second,            // 建立连接的超时时间为5秒
	}
)

// Get 方法允许 Client 结构体实例向远程节点发送请求，获取缓存数据，并将响应解码为 pb.Response 结构体。
func (c *Client) Get(in *pb.Request, out *pb.Response) error {
	cli, err := clientv3.New(defaultEtcdConfig) // 创建一个etcd客户端
	if err != nil {
		return err
	}
	defer cli.Close()

	//使用etcd客户端发现指定服务（g.baseURL）并建立连接（conn）。如果发现服务或建立连接失败，则返回错误。
	conn, err := registry.EtcdDial(cli, c.baseURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	//创建一个 gRPC 客户端，用于向远程对等节点发送请求
	grpcClient := pb.NewGroupCacheClient(conn)

	//创建一个带有10秒超时时间的上下文，并使用该上下文发送 gRPC 请求到远程节点
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	response, err := grpcClient.Get(ctx, in)
	if err != nil {
		return fmt.Errorf("reading response body:%v", err)
	}
	if err = proto.Unmarshal(response.GetValue(), out); err != nil {
		return fmt.Errorf("decoding response body:%v", err)
	}
	return nil
}

// NewClient 创建一个远程节点客户端
func NewClient(service string) *Client {
	return &Client{baseURL: service}
}

// 测试 Client 是否实现了 PeerGetter 接口
var _ PeerGetter = (*Client)(nil)
