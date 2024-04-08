package gocache

import (
	"context"
	"fmt"
	"gocache/consistenthash"
	pb "gocache/gocachepb"
	"gocache/registry"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"log"
	"net"
	"strings"
	"sync"
)

/*
	为geecache之间提供通信能力
	发送grpc请求的节点视为Client
	处理回应请求的为Server
*/

const (
	//defaultAddr     = "127.0.0.1:6324"
	defaultReplicas = 50
)

// Server 和 Group 是解耦合的 所以server要自己实现并发控制
type Server struct {
	pb.UnimplementedGroupCacheServer //gRPC 自动生成的代码，用于实现 gRPC 的服务端接口。

	self       string              // 当前服务器的地址，format: ip:port
	status     bool                // 当前服务器的运行状态，true: running false: stop
	stopSignal chan error          // 用于接收通知，通知服务器停止运行。通常是其他组件发出的信号，例如 registry 服务，用于通知当前服务停止运行。
	mu         sync.Mutex          //保护共享资源的互斥锁
	peers      *consistenthash.Map //一致性哈希（consistent hash）映射，用于确定缓存数据在集群中的分布。
	clients    map[string]*Client  //用于存储其他节点的客户端连接。键是其他节点的地址，值是与该节点建立的客户端连接
}

// NewServer 创建cache的 Server
func NewServer(self string) (*Server, error) {
	return &Server{
		self:    self,
		peers:   consistenthash.New(defaultReplicas, nil),
		clients: map[string]*Client{},
	}, nil
}

// Get 实现了 Server 结构体用于处理 gRPC 客户端的请求
func (s *Server) Get(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	// 处理客户端的 gRPC 请求，获取缓存数据并返回响应
	group, key := in.Group, in.Key
	resp := &pb.Response{}

	log.Printf("[Geecache_svr %s] Recv RPC Request - (%s)/(%s)", s.self, group, key)
	if key == "" {
		return resp, fmt.Errorf("key required")
	}
	g := GetGroup(group)
	if g == nil {
		return resp, fmt.Errorf("group not found")
	}
	view, err := g.GetCacheData(key)
	if err != nil {
		return resp, err
	}

	// 将获取到的缓存数据序列化为 protobuf 格式，并存储在响应对象的 Value 字段中
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		log.Printf("encoding response body:%v", err)
	}
	resp.Value = body
	return resp, nil
}

// Start  方法负责启动缓存服务，监听指定端口，注册 gRPC 服务至服务器，并在接收到停止信号后关闭服务
func (s *Server) Start() error {
	// 启动缓存服务，监听端口，注册 gRPC 服务，处理停止信号
	s.mu.Lock()
	if s.status == true {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	/*
		-----------------启动服务----------------------
		1. 设置status为true 表示服务器已在运行
		2. 初始化stop channel,这用于通知registry stop keep alive
		3. 初始化tcp socket并开始监听
		4. 注册rpc服务至grpc 这样grpc收到request可以分发给server处理
		5. 将自己的服务名/Host地址注册至etcd 这样client可以通过etcd,获取服务Host地址 从而进行通信。
			这样的好处是client只需知道服务名，以及etcd的Host即可获取对应服务IP 无需写在至client代码中
		----------------------------------------------
	*/
	// 设置服务器状态为运行中
	s.status = true
	s.stopSignal = make(chan error)

	port := strings.Split(s.self, ":")[1]
	lis, err := net.Listen("tcp", ":"+port) //监听指定的 TCP 端口，用于接受客户端的 gRPC 请求
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	// 注册 gRPC 服务
	// 创建一个新的 gRPC 服务器 grpcServer，然后将当前的 Server 对象 s 注册为 gRPC 服务。
	// 这样，gRPC 服务器就能够处理来自客户端的请求。
	grpcServer := grpc.NewServer()
	pb.RegisterGroupCacheServer(grpcServer, s)

	go func() {
		// 将当前服务注册至 etcd。该操作会一直阻塞，直到停止信号被接收。
		// 当停止信号被接收后，关闭通知通道 s.stopSignal，关闭 TCP 监听端口，并输出日志表示服务已经停止。
		err := registry.Register("gocache", s.self, s.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}

		// 当 registry.Register 函数执行完毕（即停止信号被接收）后，关闭通知通道 s.stopSignal，表示通知信号已经发送完毕
		close(s.stopSignal)
		// 关闭 TCP 监听端口，停止接受新的连接请求
		err = lis.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		// 服务已经停止
		log.Printf("[%s] Revoke service and close tcp socket ok.", s.self)
	}()

	s.mu.Unlock()

	//启动 gRPC 服务器。grpcServer.Serve(lis) 会阻塞，处理客户端的 gRPC 请求，直到服务器关闭或发生错误。
	//如果服务器状态为运行状态（s.status 为 true），并且发生了错误，则返回相应的错误。
	if err := grpcServer.Serve(lis); s.status && err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}

// Set 方法用于设置其他缓存节点的地址信息，并为每个节点创建相应的客户端连接
func (s *Server) Set(peersAddr ...string) {
	// 设置其他缓存节点的地址信息，并为每个节点创建客户端连接
	s.mu.Lock()
	defer s.mu.Unlock()

	// 将传入的所有节点地址批量添加到一致性哈希映射 s.peers 中
	s.peers.Add(peersAddr...)
	// 遍历传入的节点地址列表 peersAddr，为每个节点创建一个客户端连接
	// 这里拿到的是服务器的名称，这个map里面存的就是对应的地址
	for _, peerAddr := range peersAddr {
		//客户端的服务名（service）由节点地址构成，并且遵循一定的命名规则（在这里是 gocache/<peerAddr>）。
		service := fmt.Sprintf("gocache/%s", peerAddr)
		//使用 NewClient(service) 函数创建一个新的客户端连接，并将连接对象存储在 s.clients 映射中，以便后续通过节点地址进行查找和通信
		s.clients[peerAddr] = NewClient(service)
	}
}

// PickPeer 方法，用于根据给定的键选择相应的对等节点，根据在哈希环上拿到的key返回的是对应的地址
func (s *Server) PickPeer(key string) (PeerGetter, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	peerAddr := s.peers.Get(key) //根据给定的键 key 选择相应的对等节点的地址 peerAddr
	if peerAddr == s.self {      //如果选择的节点地址与当前服务器的地址相同，说明该节点就是当前服务器本身
		log.Printf("ooh! pick myself, I am %s\n", s.self)
		return nil, false
	}
	log.Printf("[cache %s] pick remote peer: %s\n", s.self, peerAddr)
	return s.clients[peerAddr], true //如果选择的节点不是当前服务器本身，日志会记录当前服务器选择了远程对等节点，并且函数会返回选择的对等节点的客户端连接（s.clients[peerAddr]）和 true，表示选择成功
}

// Stop 停止server运行 如果server没有运行 这将是一个no-op
func (s *Server) Stop() {
	s.mu.Lock()
	if s.status == false {
		s.mu.Unlock()
		return
	}
	s.stopSignal <- nil // 发送停止keepalive信号
	s.status = false    // 设置server运行状态为stop
	s.clients = nil     // 清空一致性哈希信息 有助于垃圾回收
	s.peers = nil       // 清空一致性哈希映射
	s.mu.Unlock()
}

// 测试 Server 是否实现了 PeerPicker 接口
var _ PeerPicker = (*Server)(nil)

/*
	如何理解这个Server和Client。
	比如,我8003端口pick远程节点是8001端口，
	那么8003端口的Client就会发送grpc请求给8001端口，
	8001端口的Server就会处理8003端口发过来的grpc请求。
*/
