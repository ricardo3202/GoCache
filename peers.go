package gocache

import pb "gocache/gocachepb"

// PeerPicker 定义了获取分布式节点的能力,根据key返回了一个节点
type PeerPicker interface { // 查应该去哪个节点，返回值也是节点
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter is the interface that must be implemented by a peer.
// PeerGetter 定义了从远端获取缓存的能力
// 所以每个Peer应实现这个接口
// 去请求其他节点的数据，本节点就是客户端了，抽象成这个接口，用于去远程节点取数据
type PeerGetter interface { // 这个返回的数数据
	Get(in *pb.Request, out *pb.Response) error // 用于从对应的group中查找缓存值
}
