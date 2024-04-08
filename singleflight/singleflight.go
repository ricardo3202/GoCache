package singleflight

import "sync"

// call是一个正在进行或已完成的Do调用
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

type Group struct {
	mu sync.Mutex       // 用于保护m
	m  map[string]*call // 存储函数调用的映射表，key为调用的唯一标识，value为对应的call结构体指针
}

// Do 执行给定的函数，并返回结果，确保每个key只有一个执行在进行中。
// 如果重复调用发生，则重复调用者会等待原始调用完成并接收相同的结果。
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call) // 如果映射表尚未初始化，则进行初始化
	}
	if c, ok := g.m[key]; ok { // 如果在映射表中找到了对应的调用，则释放锁并等待调用完成
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)  // 增加等待组计数器，表示有一个调用正在进行中
	g.m[key] = c // 将新的调用结构体加入到映射表中
	g.mu.Unlock()

	// 执行给定的函数调用
	c.val, c.err = fn()
	c.wg.Done() // 标记调用完成

	g.mu.Lock()
	delete(g.m, key) // 从映射表中删除该调用
	g.mu.Unlock()

	return c.val, c.err
}
