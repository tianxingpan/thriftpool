// Package thriftpool
package thriftpool

import (
	"context"
	"errors"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"sync/atomic"
	"time"
)

// thrift连接
// 约束：同一个conn不应该同时被多个协程使用
type ThriftConn struct {
	Endpoint	string			// 服务端的端点
	closed		bool			// 为 true 表示已被关闭，这种状态的不能再使用和放回池
	client		*thrift.TSocket	// thrift连接
	usedTime	time.Time		// 最近使用时间
}

// thrift连接池
type ThriftPool struct {
	Endpoint		string				// 服务端的端点
	DialTimeout		int32				// 拨号超时/连接超时（单位：秒）
	IdleTimeout		int32				// 空闲连接超时时长（单位：秒）
	MaxSize			int32				// 连接池最大连接数
	InitSize		int32				// 连接池初始连接数
	used			int32				// 已用连接数
	idle			int32				// 空闲连接数（即在 clients 中的连接数）
	assessTime		int64				// 最近异常调用Get或者Put的时间，根据它来判定该池是否活跃
	closed			int32				// 关闭连接池
	clients chan *ThriftConn			// thrift连接队列
}

func (t *ThriftConn) GetEndpoint() string {
	return t.Endpoint
}

func (t *ThriftConn) GetClient() *thrift.TSocket {
	return t.client
}

func (t *ThriftConn) GetUsedTime() int64 {
	return t.usedTime.Unix()
}

func (t *ThriftConn) UpdateUsedTime() int64 {
	t.usedTime = time.Now()
	return t.usedTime.Unix()
}

// 关闭thrift连接
func (t *ThriftConn) Close() error {
	if t.closed {
		return nil
	}
	t.closed = true
	return t.client.Close()
}

func (t *ThriftConn) IsClose() bool {
	return t.closed
}

// 更新最近使用时间

//
func (t *ThriftPool) Get(ctx context.Context) (*ThriftConn, error) {
	return t.get(ctx, false)
}

func (t *ThriftPool) get(ctx context.Context, doNotNew bool) (*ThriftConn, error) {
	accessTime := time.Now().Unix()
	atomic.StoreInt64(&t.assessTime, accessTime)
	curUsed := t.addUsed()

	select {
	case conn := <-t.clients:
		t.subIdle()
		return conn, nil
	default:
		if doNotNew {
			return nil, nil
		}
		if curUsed > t.MaxSize {
			newUsed := t.subUsed()
			return nil, errors.New(fmt.Sprintf("thriftpool empty, used:%d/%d", curUsed, newUsed))
		}
		var err error
		var client *thrift.TSocket

		if t.DialTimeout > 0 {
			client, err = thrift.NewTSocketTimeout(t.Endpoint, time.Duration(t.DialTimeout) * time.Second)
		} else {
			client, err = thrift.NewTSocket(t.Endpoint)
		}

		if err != nil {
			// 错误处理还得继续
			return nil, err
		}

		err = client.Open()
		if err != nil {
			// 错误错误处理
			return nil, err
		}
		conn := new(ThriftConn)
		conn.Endpoint = t.Endpoint
		conn.closed = false
		conn.client = client
		conn.usedTime = time.Now()
		return conn, nil
	}
}

func (t *ThriftPool) Put(conn *ThriftConn) error {
	return t.put(conn, false)
}

func (t *ThriftPool) put(conn *ThriftConn, doNotNew bool) error {
	accessTime := time.Now().Unix()
	atomic.StoreInt64(&t.assessTime, accessTime)
	defer func() {
		// 捕获panic，因为channel关闭时，再向关闭的channel写数据时，会导致panic
		if err := recover(); err != nil {
			_ = conn.Close()
			t.subIdle()
		}
	}()

	used := t.subUsed()
	closed := atomic.LoadInt32(&t.closed)
	if closed == 1 {
		if !conn.IsClose() {
			_ = conn.Close()
		}
		return nil
	}
	if conn.IsClose() {
		// 如果ThriftConn关闭时，无需返回队列
		return nil
	}
	idle := t.addIdle()
	usedTime := conn.GetUsedTime()
	var nowTime int64
	if !doNotNew {
		nowTime = conn.UpdateUsedTime()
	} else {
		nowTime = time.Now().Unix()
	}

	if idle > t.InitSize {
		if nowTime > usedTime {
			iTime := nowTime - usedTime
			if iTime > int64(t.IdleTimeout) {
				_ = conn.Close()
				t.subIdle()
				// 闲置连接，回收连接资源
				return nil
			}
			// 创建的资源大于最大连接数时，关闭连接，回收连接资源
			if idle > t.MaxSize {
				_ = conn.Close()
				t.subIdle()
				return nil
			}
		}
	}
	select {
	case t.clients <- conn:
		return nil
	default:
		_ = conn.Close()
		t.subIdle()
		return errors.New(fmt.Sprintf("use:%d, init:%d, idle:%d", used, t.InitSize, t.GetIdle()))
	}
}

func (t *ThriftPool) GetAssessTime() int64 {
	return atomic.LoadInt64(&t.assessTime)
}
// 关闭连接池（释放资源）
func (t *ThriftPool) Close() {
	swp := atomic.CompareAndSwapInt32(&t.closed, 0, 1)
	if !swp {
		return
	}

	close(t.clients)
	for {
		select {
		case conn := <- t.clients:
			if conn == nil {
				continue
			}
			_ = conn.Close()
		default:
			return
		}
	}
}

// 回收闲置资源
func (t *ThriftPool) releaseIdleConn() {
	for {
		closed := atomic.LoadInt32(&t.closed)
		if closed == 1 {
			break
		}

		time.Sleep(time.Duration(1) * time.Second)
		initSize := t.GetInitSize()
		idleSize := t.GetIdle()
		usedSize := t.GetUsed()
		// 当闲置连接大于在用连接，说明连接池比较空闲
		if idleSize > initSize && usedSize < idleSize {
			for i:=0; i<int(idleSize); i++ {
				conn, _ := t.get(context.Background(), true)
				if conn == nil {
					break
				}
				err := t.put(conn, true)
				// TODO:错误处理
				fmt.Println(err)
			}
		}
	}
}

func (t *ThriftPool) addUsed() int32 {
	return atomic.AddInt32(&t.used, 1)
}

func (t *ThriftPool) subUsed() int32 {
	return atomic.AddInt32(&t.used, -1)
}

func (t *ThriftPool) addIdle() int32 {
	return atomic.AddInt32(&t.idle, 1)
}

func (t *ThriftPool) subIdle() int32 {
	return atomic.AddInt32(&t.idle, -1)
}

func (t *ThriftPool) GetIdle() int32 {
	return atomic.LoadInt32(&t.idle)
}

func (t *ThriftPool) GetUsed() int32 {
	return atomic.LoadInt32(&t.used)
}

func (t *ThriftPool) GetInitSize() int32 {
	return t.InitSize
}

func (t *ThriftPool) GetMaxSize() int32 {
	return t.MaxSize
}

func (t *ThriftPool) GetEndpoint() string {
	return t.Endpoint
}

func (t *ThriftPool) SetIdleTimeout(timeout int32) {
	if timeout < 1 {
		t.IdleTimeout = 1
	} else {
		t.IdleTimeout = timeout
	}
}

func (t *ThriftPool) SetDialTimeout(timeout int32) {
	if timeout < 1 {
		t.DialTimeout = 1
	} else {
		t.DialTimeout = timeout
	}
}
