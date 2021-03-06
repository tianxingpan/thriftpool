// package main provides thriftpool test cases
package main

import (
	"flag"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/tianxingpan/thriftpool"
	"github.com/tianxingpan/thriftpool/example/echo"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	help = flag.Bool("h", false, "Display a help message and exit")
	server = flag.String("server", "127.0.0.1:9898", "Server of Thrift to connect.")
	initSize = flag.Int("init_size", 1, "Initial size of Thrift pool.")
	maxSize = flag.Int("max_size", 100, "Max size of Thrift pool.")

	numRequests = flag.Uint("n", 1, "Number of requests to perform.")
	numConcurrency = flag.Uint("c", 1, "Number of multiple requests to make a time.")
	dialTimeout = flag.Uint("dial_timeout", 5000, "Dial timeout in Millisecond.")
	idleTimeout = flag.Uint("idle_timeout", 5000, "Idle timeout in Millisecond.")
)

var (
	thriftPool *thriftpool.ThriftPool
	stopChan chan bool
	wg sync.WaitGroup
	numPendingRequests		int32	// 未处理的请求数
	numFinishRequests		int32	// 完成的请求数
	numSuccessRequests		int32	// 成功的请求数
	numCallFailedRequests	int32	// 调用失败的请求数
	numPoolFailedRequests	int32	// 取池失败数
)

func main() {
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(1)
	}
	if *server == "" {
		fmt.Printf("Parameter[-server] is not set.\n")
		flag.Usage()
		os.Exit(1)
	}
	fmt.Printf("PID is %d\n", os.Getpid())
	thriftPool = thriftpool.NewThriftPool(*server,
		int32(*dialTimeout), int32(*idleTimeout),
		int32(*maxSize), int32(*initSize))

	stopChan = make(chan bool)
	// 启动metric协程
	go metricCoroutine()

	wg.Add(int(*numConcurrency))

	startTime := time.Now()
	numPendingRequests = int32(*numRequests)
	for i:=0; i<int(*numConcurrency); i++ {
		go consumer(i)
	}
	wg.Wait()

	consumerDuration := time.Since(startTime)
	// 验证连接池回收连接
	idle := int32(*idleTimeout)
	time.Sleep(time.Duration(idle * 20) * time.Millisecond)
	stopChan <-true
	thriftPool.Close()
	close(stopChan)

	s := int(consumerDuration.Seconds())
	if s > 0 {
		qps := int(numFinishRequests) / s
		fmt.Printf("QPS: %d (Total: %d, Seconds: %d, Success: %d, PoolFailed: %d, CallFailed: %d)\n", qps,
			numFinishRequests, s, numSuccessRequests, numPoolFailedRequests, numCallFailedRequests)
	} else {
		fmt.Printf("QPS: %d (Total: %d, Seconds: %d, Success: %d, PoolFailed: %d, CallFailed: %d)\n", 0,
			numFinishRequests, s, numSuccessRequests, numPoolFailedRequests, numCallFailedRequests)
	}
}

// 统计协程
func metricCoroutine() {
	for ; ;  {
		select {
		case <-stopChan:
			return
		case <-time.After(time.Second*2):
			break
		}
		used := thriftPool.GetUsed()
		idle := thriftPool.GetIdle()
		chanSize := thriftPool.GetChanSize()
		fmt.Printf("[METRIC] thrift Pool metric, used:%d, idle:%d, chan:%d\n", used, idle, chanSize)
	}
}

func consumer(index int) {
	defer wg.Done()

	for {
		n := atomic.AddInt32(&numPendingRequests, -1)
		if n < 0 {
			break
		}
		atomic.AddInt32(&numFinishRequests, 1)
		request(index)
	}
}

func request(index int) {
	thriftConn, err := thriftPool.Get()
	if err != nil {
		atomic.AddInt32(&numPoolFailedRequests, 1)
		fmt.Printf("Get a Thrift connection from pool failed: %s\n", err.Error())
		return
	}
	socket := thriftConn.GetSocket()
	transF := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protoF := thrift.NewTBinaryProtocolFactoryDefault()
	useTrans := transF.GetTransport(socket)
	client := echo.NewEchoClientFactory(useTrans, protoF)

	req := echo.EchoReq{Msg:"Hello"}
	_, err = client.Echo(&req)
	if err != nil {
		fmt.Printf("[ECHO]%s\n", err.Error())
		_ = thriftConn.Close()
		_ = thriftPool.Put(thriftConn)
		atomic.AddInt32(&numCallFailedRequests, 1)
		if index == 0 {
			fmt.Printf("Echo to %d failed: %s\n", client.SeqId, err.Error())
		}
		return
	}
	_ = thriftPool.Put(thriftConn)
	atomic.AddInt32(&numSuccessRequests, 1)
}
