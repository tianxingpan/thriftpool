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
	dialTimeout = flag.Uint("dial_timeout", 5, "Dial timeout in seconds.")
	idleTimeout = flag.Uint("idle_timeout", 5, "Idle timeout in seconds.")
)

var (
	thriftPool *thriftpool.ThriftPool
	//stopChan chan bool
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

	wg.Add(int(*numConcurrency))

	startTime := time.Now()
	numPendingRequests = int32(*numRequests)
	for i:=0; i<int(*numConcurrency); i++ {
		go consumer(i)
	}
	wg.Wait()

	thriftPool.Close()
	consumerDuration := time.Since(startTime)
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
	tClient := thriftConn.GetClient()
	transF := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protoF := thrift.NewTBinaryProtocolFactoryDefault()
	useTrans := transF.GetTransport(tClient)
	client := echo.NewEchoClientFactory(useTrans, protoF)

	req := echo.EchoReq{Msg:"Hello"}
	_, err = client.Echo(&req)
	if err != nil {
		fmt.Printf("%s\n", err.Error())
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
	//if index == 0 {
	//	fmt.Printf("call res:%s\n", res.Msg)
	//}
}
