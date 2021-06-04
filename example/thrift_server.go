// package main provides thriftpool test cases
package main

import (
	"flag"
	"fmt"
	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/tianxingpan/thriftpool/example/echo"
	"os"
)

var (
	help = flag.Bool("h", false, "Display a help message and exit.")
	port = flag.Uint("port", 9898, "Port of Thrift server.")

)

type EchoServer struct {
}

// 接口Echo的实现
func (e *EchoServer) Echo(req *echo.EchoReq) (*echo.EchoRes, error) {
	fmt.Printf("message from client: %v\n", req.GetMsg())

	res := &echo.EchoRes{
		Msg: "success",
	}

	return res, nil
}

func main() {
	flag.Parse()
	if *help {
		flag.Usage()
		os.Exit(1)
	}

	transportFactory := thrift.NewTFramedTransportFactory(thrift.NewTTransportFactory())
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	serverTransport, err := thrift.NewTServerSocket(fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Println("Error!", err)
		os.Exit(1)
	}

	handler := &EchoServer{}
	processor := echo.NewEchoProcessor(handler)

	server := thrift.NewTSimpleServer4(processor, serverTransport, transportFactory, protocolFactory)
	fmt.Println("thrift server in :", *port)

	if err := server.Serve(); err != nil {
		panic(err)
	}
}
