# Writed by TianxingPan on 2021/06/04

.PHONY: build
build:
	@which thrift > /dev/null 2>&1; if test $$? -ne 0; then echo -e "No command '\033[1;33mprotoc\033[m' found, please check environment variable '\033[1;33mPATH\033[m'."; exit 1; fi
	thrift --gen go -r -out . ./echo.thrift
	#go build -o thrift_client thrift_client.go
	go build -o thrift_server thrift_server.go

.PHONY: clean
clean:
	rm -f thrift_server thrift_client
