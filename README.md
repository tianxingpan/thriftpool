# thriftpool
一个Go版本的thrift连接池实现，已解决协程长连接方式使用thrift，该package是基于chan队列方式来实现的。
该package是基于thrift-0.9.3来实现的，暂时还不支持其他版本，故此请使用该package时，请谨慎考虑。

## 版本依赖
thrift-0.9.3

## 使用说明
具体的使用方法可以参考[thrift_client.go](./example/thrift_client.go)