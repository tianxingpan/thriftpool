// Writed by TianxingPan on 2021/06/04
namespace go echo

// 请求
struct EchoReq {
    1: string msg;  // 问候内容
}

// 响应
struct EchoRes {
    1: string msg;  // 响应内容
}

service Echo {
    EchoRes echo(1: EchoReq req);
}