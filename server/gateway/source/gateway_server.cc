//网关子服务的服务器入口文件
//网关是客户端(APP/Web/Android/iOS)访问后端各微服务的统一入口,职责包括:
//  1. 协议转换:HTTP(9000)/WebSocket(9001) ↔ brpc
//  2. 统一鉴权:基于 session_id 从 Redis 反查 user_id 并注入下游请求
//  3. WebSocket 长连接管理 + 业务推送(好友申请、新消息、会话创建等)
//  4. 服务发现 + 负载均衡(RR):通过 ServiceManager 选择下游子服务节点
//本文件只负责参数解析、日志初始化、Builder 装配与启动,具体业务逻辑见 gateway_server.hpp

#include "gateway_server.hpp"

//--------------- 日志与运行模式 ---------------
DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

//--------------- 对外服务端口 ---------------
DEFINE_int32(http_listen_port, 9000, "HTTP服务器监听端口");
DEFINE_int32(websocket_listen_port, 9001, "Websocket服务器监听端口");

//--------------- etcd 服务发现相关 ---------------
//registry_host 为 etcd 注册中心地址;其余为各下游子服务在 etcd 中的监控路径
DEFINE_string(registry_host, "http://127.0.0.1:2379", "服务注册中心地址");
DEFINE_string(base_service, "/service", "服务监控根目录");
DEFINE_string(file_service, "/service/file_service", "文件存储子服务名称");
DEFINE_string(friend_service, "/service/friend_service", "好友管理子服务名称");
DEFINE_string(message_service, "/service/message_service", "消息存储子服务名称");
DEFINE_string(user_service, "/service/user_service", "用户管理子服务名称");
DEFINE_string(speech_service, "/service/speech_service", "语音识别子服务名称");
DEFINE_string(transmite_service, "/service/transmite_service", "转发管理子服务名称");

//--------------- Redis 相关(用于会话/在线状态) ---------------
DEFINE_string(redis_host, "127.0.0.1", "Redis服务器访问地址");
DEFINE_int32(redis_port, 6379, "Redis服务器访问端口");
DEFINE_int32(redis_db, 0, "Redis默认库号");
DEFINE_bool(redis_keep_alive, true, "Redis长连接保活选项");

int main(int argc, char *argv[])
{
    //1. 解析命令行参数(支持 -flagfile=/im/conf/gateway_server.conf 加载配置文件)
    google::ParseCommandLineFlags(&argc, &argv, true);
    //2. 初始化日志模块(根据 run_mode 决定输出到控制台或文件)
    bite_im::init_logger(FLAGS_run_mode, FLAGS_log_file, FLAGS_log_level);

    //3. 通过 Builder 模式装配网关各模块并构造服务器对象
    bite_im::GatewayServerBuilder gsb;
    //3.1 装配 Redis 客户端(Session/Status 两张表的底层依赖)
    gsb.make_redis_object(FLAGS_redis_host, FLAGS_redis_port, FLAGS_redis_db, FLAGS_redis_keep_alive);
    //3.2 装配服务发现 + 信道管理(声明关注 6 个子服务,并绑定 etcd 上下线回调)
    gsb.make_discovery_object(FLAGS_registry_host, FLAGS_base_service, FLAGS_file_service,
        FLAGS_speech_service, FLAGS_message_service, FLAGS_friend_service, 
        FLAGS_user_service, FLAGS_transmite_service);
    //3.3 装配 HTTP/WebSocket 监听端口
    gsb.make_server_object(FLAGS_websocket_listen_port, FLAGS_http_listen_port);
    //4. 校验依赖完备性后构造 GatewayServer 实例
    auto server = gsb.build();
    //5. 启动网关:_ws_server.run() 阻塞当前线程跑 WebSocket 事件循环
    //   HTTP 服务在独立线程中运行(见 GatewayServer 构造函数 _http_thread)
    server->start();
    return 0;
}
