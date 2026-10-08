//消息转发子服务的服务器入口文件
//本文件是消息转发子服务(MsgTransmitService)的启动入口,职责是参数解析、日志初始化、
//并通过建造者模式(TransmiteServerBuilder)逐步装配各组成模块后启动RPC服务器。
//
//消息转发子服务在整个系统中的定位:
//  1. 接收网关(NewMessage handler)通过brpc同步调用的GetTransmitTarget请求
//  2. 调用用户子服务补全发送者信息,组装出完整的MessageInfo
//  3. 查询会话成员表,算出本条消息的所有目标用户(target_id_list)
//  4. 将MessageInfo发布到RabbitMQ,交由消息存储子服务异步持久化(留底)
//  5. 把组织好的message + target_id_list打包成响应返回给调用方网关
//注意:本服务只负责"算目标 + 发MQ留底",并不直接向客户端推送 --
//     客户端的实时推送由调用方网关在自己本地按target_id_list反查WebSocket连接完成。
#include "transmite_server.hpp"

//--------------- 日志与运行模式 ---------------
DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

//--------------- 服务实例标识与etcd注册 ---------------
//instance_name + access_host 用于多实例区分,本实例会以 base_service+instance_name 为键注册到etcd
DEFINE_string(registry_host, "http://127.0.0.1:2379", "服务注册中心地址");
DEFINE_string(instance_name, "/transmite_service/instance", "当前实例名称");
DEFINE_string(access_host, "127.0.0.1:10004", "当前实例的外部访问地址");

//--------------- RPC服务器参数 ---------------
DEFINE_int32(listen_port, 10004, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");

//--------------- 服务发现相关(本服务作为客户端,需发现用户子服务) ---------------
DEFINE_string(base_service, "/service", "服务监控根目录");
DEFINE_string(user_service, "/service/user_service", "用户管理子服务名称");

//--------------- MySQL参数(用于查询聊天会话成员表,确定消息转发目标) ---------------
DEFINE_string(mysql_host, "127.0.0.1", "Mysql服务器访问地址");
DEFINE_string(mysql_user, "root", "Mysql服务器访问用户名");
DEFINE_string(mysql_pswd, "123456", "Mysql服务器访问密码");
DEFINE_string(mysql_db, "bite_im", "Mysql默认库名称");
DEFINE_string(mysql_cset, "utf8", "Mysql客户端字符集");
DEFINE_int32(mysql_port, 0, "Mysql服务器访问端口");
DEFINE_int32(mysql_pool_count, 4, "Mysql连接池最大连接数量");

//--------------- RabbitMQ参数(用于将消息发布到队列,供消息存储子服务异步持久化) ---------------
DEFINE_string(mq_user, "root", "消息队列服务器访问用户名");
DEFINE_string(mq_pswd, "123456", "消息队列服务器访问密码");
DEFINE_string(mq_host, "127.0.0.1:5672", "消息队列服务器访问地址");
DEFINE_string(mq_msg_exchange, "msg_exchange", "持久化消息的发布交换机名称");
DEFINE_string(mq_msg_queue, "msg_queue", "持久化消息的发布队列名称");
DEFINE_string(mq_msg_binding_key, "msg_queue", "持久化消息的发布队列名称");


int main(int argc, char *argv[])
{
    //1. 解析命令行参数(支持 -flagfile 从配置文件读取,见部署时的 -flagfile=/im/conf/transmite_server.conf)
    google::ParseCommandLineFlags(&argc, &argv, true);
    //2. 初始化日志模块(根据运行模式决定输出到控制台还是文件,并设置日志等级)
    bite_im::init_logger(FLAGS_run_mode, FLAGS_log_file, FLAGS_log_level);

    //3. 通过建造者模式逐步装配消息转发子服务的各组成模块
    bite_im::TransmiteServerBuilder tsb;
    //3.1 装配RabbitMQ客户端(声明exchange/queue/binding,用于发布待持久化的消息)
    tsb.make_mq_object(FLAGS_mq_user, FLAGS_mq_pswd, FLAGS_mq_host,
        FLAGS_mq_msg_exchange, FLAGS_mq_msg_queue, FLAGS_mq_msg_binding_key);
    //3.2 装配MySQL客户端(查询聊天会话成员表,确定消息转发目标用户列表)
    tsb.make_mysql_object(FLAGS_mysql_user, FLAGS_mysql_pswd, FLAGS_mysql_host, 
        FLAGS_mysql_db, FLAGS_mysql_cset, FLAGS_mysql_port, FLAGS_mysql_pool_count);
    //3.3 装配服务发现+信道管理(声明关注用户子服务,并绑定etcd上下线回调)
    tsb.make_discovery_object(FLAGS_registry_host, FLAGS_base_service, FLAGS_user_service);
    //3.4 构造并启动RPC服务器,注册TransmiteServiceImpl(必须在注册etcd前完成,避免请求超时)
    tsb.make_rpc_server(FLAGS_listen_port, FLAGS_rpc_timeout, FLAGS_rpc_threads);
    //3.5 向etcd注册当前实例(服务名=base_service+instance_name,访问地址=access_host)
    tsb.make_registry_object(FLAGS_registry_host, FLAGS_base_service + FLAGS_instance_name, FLAGS_access_host);
    //4. 校验依赖完备性后构造TransmiteServer对象
    auto server = tsb.build();
    //5. 启动RPC服务器(阻塞运行,直到收到退出信号)
    server->start();
    return 0;
}
