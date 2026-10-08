//用户管理子服务的服务器入口文件
//主要职责：通过 gflags 定义运行参数，借助 UserServerBuilder 组装各子模块
//（DMS短信、ES搜索、MySQL、Redis、服务发现/注册、RPC服务器），并启动用户管理Rpc服务。
//对外提供：用户注册/登录、手机号注册/登录、短信验证码获取、
//用户信息查询（单个/批量）、头像/昵称/签名/手机号设置等接口。
#include "user_server.hpp"

//==== 日志与运行模式相关参数 ====
DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

//==== 服务注册中心(etcd)与当前实例信息 ====
DEFINE_string(registry_host, "http://127.0.0.1:2379", "服务注册中心地址");
DEFINE_string(instance_name, "/user_service/instance", "当前实例名称");
DEFINE_string(access_host, "127.0.0.1:10003", "当前实例的外部访问地址");

//==== RPC服务器参数 ====
DEFINE_int32(listen_port, 10003, "Rpc服务器监听端口");
DEFINE_int32(rpc_timeout, -1, "Rpc调用超时时间");
DEFINE_int32(rpc_threads, 1, "Rpc的IO线程数量");

//==== 服务发现参数：当前服务依赖的文件管理子服务 ====
DEFINE_string(base_service, "/service", "服务监控根目录");
DEFINE_string(file_service, "/service/file_service", "文件管理子服务名称");

//==== ES搜索引擎参数（用于用户信息的搜索/存储） ====
DEFINE_string(es_host, "http://127.0.0.1:9200/", "ES搜索引擎服务器URL");

//==== MySQL数据库参数（用于用户数据的持久化存储） ====
DEFINE_string(mysql_host, "127.0.0.1", "Mysql服务器访问地址");
DEFINE_string(mysql_user, "root", "Mysql服务器访问用户名");
DEFINE_string(mysql_pswd, "123456", "Mysql服务器访问密码");
DEFINE_string(mysql_db, "bite_im", "Mysql默认库名称");
DEFINE_string(mysql_cset, "utf8", "Mysql客户端字符集");
DEFINE_int32(mysql_port, 0, "Mysql服务器访问端口");
DEFINE_int32(mysql_pool_count, 4, "Mysql连接池最大连接数量");

//==== Redis参数（用于登录会话、登录状态、短信验证码的缓存） ====
DEFINE_string(redis_host, "127.0.0.1", "Redis服务器访问地址");
DEFINE_int32(redis_port, 6379, "Redis服务器访问端口");
DEFINE_int32(redis_db, 0, "Redis默认库号");
DEFINE_bool(redis_keep_alive, true, "Redis长连接保活选项");

//==== 阿里云短信平台(DMS)密钥（用于发送手机短信验证码） ====
DEFINE_string(dms_key_id, "", "阿里云访问密钥 ID（必须通过命令行或 flagfile 提供）");
DEFINE_string(dms_key_secret, "", "阿里云访问密钥 Secret（必须通过命令行或 flagfile 提供）");

int main(int argc, char *argv[])
{
    //1. 解析命令行参数（支持 -flagfile 从配置文件读取，见 dockerfile 中的 -flagfile=/im/conf/user_server.conf）
    google::ParseCommandLineFlags(&argc, &argv, true);
    //2. 初始化日志模块（根据运行模式决定输出到控制台还是文件，并设置日志等级）
    bite_im::init_logger(FLAGS_run_mode, FLAGS_log_file, FLAGS_log_level);

    if (FLAGS_dms_key_id.empty() || FLAGS_dms_key_secret.empty()) {
        LOG_ERROR("必须通过命令行或 flagfile 提供阿里云访问密钥");
        return 1;
    }

    //3. 通过建造者模式逐步组装用户子服务的各个组成模块
    bite_im::UserServerBuilder usb;
    usb.make_dms_object(FLAGS_dms_key_id, FLAGS_dms_key_secret);    //构造阿里云短信平台客户端（发送验证码）
    usb.make_es_object({FLAGS_es_host});                            //构造ES搜索引擎客户端（用户信息检索/存储）
    usb.make_mysql_object(FLAGS_mysql_user, FLAGS_mysql_pswd, FLAGS_mysql_host,
        FLAGS_mysql_db, FLAGS_mysql_cset, FLAGS_mysql_port, FLAGS_mysql_pool_count);  //构造MySQL客户端（用户数据持久化）
    usb.make_redis_object(FLAGS_redis_host, FLAGS_redis_port, FLAGS_redis_db, FLAGS_redis_keep_alive);  //构造Redis客户端（会话/状态/验证码缓存）
    usb.make_discovery_object(FLAGS_registry_host, FLAGS_base_service, FLAGS_file_service);  //构造服务发现对象，并管理文件子服务的信道
    usb.make_rpc_server(FLAGS_listen_port, FLAGS_rpc_timeout, FLAGS_rpc_threads);  //构造并启动RPC服务器，注册UserServiceImpl
    usb.make_registry_object(FLAGS_registry_host, FLAGS_base_service + FLAGS_instance_name, FLAGS_access_host);  //向etcd注册当前实例
    //4. 构建出完整的UserServer对象
    auto server = usb.build();
    //5. 启动RPC服务器（阻塞运行，直到收到退出信号）
    server->start();
    return 0;
}
