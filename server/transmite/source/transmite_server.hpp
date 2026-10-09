//消息转发子服务的核心实现文件
//本文件实现消息转发子服务(MsgTransmitService)的完整业务逻辑,包含三个核心类:
//  1. TransmiteServiceImpl —— 消息转发Rpc服务的业务实现类,继承自protobuf生成的MsgTransmitService
//     唯一接口GetTransmitTarget负责:补全发送者信息→组装MessageInfo→算出目标用户列表→发MQ留底→响应
//  2. TransmiteServer      —— 消息转发Rpc服务的运行容器,封装Rpc服务器的生命周期
//  3. TransmiteServerBuilder —— 建造者类,用于分步组装TransmiteServer所需的各组成模块
//                              (MQ/MySQL/服务发现/RPC服务器/服务注册)
//
//关键设计说明:
//  - 本服务只做"算目标 + 发MQ留底",不直接推送客户端;实时推送由调用方网关在本地完成
//  - RabbitMQ在这里是"持久化通道"(发给消息存储子服务留底),不在实时推送路径上
//  - 本服务无WebSocket连接状态,可像其他子服务一样多实例横向扩容+RR负载均衡
#include <brpc/server.h>
#include <butil/logging.h>

#include "etcd.hpp"     // 服务注册模块封装
#include "logger.hpp"   // 日志模块封装
#include "rabbitmq.hpp" // RabbitMQ客户端封装(发布消息供存储子服务消费)
#include "channel.hpp"  // 信道管理模块封装(发现并选择用户子服务节点)
#include "utils.hpp"    // 基础工具接口(uuid等)
#include "mysql_chat_session_member.hpp"  // 聊天会话成员表操作封装(Table层)

#include "base.pb.h"  // protobuf框架代码 - 基础协议(MessageContent/MessageInfo等)
#include "user.pb.h"  // protobuf框架代码 - 用户子服务协议(GetUserInfoReq/Rsp)
#include "transmite.pb.h"  // protobuf框架代码 - 本服务协议(MsgTransmitService)

namespace bite_im{
//消息转发Rpc服务业务实现类
//继承自protobuf生成的bite_im::MsgTransmitService基类,重写其全部rpc接口方法
class TransmiteServiceImpl : public bite_im::MsgTransmitService {
    public:
        //构造函数:注入各外部依赖(用户子服务名/信道管理/MySQL/MQ发布参数)
        TransmiteServiceImpl(const std::string &user_service_name,
            const ServiceManager::ptr &channels,
            const std::shared_ptr<odb::core::database> &mysql_client,
            const std::string &exchange_name,
            const std::string &routing_key,
            const MQClient::ptr &mq_client):
            _user_service_name(user_service_name),
            _mm_channels(channels),
            _mysql_session_member_table(std::make_shared<ChatSessionMemeberTable>(mysql_client)),
            _exchange_name(exchange_name),
            _routing_key(routing_key),
            _mq_client(mq_client){}
        ~TransmiteServiceImpl(){}
        //消息转发核心接口:网关收到客户端新消息后通过brpc同步调用本方法
        //处理流程:取参→调用户子服务补发送者→组装MessageInfo→查会话成员得目标列表→发MQ留底→响应
        //注意:本方法不负责向客户端推送,实时推送由调用方网关拿到响应后自行完成
        void GetTransmitTarget(google::protobuf::RpcController* controller,
                       const ::bite_im::NewMessageReq* request,
                       ::bite_im::GetTransmitTargetRsp* response,
                       ::google::protobuf::Closure* done) override {
            //ClosureGuard保证done在作用域结束时被调用(无论正常返回还是异常)
            brpc::ClosureGuard rpc_guard(done);
            //定义错误处理函数,出错时统一填充失败响应
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            //1. 从请求中获取关键信息:请求ID,发送者用户ID,所属会话ID,消息内容
            std::string rid = request->request_id();
            std::string uid = request->user_id();
            std::string chat_ssid = request->chat_session_id();
            std::string client_mid = request->client_message_id();
            const MessageContent &content = request->message();
            if (client_mid.empty() || client_mid.size() > 64) {
                LOG_ERROR("{} - 客户端消息ID无效！", rid);
                return err_response(rid, "客户端消息ID无效!");
            }
            //2. 调用用户子服务获取发送者完整信息(昵称/头像等),用于组装MessageInfo的sender字段
            auto channel = _mm_channels->choose(_user_service_name);
            if (!channel) {
                LOG_ERROR("{}-{} 没有可供访问的用户子服务节点！", rid, _user_service_name);
                return err_response(rid, "没有可供访问的用户子服务节点！");
            }
            UserService_Stub stub(channel.get());
            GetUserInfoReq req;
            GetUserInfoRsp rsp;
            req.set_request_id(rid);
            req.set_user_id(uid);
            brpc::Controller cntl;
            stub.GetUserInfo(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed() == true || rsp.success() == false) {
                LOG_ERROR("{} - 用户子服务调用失败：{}！", request->request_id(), cntl.ErrorText());
                return err_response(request->request_id(), "用户子服务调用失败!");
            }
            //3. 组装完整的MessageInfo:消息ID/会话ID/时间戳/发送者/消息内容
            MessageInfo message;
            // 同一发送者重试同一个 client_message_id 时始终得到相同的服务端消息ID。
            message.set_message_id(stableMessageId(uid, client_mid));
            message.set_client_message_id(client_mid);
            message.set_chat_session_id(chat_ssid);
            message.set_timestamp(time(nullptr));
            message.mutable_sender()->CopyFrom(rsp.user_info());
            message.mutable_message()->CopyFrom(content);
            //4. 查询聊天会话成员表,得到本条消息的所有转发目标用户(包含接收方)
            auto target_list = _mysql_session_member_table->members(chat_ssid);
            //5. 将组装完毕的消息发布到RabbitMQ,待消息存储子服务进行异步持久化(留底)
            //   这一步与实时推送解耦:即便客户端没收到实时推送,下次拉历史消息也能取到
            bool ret = _mq_client->publish(_exchange_name, message.SerializeAsString(), _routing_key);
            if (ret == false) {
                LOG_ERROR("{} - 持久化消息发布失败：{}！", request->request_id(), cntl.ErrorText());
                return err_response(request->request_id(), "持久化消息发布失败：!");
            }
            //6. 组织成功响应:回填组织好的message与目标用户列表,供调用方网关本地推送
            response->set_request_id(rid);
            response->set_success(true);
            response->mutable_message()->CopyFrom(message);
            for (const auto &id : target_list) {
                response->add_target_id_list(id);
            }
        }
    private:
        //用户子服务调用相关信息
        std::string _user_service_name;
        ServiceManager::ptr _mm_channels;

        //聊天会话成员表的操作句柄(Table层,用于查询会话成员确定转发目标)
        ChatSessionMemeberTable::ptr _mysql_session_member_table;

        //消息队列客户端句柄(用于发布消息到exchange,供存储子服务消费持久化)
        std::string _exchange_name;
        std::string _routing_key;
        MQClient::ptr _mq_client;
};

//消息转发Rpc服务运行容器
//封装Rpc服务器及各依赖客户端的生命周期,仅对外暴露start()启动接口
class TransmiteServer {
    public:
        using ptr = std::shared_ptr<TransmiteServer>;
        //构造函数:注入服务发现/服务注册/MySQL/Rpc服务器
        TransmiteServer(
            const std::shared_ptr<odb::core::database> &mysql_client,
            const Discovery::ptr discovery_client,
            const Registry::ptr &registry_client,
            const std::shared_ptr<brpc::Server> &server):
            _service_discoverer(discovery_client),
            _registry_client(registry_client),
            _mysql_client(mysql_client),
            _rpc_server(server){}
        ~TransmiteServer(){}
        //搭建RPC服务器,并启动服务器(阻塞运行直到收到退出信号)
        void start() {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        Discovery::ptr _service_discoverer; //服务发现客户端
        Registry::ptr _registry_client; // 服务注册客户端
        std::shared_ptr<odb::core::database> _mysql_client; //mysql数据库客户端
        std::shared_ptr<brpc::Server> _rpc_server;
};

//消息转发Rpc服务建造者类
//采用建造者模式,分步构造并组装TransmiteServer所需的各组成模块
//使用流程:依次调用make_xxx_object构造各模块 -> make_rpc_server注册服务 -> build()组装出TransmiteServer
class TransmiteServerBuilder {
    public:
        //构造mysql客户端对象(用于查询聊天会话成员表)
        void make_mysql_object(
            const std::string &user,
            const std::string &pswd,
            const std::string &host,
            const std::string &db,
            const std::string &cset,
            int port,
            int conn_pool_count) {
            _mysql_client = ODBFactory::create(user, pswd, host, db, cset, port, conn_pool_count);
        }
        //用于构造服务发现客户端&信道管理对象
        //同时声明关注的用户子服务,并绑定其etcd上下线回调
        void make_discovery_object(const std::string &reg_host,
            const std::string &base_service_name,
            const std::string &user_service_name) {
            _user_service_name = user_service_name;
            _mm_channels = std::make_shared<ServiceManager>();
            _mm_channels->declared(user_service_name);
            LOG_DEBUG("设置用户子服务为需添加管理的子服务：{}", user_service_name);
            auto put_cb = std::bind(&ServiceManager::onServiceOnline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
            auto del_cb = std::bind(&ServiceManager::onServiceOffline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
            _service_discoverer = std::make_shared<Discovery>(reg_host, base_service_name, put_cb, del_cb);
        }
        //用于构造服务注册客户端对象
        //向etcd注册中心注册当前实例(服务名+访问地址)
        void make_registry_object(const std::string &reg_host,
            const std::string &service_name,
            const std::string &access_host) {
            _registry_client = std::make_shared<Registry>(reg_host);
            _registry_client->registry(service_name, access_host);
        }
        //用于构造rabbitmq客户端对象
        //声明exchange/queue/binding,后续用于发布消息供消息存储子服务消费持久化
        void make_mq_object(const std::string &user, 
            const std::string &passwd,
            const std::string &host,
            const std::string &exchange_name,
            const std::string &queue_name,
            const std::string &binding_key) {
            _routing_key = binding_key;
            _exchange_name = exchange_name;
            _mq_client = std::make_shared<MQClient>(user, passwd, host);
            _mq_client->declareComponents(exchange_name, queue_name, binding_key);
        }
        //构造并启动Rpc服务器
        //会先校验所有依赖模块是否就绪,再创建TransmiteServiceImpl并注册到brpc服务器
        void make_rpc_server(uint16_t port, int32_t timeout, uint8_t num_threads) {
            if (!_mq_client) {
                LOG_ERROR("还未初始化消息队列客户端模块！");
                abort();
            }
            if (!_mm_channels) {
                LOG_ERROR("还未初始化信道管理模块！");
                abort();
            }
            if (!_mysql_client) {
                LOG_ERROR("还未初始化Mysql数据库模块！");
                abort();
            }

            _rpc_server = std::make_shared<brpc::Server>();

            TransmiteServiceImpl *transmite_service = new TransmiteServiceImpl(
                _user_service_name, _mm_channels, _mysql_client, _exchange_name, _routing_key, _mq_client);

            int ret = _rpc_server->AddService(transmite_service, 
                brpc::ServiceOwnership::SERVER_OWNS_SERVICE);
            if (ret == -1) {
                LOG_ERROR("添加Rpc服务失败！");
                abort();
            }
            brpc::ServerOptions options;
            options.idle_timeout_sec = timeout;
            options.num_threads = num_threads;
            ret = _rpc_server->Start(port, &options);
            if (ret == -1) {
                LOG_ERROR("服务启动失败！");
                abort();
            }
        }
        //构造Rpc服务器对象
        //最终组装前再次校验服务发现、服务注册、Rpc服务器三模块是否就绪
        TransmiteServer::ptr build() {
            if (!_service_discoverer) {
                LOG_ERROR("还未初始化服务发现模块！");
                abort();
            }
            if (!_registry_client) {
                LOG_ERROR("还未初始化服务注册模块！");
                abort();
            }
            if (!_rpc_server) {
                LOG_ERROR("还未初始化RPC服务器模块！");
                abort();
            }
            TransmiteServer::ptr server = std::make_shared<TransmiteServer>(
                _mysql_client, _service_discoverer, _registry_client, _rpc_server);
            return server;
        }
    private:
        std::string _user_service_name;
        ServiceManager::ptr _mm_channels;
        Discovery::ptr _service_discoverer;
        
        std::string _routing_key;
        std::string _exchange_name;
        MQClient::ptr _mq_client;

        Registry::ptr _registry_client; // 服务注册客户端
        std::shared_ptr<odb::core::database> _mysql_client; //mysql数据库客户端
        std::shared_ptr<brpc::Server> _rpc_server;
};
}
