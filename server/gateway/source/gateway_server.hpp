//网关子服务核心实现文件
//GatewayServer 是客户端访问后端各微服务的统一入口,职责包括:
//  1. HTTP(9000) 请求响应:解析 protobuf body → 鉴权 → 选路 → 转发 brpc → 序列化响应
//  2. WebSocket(9001) 长连接管理:握手 → 第一条消息做身份绑定 → ping 心跳保活 → 断开清理 Redis
//  3. 业务推送:好友申请/处理结果/会话创建/新消息等通过 WebSocket 推送给目标用户
//  4. 服务发现 + 负载均衡:基于 etcd 维护下游子服务节点,RR 选择 brpc channel
//所有 HTTP 处理函数遵循统一 8 步流程:body 反序列化 → 鉴权(注入 user_id) →
//   服务发现 choose → stub 构造 → RPC 调用 → 失败处理 → 响应序列化 → 可选推送
//注意:Content-Type 故意拼写为 "application/x-protbuf"(缺 'o')以兼容客户端
#include "data_redis.hpp"      // redis数据管理客户端封装
#include "etcd.hpp"     // 服务注册模块封装
#include "logger.hpp"   // 日志模块封装
#include "channel.hpp"  // 信道管理模块封装

#include "connection.hpp"

#include "user.pb.h"  // protobuf框架代码
#include "base.pb.h"  // protobuf框架代码
#include "file.pb.h"  // protobuf框架代码
#include "friend.pb.h"  // protobuf框架代码
#include "gateway.pb.h"  // protobuf框架代码
#include "message.pb.h"  // protobuf框架代码
#include "speech.pb.h"  // protobuf框架代码
#include "transmite.pb.h"  // protobuf框架代码
#include "notify.pb.h"

#include "httplib.h"


namespace bite_im{
    //============= 客户端 HTTP 路由常量 =============
    //--- 用户管理类(转发至 UserService) ---
    #define GET_PHONE_VERIFY_CODE   "/service/user/get_phone_verify_code"
    #define USERNAME_REGISTER       "/service/user/username_register"
    #define USERNAME_LOGIN          "/service/user/username_login"
    #define PHONE_REGISTER          "/service/user/phone_register"
    #define PHONE_LOGIN             "/service/user/phone_login"
    #define GET_USERINFO            "/service/user/get_user_info"
    #define SET_USER_AVATAR         "/service/user/set_avatar"
    #define SET_USER_NICKNAME       "/service/user/set_nickname"
    #define SET_USER_DESC           "/service/user/set_description"
    #define SET_USER_PHONE          "/service/user/set_phone"
    //--- 好友管理类(转发至 FriendService) ---
    #define FRIEND_GET_LIST         "/service/friend/get_friend_list"
    #define FRIEND_APPLY            "/service/friend/add_friend_apply"
    #define FRIEND_APPLY_PROCESS    "/service/friend/add_friend_process"
    #define FRIEND_REMOVE           "/service/friend/remove_friend"
    #define FRIEND_SEARCH          "/service/friend/search_friend"
    #define FRIEND_GET_PENDING_EV  "/service/friend/get_pending_friend_events"
    //--- 会话管理类(转发至 FriendService) ---
    #define CSS_GET_LIST            "/service/friend/get_chat_session_list"
    #define CSS_CREATE             "/service/friend/create_chat_session"
    #define CSS_GET_MEMBER         "/service/friend/get_chat_session_member"
    //--- 消息存储类(转发至 MsgStorageService) ---
    #define MSG_GET_RANGE           "/service/message_storage/get_history"
    #define MSG_GET_RECENT          "/service/message_storage/get_recent"
    #define MSG_KEY_SEARCH          "/service/message_storage/search_history"
    //--- 消息转发类(转发至 MsgTransmitService,触发 WebSocket 推送) ---
    #define NEW_MESSAGE             "/service/message_transmit/new_message"
    //--- 文件管理类(转发至 FileService) ---
    #define FILE_GET_SINGLE         "/service/file/get_single_file"
    #define FILE_GET_MULTI         "/service/file/get_multi_file"
    #define FILE_PUT_SINGLE         "/service/file/put_single_file"
    #define FILE_PUT_MULTI         "/service/file/put_multi_file"
    //--- 语音识别类(转发至 SpeechService) ---
    #define SPEECH_RECOGNITION      "/service/speech/recognition"

    //网关主类:同时承载 HTTP 与 WebSocket 两种协议,转发请求至下游子服务
    class GatewayServer {
        public:
            using ptr = std::shared_ptr<GatewayServer>;
            //构造函数:注入依赖 + 初始化 HTTP/WebSocket 双服务
            //参数说明:
            //  websocket_port/http_port:对外监听端口(默认 9001/9000)
            //  redis_client:Redis 客户端,用于会话 Session/在线状态 Status 读写
            //  channels:ServiceManager,持有各下游子服务的 brpc channel 池(RR 选路)
            //  service_discoverer:etcd 服务发现对象,负责感知节点上下线
            //  其余 *_service_name:各下游子服务在 etcd 中的路径,用于 choose 时按名查找
            GatewayServer(
                int websocket_port,
                int http_port,
                const std::shared_ptr<sw::redis::Redis> &redis_client,
                const ServiceManager::ptr &channels,
                const Discovery::ptr &service_discoverer,
                const std::string user_service_name,
                const std::string file_service_name,
                const std::string speech_service_name,
                const std::string message_service_name,
                const std::string transmite_service_name,
                const std::string friend_service_name)
                :_redis_session(std::make_shared<Session>(redis_client)),
                _redis_status(std::make_shared<Status>(redis_client)),
                _mm_channels(channels),
                _service_discoverer(service_discoverer),
                _user_service_name(user_service_name),
                _file_service_name(file_service_name),
                _speech_service_name(speech_service_name),
                _message_service_name(message_service_name),
                _transmite_service_name(transmite_service_name),
                _friend_service_name(friend_service_name),
                _connections(std::make_shared<Connection>()){
                
                //--- WebSocket 服务初始化(websocketpp + boost::asio) ---
                _ws_server.set_access_channels(websocketpp::log::alevel::none);  // 关闭访问日志,避免噪声
                _ws_server.init_asio();                                            // 接入 boost::asio io_loop
                _ws_server.set_open_handler(std::bind(&GatewayServer::onOpen, this, std::placeholders::_1));     // 握手完成回调
                _ws_server.set_close_handler(std::bind(&GatewayServer::onClose, this, std::placeholders::_1));   // 连接断开回调(负责清理 Redis)
                auto wscb = std::bind(&GatewayServer::onMessage, this, 
                    std::placeholders::_1, std::placeholders::_2);
                _ws_server.set_message_handler(wscb);                              // 收到消息回调(第一条消息做身份绑定)
                _ws_server.set_reuse_addr(true);                                    // 端口复用,避免 TIME_WAIT 卡住重启
                _ws_server.listen(websocket_port);                                  // TCP 监听 9001
                _ws_server.start_accept();                                         // 开始接受握手

                //--- HTTP 路由注册:每条路径绑定到对应处理函数 ---
                _http_server.Post(GET_PHONE_VERIFY_CODE  , (httplib::Server::Handler)std::bind(&GatewayServer::GetPhoneVerifyCode         , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(USERNAME_REGISTER      , (httplib::Server::Handler)std::bind(&GatewayServer::UserRegister               , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(USERNAME_LOGIN         , (httplib::Server::Handler)std::bind(&GatewayServer::UserLogin                  , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(PHONE_REGISTER         , (httplib::Server::Handler)std::bind(&GatewayServer::PhoneRegister              , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(PHONE_LOGIN            , (httplib::Server::Handler)std::bind(&GatewayServer::PhoneLogin                 , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(GET_USERINFO           , (httplib::Server::Handler)std::bind(&GatewayServer::GetUserInfo                , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(SET_USER_AVATAR        , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserAvatar              , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(SET_USER_NICKNAME      , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserNickname            , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(SET_USER_DESC          , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserDescription         , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(SET_USER_PHONE         , (httplib::Server::Handler)std::bind(&GatewayServer::SetUserPhoneNumber         , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FRIEND_GET_LIST        , (httplib::Server::Handler)std::bind(&GatewayServer::GetFriendList              , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FRIEND_APPLY           , (httplib::Server::Handler)std::bind(&GatewayServer::FriendAdd                  , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FRIEND_APPLY_PROCESS   , (httplib::Server::Handler)std::bind(&GatewayServer::FriendAddProcess           , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FRIEND_REMOVE          , (httplib::Server::Handler)std::bind(&GatewayServer::FriendRemove               , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FRIEND_SEARCH          , (httplib::Server::Handler)std::bind(&GatewayServer::FriendSearch               , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FRIEND_GET_PENDING_EV  , (httplib::Server::Handler)std::bind(&GatewayServer::GetPendingFriendEventList  , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(CSS_GET_LIST           , (httplib::Server::Handler)std::bind(&GatewayServer::GetChatSessionList         , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(CSS_CREATE             , (httplib::Server::Handler)std::bind(&GatewayServer::ChatSessionCreate          , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(CSS_GET_MEMBER         , (httplib::Server::Handler)std::bind(&GatewayServer::GetChatSessionMember       , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(MSG_GET_RANGE          , (httplib::Server::Handler)std::bind(&GatewayServer::GetHistoryMsg              , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(MSG_GET_RECENT         , (httplib::Server::Handler)std::bind(&GatewayServer::GetRecentMsg               , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(MSG_KEY_SEARCH         , (httplib::Server::Handler)std::bind(&GatewayServer::MsgSearch                  , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(NEW_MESSAGE            , (httplib::Server::Handler)std::bind(&GatewayServer::NewMessage                 , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FILE_GET_SINGLE        , (httplib::Server::Handler)std::bind(&GatewayServer::GetSingleFile              , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FILE_GET_MULTI         , (httplib::Server::Handler)std::bind(&GatewayServer::GetMultiFile               , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FILE_PUT_SINGLE        , (httplib::Server::Handler)std::bind(&GatewayServer::PutSingleFile              , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(FILE_PUT_MULTI         , (httplib::Server::Handler)std::bind(&GatewayServer::PutMultiFile               , this, std::placeholders::_1, std::placeholders::_2));
                _http_server.Post(SPEECH_RECOGNITION     , (httplib::Server::Handler)std::bind(&GatewayServer::SpeechRecognition          , this, std::placeholders::_1, std::placeholders::_2));
                //--- HTTP 服务独立线程启动:与 WebSocket 主线程并行,避免互相阻塞 ---
                _http_thread = std::thread([this, http_port](){
                    _http_server.listen("0.0.0.0", http_port);                  // 阻塞 listen,因此必须独立线程
                });
                _http_thread.detach();                                          // 与主线程分离,随进程生命周期
            }
        //启动网关:阻塞当前线程跑 WebSocket 事件循环
        //(HTTP 服务已在构造函数的独立线程中运行)
        void start() {
            _ws_server.run();
        }
        private:
            //WebSocket 握手完成回调:连接已建立但身份未绑定,仅记日志
            //(真正身份识别在 onMessage 收到第一条 ClientAuthenticationReq 时完成)
            void onOpen(websocketpp::connection_hdl hdl) {
                LOG_DEBUG("websocket长连接建立成功 {}", (size_t)_ws_server.get_con_from_hdl(hdl).get());
            }
            void onClose(websocketpp::connection_hdl hdl) {
                //长连接断开时做的清理工作
                //0. 通过连接对象，获取对应的用户ID与登录会话ID
                auto conn = _ws_server.get_con_from_hdl(hdl);
                std::string uid, ssid;
                bool ret = _connections->client(conn, uid, ssid);
                if (ret == false) {
                    LOG_WARN("长连接断开，未找到长连接对应的客户端信息！");
                    return ;
                }
                //1. 移除登录会话信息
                _redis_session->remove(ssid);
                //2. 移除登录状态信息
                _redis_status->remove(uid);
                //3. 移除长连接管理数据
                _connections->remove(conn);
                LOG_DEBUG("用户 {} 长连接断开，清理缓存数据，connection={}", uid, (size_t)conn.get());
            }
            //心跳保活:每 60 秒发一个 WebSocket ping 帧,防止中间路由回收空闲连接
            //连接已关闭则停止递归(递归靠 set_timer 自驱动,非独立定时器线程)
            void keepAlive(server_t::connection_ptr conn) {
                if (!conn || conn->get_state() != websocketpp::session::state::value::open) {
                    LOG_DEBUG("非正常连接状态，结束连接保活");
                    return;
                }
                conn->ping("");
                _ws_server.set_timer(60000, std::bind(&GatewayServer::keepAlive, this, conn));
            }
            //WebSocket 收消息回调:专门处理第一条消息做身份绑定
            //协议规定:客户端握手成功后,第一条消息必须发 ClientAuthenticationReq(含 session_id)
            //鉴权通过后将 (conn ↔ uid,ssid) 双向映射塞入 _connections,此后该连接可收推送
            void onMessage(websocketpp::connection_hdl hdl, server_t::message_ptr msg) {
                //收到第一条消息后，根据消息中的会话ID进行身份识别，将客户端长连接添加管理
                //1. 取出长连接对应的连接对象
                auto conn = _ws_server.get_con_from_hdl(hdl);
                //2. 针对消息内容进行反序列化 -- ClientAuthenticationReq -- 提取登录会话ID
                ClientAuthenticationReq request;
                bool ret = request.ParseFromString(msg->get_payload());
                if (ret == false) {
                    LOG_ERROR("长连接身份识别失败：正文反序列化失败！");
                    _ws_server.close(hdl, websocketpp::close::status::unsupported_data, "正文反序列化失败!");
                    return;
                }
                //3. 在会话信息缓存中，查找会话信息 
                std::string ssid = request.session_id();
                auto uid = _redis_session->uid(ssid);
                //4. 会话信息不存在则关闭连接
                if (!uid) {
                    LOG_ERROR("长连接身份识别失败：未找到会话信息！");
                    _ws_server.close(hdl, websocketpp::close::status::unsupported_data, "未找到会话信息!");
                    return;
                }
                //5. 会话信息存在，则添加长连接管理
                _connections->insert(conn, *uid, ssid);
                LOG_DEBUG("新增长连接管理：用户={}，connection={}", *uid, (size_t)conn.get());
                keepAlive(conn);
            }
            //--- 用户管理类 handler(均转发至 UserService) ---
            //获取短信验证码:[无鉴权]透传至 UserService.GetPhoneVerifyCode
            void GetPhoneVerifyCode(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                PhoneVerifyCodeReq req;
                PhoneVerifyCodeRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取短信验证码请求正文反序列化失败！");
                    return err_response("获取短信验证码请求正文反序列化失败！");
                }
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetPhoneVerifyCode(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //用户名注册:[无鉴权]透传至 UserService.UserRegister
            void UserRegister(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                UserRegisterReq req;
                UserRegisterRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户名注册请求正文反序列化失败！");
                    return err_response("用户名注册请求正文反序列化失败！");
                }
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.UserRegister(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //用户名登录:[无鉴权]透传至 UserService.UserLogin,返回 login_session_id
            void UserLogin(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                UserLoginReq req;
                UserLoginRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户登录请求正文反序列化失败！");
                    return err_response("用户登录请求正文反序列化失败！");
                }
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.UserLogin(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //手机号注册:[无鉴权]透传至 UserService.PhoneRegister
            void PhoneRegister(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                PhoneRegisterReq req;
                PhoneRegisterRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("手机号注册请求正文反序列化失败！");
                    return err_response("手机号注册请求正文反序列化失败！");
                }
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.PhoneRegister(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //手机号登录:[无鉴权]透传至 UserService.PhoneLogin,返回 login_session_id
            void PhoneLogin(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                PhoneLoginReq req;
                PhoneLoginRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("手机号登录请求正文反序列化失败！");
                    return err_response("手机号登录请求正文反序列化失败！");
                }
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.PhoneLogin(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            void GetUserInfo(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                GetUserInfoReq req;
                GetUserInfoRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取用户信息请求正文反序列化失败！");
                    return err_response("获取用户信息请求正文反序列化失败！");
                }
                //2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetUserInfo(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //设置用户头像:[鉴权]注入 uid 后转发至 UserService.SetUserAvatar
            void SetUserAvatar(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                SetUserAvatarReq req;
                SetUserAvatarRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户头像设置请求正文反序列化失败！");
                    return err_response("用户头像设置请求正文反序列化失败！");
                }
                //2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.SetUserAvatar(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //设置用户昵称:[鉴权]注入 uid 后转发至 UserService.SetUserNickname
            void SetUserNickname(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                SetUserNicknameReq req;
                SetUserNicknameRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户昵称设置请求正文反序列化失败！");
                    return err_response("用户昵称设置请求正文反序列化失败！");
                }
                //2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.SetUserNickname(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //设置用户签名:[鉴权]注入 uid 后转发至 UserService.SetUserDescription
            void SetUserDescription(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                SetUserDescriptionReq req;
                SetUserDescriptionRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户签名设置请求正文反序列化失败！");
                    return err_response("用户签名设置请求正文反序列化失败！");
                }
                //2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.SetUserDescription(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //设置用户手机号:[鉴权]注入 uid 后转发至 UserService.SetUserPhoneNumber
            void SetUserPhoneNumber(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                SetUserPhoneNumberReq req;
                SetUserPhoneNumberRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户手机号设置请求正文反序列化失败！");
                    return err_response("用户手机号设置请求正文反序列化失败！");
                }
                //2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.SetUserPhoneNumber(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return err_response("用户子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //--- 好友管理类 handler(均转发至 FriendService) ---
            //获取好友列表:[鉴权]注入 uid 后转发至 FriendService.GetFriendList
            void GetFriendList(const httplib::Request &request, httplib::Response &response) {
                //1. 取出http请求正文，将正文进行反序列化
                GetFriendListReq req;
                GetFriendListRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取好友列表请求正文反序列化失败！");
                    return err_response("获取好友列表请求正文反序列化失败！");
                }
                //2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                //2. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetFriendList(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                //3. 得到用户子服务的响应后，将响应内容进行序列化作为http响应正文
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }

            //内部辅助:根据 uid 反查用户信息(用于推送通知时构造 user_info 字段)
            //返回空 shared_ptr 表示调用失败(节点未找到或 RPC 失败)
            std::shared_ptr<GetUserInfoRsp> _GetUserInfo(const std::string &rid, const std::string &uid) {
                GetUserInfoReq req;
                auto rsp = std::make_shared<GetUserInfoRsp>();
                req.set_request_id(rid);
                req.set_user_id(uid);
                //2. 将请求转发给用户子服务进行业务处理
                auto channel = _mm_channels->choose(_user_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return std::shared_ptr<GetUserInfoRsp>();
                }
                bite_im::UserService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetUserInfo(&cntl, &req, rsp.get(), nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 用户子服务调用失败！", req.request_id());
                    return std::shared_ptr<GetUserInfoRsp>();
                }
                return rsp;
            }
            //好友申请:[鉴权]转发至 FriendService.FriendAdd
            //成功后通过 WebSocket 向被申请人推送 FRIEND_ADD_APPLY_NOTIFY(含申请人用户信息)
            void FriendAdd(const httplib::Request &request, httplib::Response &response) {
                // 好友申请的业务处理中，好友子服务其实只是在数据库创建了申请事件
                // 网关需要做的事情：当好友子服务将业务处理完毕后，如果处理是成功的--需要通知被申请方
                // 1. 正文的反序列化，提取关键要素：登录会话ID
                FriendAddReq req;
                FriendAddRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("申请好友请求正文反序列化失败！");
                    return err_response("申请好友请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.FriendAdd(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 4. 若业务处理成功 --- 且获取被申请方长连接成功，则向被申请放进行好友申请事件通知
                auto conn = _connections->connection(req.respondent_id());
                if (rsp.success() && conn) {
                    LOG_DEBUG("找到被申请人 {} 长连接，对其进行好友申请通知", req.respondent_id());
                    auto user_rsp = _GetUserInfo(req.request_id(), *uid);
                    if (!user_rsp) {
                        LOG_ERROR("{} 获取当前客户端用户信息失败！", req.request_id());
                        return err_response("获取当前客户端用户信息失败！");
                    }
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::FRIEND_ADD_APPLY_NOTIFY);
                    notify.mutable_friend_add_apply()->mutable_user_info()->CopyFrom(user_rsp->user_info());
                    conn->send(notify.SerializeAsString(), websocketpp::frame::opcode::value::binary);
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //好友申请处理:[鉴权]转发至 FriendService.FriendAddProcess
            //成功后向申请人推送 FRIEND_ADD_PROCESS_NOTIFY;
            //若同意则同时给双方推送 CHAT_SESSION_CREATE_NOTIFY(单聊会话)
            void FriendAddProcess(const httplib::Request &request, httplib::Response &response) {
                //好友申请的处理-----
                FriendAddProcessReq req;
                FriendAddProcessRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("好友申请处理请求正文反序列化失败！");
                    return err_response("好友申请处理请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.FriendAddProcess(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                
                if (rsp.success()) {
                    auto process_user_rsp = _GetUserInfo(req.request_id(), *uid);
                    if (!process_user_rsp) {
                        LOG_ERROR("{} 获取用户信息失败！", req.request_id());
                        return err_response("获取用户信息失败！");
                    }
                    auto apply_user_rsp = _GetUserInfo(req.request_id(), req.apply_user_id());
                    if (!process_user_rsp) {
                        LOG_ERROR("{} 获取用户信息失败！", req.request_id());
                        return err_response("获取用户信息失败！");
                    }
                    auto process_conn = _connections->connection(*uid);
                    if (process_conn) LOG_DEBUG("找到处理人的长连接！");
                    else LOG_DEBUG("未找到处理人的长连接！");
                    auto apply_conn = _connections->connection(req.apply_user_id());
                    if (apply_conn) LOG_DEBUG("找到申请人的长连接！");
                    else LOG_DEBUG("未找到申请人的长连接！");
                    //4. 将处理结果给申请人进行通知
                    if (apply_conn) {
                        NotifyMessage notify;
                        notify.set_notify_type(NotifyType::FRIEND_ADD_PROCESS_NOTIFY);
                        auto process_result = notify.mutable_friend_process_result();
                        process_result->mutable_user_info()->CopyFrom(process_user_rsp->user_info());
                        process_result->set_agree(req.agree());
                        apply_conn->send(notify.SerializeAsString(), 
                            websocketpp::frame::opcode::value::binary);
                        LOG_DEBUG("对申请人进行申请处理结果通知！");
                    }
                    //5. 若处理结果是同意 --- 会伴随着单聊会话的创建 -- 因此需要对双方进行会话创建的通知
                    if (req.agree() && apply_conn) { //对申请人的通知---会话信息就是处理人信息
                        NotifyMessage notify;
                        notify.set_notify_type(NotifyType::CHAT_SESSION_CREATE_NOTIFY);
                        auto chat_session = notify.mutable_new_chat_session_info();
                        chat_session->mutable_chat_session_info()->set_single_chat_friend_id(*uid);
                        chat_session->mutable_chat_session_info()->set_chat_session_id(rsp.new_session_id());
                        chat_session->mutable_chat_session_info()->set_chat_session_name(process_user_rsp->user_info().nickname());
                        chat_session->mutable_chat_session_info()->set_avatar(process_user_rsp->user_info().avatar());
                        apply_conn->send(notify.SerializeAsString(), websocketpp::frame::opcode::value::binary);
                        LOG_DEBUG("对申请人进行会话创建通知！");
                    }
                    if (req.agree() && process_conn) { //对处理人的通知 --- 会话信息就是申请人信息
                        NotifyMessage notify;
                        notify.set_notify_type(NotifyType::CHAT_SESSION_CREATE_NOTIFY);
                        auto chat_session = notify.mutable_new_chat_session_info();
                        chat_session->mutable_chat_session_info()->set_single_chat_friend_id(req.apply_user_id());
                        chat_session->mutable_chat_session_info()->set_chat_session_id(rsp.new_session_id());
                        chat_session->mutable_chat_session_info()->set_chat_session_name(apply_user_rsp->user_info().nickname());
                        chat_session->mutable_chat_session_info()->set_avatar(apply_user_rsp->user_info().avatar());
                        process_conn->send(notify.SerializeAsString(), websocketpp::frame::opcode::value::binary);
                        LOG_DEBUG("对处理人进行会话创建通知！");
                    }
                }
                //6. 对客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //删除好友:[鉴权]转发至 FriendService.FriendRemove
            //成功后通过 WebSocket 向被删除人推送 FRIEND_REMOVE_NOTIFY
            void FriendRemove(const httplib::Request &request, httplib::Response &response) {
                // 1. 正文的反序列化，提取关键要素：登录会话ID
                FriendRemoveReq req;
                FriendRemoveRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("删除好友请求正文反序列化失败！");
                    return err_response("删除好友请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.FriendRemove(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 4. 若业务处理成功 --- 且获取被申请方长连接成功，则向被申请放进行好友申请事件通知
                auto conn = _connections->connection(req.peer_id());
                if (rsp.success() && conn) {
                    LOG_ERROR("对被删除人 {} 进行好友删除通知！", req.peer_id());
                    NotifyMessage notify;
                    notify.set_notify_type(NotifyType::FRIEND_REMOVE_NOTIFY);
                    notify.mutable_friend_remove()->set_user_id(*uid);
                    conn->send(notify.SerializeAsString(), websocketpp::frame::opcode::value::binary);
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //搜索用户:[鉴权]转发至 FriendService.FriendSearch(底层走 ES)
            void FriendSearch(const httplib::Request &request, httplib::Response &response) {
                FriendSearchReq req;
                FriendSearchRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("用户搜索请求正文反序列化失败！");
                    return err_response("用户搜索请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.FriendSearch(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //获取待处理好友申请事件列表:[鉴权]转发至 FriendService.GetPendingFriendEventList
            void GetPendingFriendEventList(const httplib::Request &request, httplib::Response &response) {
                GetPendingFriendEventListReq req;
                GetPendingFriendEventListRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取待处理好友申请请求正文反序列化失败！");
                    return err_response("获取待处理好友申请请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetPendingFriendEventList(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //--- 会话管理类 handler(均转发至 FriendService) ---
            //获取聊天会话列表:[鉴权]转发至 FriendService.GetChatSessionList
            void GetChatSessionList(const httplib::Request &request, httplib::Response &response) {
                GetChatSessionListReq req;
                GetChatSessionListRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取聊天会话列表请求正文反序列化失败！");
                    return err_response("获取聊天会话列表请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetChatSessionList(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //获取聊天会话成员列表:[鉴权]转发至 FriendService.GetChatSessionMember
            void GetChatSessionMember(const httplib::Request &request, httplib::Response &response) {
                GetChatSessionMemberReq req;
                GetChatSessionMemberRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取聊天会话成员请求正文反序列化失败！");
                    return err_response("获取聊天会话成员请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetChatSessionMember(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //创建聊天会话(群聊):[鉴权]转发至 FriendService.ChatSessionCreate
            //成功后遍历所有群成员,通过 WebSocket 推送 CHAT_SESSION_CREATE_NOTIFY
            //注意:响应给发起人的 rsp 已 clear_chat_session_info,只在推送里带会话信息
            void ChatSessionCreate(const httplib::Request &request, httplib::Response &response) {
                ChatSessionCreateReq req;
                ChatSessionCreateRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("创建聊天会话请求正文反序列化失败！");
                    return err_response("创建聊天会话请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发给好友子服务进行业务处理
                auto channel = _mm_channels->choose(_friend_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FriendService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.ChatSessionCreate(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 好友子服务调用失败！", req.request_id());
                    return err_response("好友子服务调用失败！");
                }
                // 4. 若业务处理成功 --- 且获取被申请方长连接成功，则向被申请放进行好友申请事件通知
                if (rsp.success()){
                    for (int i = 0; i < req.member_id_list_size(); i++) {
                        auto conn = _connections->connection(req.member_id_list(i));
                        if (!conn) { 
                            LOG_DEBUG("未找到群聊成员 {} 长连接", req.member_id_list(i));
                            continue;
                        }
                        NotifyMessage notify;
                        notify.set_notify_type(NotifyType::CHAT_SESSION_CREATE_NOTIFY);
                        auto chat_session = notify.mutable_new_chat_session_info();
                        chat_session->mutable_chat_session_info()->CopyFrom(rsp.chat_session_info());
                        conn->send(notify.SerializeAsString(), websocketpp::frame::opcode::value::binary);
                        LOG_DEBUG("对群聊成员 {} 进行会话创建通知", req.member_id_list(i));
                    }
                }
                // 5. 向客户端进行响应
                rsp.clear_chat_session_info();
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //--- 消息存储类 handler(均转发至 MsgStorageService) ---
            //获取区间历史消息:[鉴权]转发至 MsgStorageService.GetHistoryMsg
            void GetHistoryMsg(const httplib::Request &request, httplib::Response &response) {
                GetHistoryMsgReq req;
                GetHistoryMsgRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取区间消息请求正文反序列化失败！");
                    return err_response("获取区间消息请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至消息存储子服务进行业务处理
                auto channel = _mm_channels->choose(_message_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::MsgStorageService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetHistoryMsg(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 消息存储子服务调用失败！", req.request_id());
                    return err_response("消息存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //获取最近 N 条消息:[鉴权]转发至 MsgStorageService.GetRecentMsg
            void GetRecentMsg(const httplib::Request &request, httplib::Response &response) {
                GetRecentMsgReq req;
                GetRecentMsgRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("获取最近消息请求正文反序列化失败！");
                    return err_response("获取最近消息请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至消息存储子服务进行业务处理
                auto channel = _mm_channels->choose(_message_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::MsgStorageService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetRecentMsg(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 消息存储子服务调用失败！", req.request_id());
                    return err_response("消息存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //消息关键字搜索:[鉴权]转发至 MsgStorageService.MsgSearch(底层走 ES)
            void MsgSearch(const httplib::Request &request, httplib::Response &response) {
                MsgSearchReq req;
                MsgSearchRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("消息搜索请求正文反序列化失败！");
                    return err_response("消息搜索请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至消息存储子服务进行业务处理
                auto channel = _mm_channels->choose(_message_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::MsgStorageService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.MsgSearch(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 消息存储子服务调用失败！", req.request_id());
                    return err_response("消息存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //--- 文件管理类 handler(均转发至 FileService) ---
            //下载单个文件:[鉴权]转发至 FileService.GetSingleFile
            void GetSingleFile(const httplib::Request &request, httplib::Response &response) {
                GetSingleFileReq req;
                GetSingleFileRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("单文件下载请求正文反序列化失败！");
                    return err_response("单文件下载请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至文件存储子服务进行业务处理
                auto channel = _mm_channels->choose(_file_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FileService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetSingleFile(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 文件存储子服务调用失败！", req.request_id());
                    return err_response("文件存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //批量下载文件:[鉴权]转发至 FileService.GetMultiFile
            void GetMultiFile(const httplib::Request &request, httplib::Response &response) {
                GetMultiFileReq req;
                GetMultiFileRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("批量文件下载请求正文反序列化失败！");
                    return err_response("批量文件下载请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至文件存储子服务进行业务处理
                auto channel = _mm_channels->choose(_file_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FileService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetMultiFile(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 文件存储子服务调用失败！", req.request_id());
                    return err_response("文件存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //上传单个文件:[鉴权]转发至 FileService.PutSingleFile
            void PutSingleFile(const httplib::Request &request, httplib::Response &response) {
                PutSingleFileReq req;
                PutSingleFileRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("单文件上传请求正文反序列化失败！");
                    return err_response("单文件上传请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至文件存储子服务进行业务处理
                auto channel = _mm_channels->choose(_file_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FileService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.PutSingleFile(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 文件存储子服务调用失败！", req.request_id());
                    return err_response("文件存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //批量上传文件:[鉴权]转发至 FileService.PutMultiFile
            void PutMultiFile(const httplib::Request &request, httplib::Response &response) {
                PutMultiFileReq req;
                PutMultiFileRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("批量文件上传请求正文反序列化失败！");
                    return err_response("批量文件上传请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至文件存储子服务进行业务处理
                auto channel = _mm_channels->choose(_file_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::FileService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.PutMultiFile(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 文件存储子服务调用失败！", req.request_id());
                    return err_response("文件存储子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
            //--- 语音识别类 handler ---
            //语音转文字:[鉴权]转发至 SpeechService.SpeechRecognition
            void SpeechRecognition(const httplib::Request &request, httplib::Response &response) {
                LOG_DEBUG("收到语音转文字请求！");
                SpeechRecognitionReq req;
                SpeechRecognitionRsp rsp;
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("语音识别请求正文反序列化失败！");
                    return err_response("语音识别请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至语音识别子服务进行业务处理
                auto channel = _mm_channels->choose(_speech_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::SpeechService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.SpeechRecognition(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 语音识别子服务调用失败！", req.request_id());
                    return err_response("语音识别子服务调用失败！");
                }
                // 5. 向客户端进行响应
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }

            //--- 消息转发类 handler ---
            //发送新消息:[鉴权]转发至 MsgTransmitService.GetTransmitTarget
            //成功后遍历目标用户列表,通过 WebSocket 向每个在线目标推送 CHAT_MESSAGE_NOTIFY
            //(不通知自己;离线目标跳过,靠客户端下次拉取历史消息补偿)
            void NewMessage(const httplib::Request &request, httplib::Response &response) {
                NewMessageReq req;
                NewMessageRsp rsp;//这是给客户端的响应
                GetTransmitTargetRsp target_rsp;//这是请求子服务的响应
                auto err_response = [&req, &rsp, &response](const std::string &errmsg) -> void {
                    rsp.set_success(false);
                    rsp.set_errmsg(errmsg);
                    rsp.set_client_message_id(req.client_message_id());
                    rsp.set_delivery_status(DeliveryStatus::FAILED);
                    response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
                };
                bool ret = req.ParseFromString(request.body);
                if (ret == false) {
                    LOG_ERROR("新消息请求正文反序列化失败！");
                    return err_response("新消息请求正文反序列化失败！");
                }
                // 2. 客户端身份识别与鉴权
                std::string ssid = req.session_id();
                auto uid = _redis_session->uid(ssid);
                if (!uid) {
                    LOG_ERROR("{} 获取登录会话关联用户信息失败！", req.request_id());
                    return err_response("获取登录会话关联用户信息失败！");
                }
                req.set_user_id(*uid);
                // 3. 将请求转发至消息转发子服务进行业务处理
                auto channel = _mm_channels->choose(_transmite_service_name);
                if (!channel) {
                    LOG_ERROR("{} 未找到可提供业务处理的用户子服务节点！", req.request_id());
                    return err_response("未找到可提供业务处理的用户子服务节点！");
                }
                bite_im::MsgTransmitService_Stub stub(channel.get());
                brpc::Controller cntl;
                stub.GetTransmitTarget(&cntl, &req, &target_rsp, nullptr);
                if (cntl.Failed()) {
                    LOG_ERROR("{} 消息转发子服务调用失败！", req.request_id());
                    return err_response("消息转发子服务调用失败！");
                }
                // 4. 若业务处理成功 --- 且获取被申请方长连接成功，则向被申请放进行好友申请事件通知
                uint32_t delivered_target_count = 0;
                if (target_rsp.success()){
                    for (int i = 0; i < target_rsp.target_id_list_size(); i++) {
                        std::string notify_uid = target_rsp.target_id_list(i);
                        if (notify_uid == *uid) continue; //不通知自己
                        auto conn = _connections->connection(notify_uid);
                        if (!conn) { continue;}
                        NotifyMessage notify;
                        notify.set_notify_type(NotifyType::CHAT_MESSAGE_NOTIFY);
                        auto msg_info = notify.mutable_new_message_info();
                        msg_info->mutable_message_info()->CopyFrom(target_rsp.message());
                        try {
                            conn->send(notify.SerializeAsString(), websocketpp::frame::opcode::value::binary);
                            ++delivered_target_count;
                        } catch (const std::exception &e) {
                            LOG_WARN("{} 消息实时推送失败，目标用户={}：{}", req.request_id(), notify_uid, e.what());
                        }
                    }
                }
                // 5. 向客户端进行响应
                rsp.set_request_id(req.request_id());
                rsp.set_success(target_rsp.success());
                rsp.set_errmsg(target_rsp.errmsg());
                rsp.set_client_message_id(req.client_message_id());
                rsp.set_message_id(target_rsp.message().message_id());
                rsp.set_delivered_target_count(delivered_target_count);
                rsp.set_delivery_status(target_rsp.success()
                    ? (delivered_target_count > 0 ? DeliveryStatus::DELIVERED : DeliveryStatus::SENT)
                    : DeliveryStatus::FAILED);
                response.set_content(rsp.SerializeAsString(), "application/x-protbuf");
            }
        private:
            //--- Redis 状态层(鉴权/在线判断的核心) ---
            Session::ptr _redis_session;    // ssid → uid 映射(登录会话表)
            Status::ptr _redis_status;       // uid → 在线标记表

            //--- 下游子服务名(从 etcd 配置加载,choose() 按此查 ServiceChannel) ---
            std::string _user_service_name;
            std::string _file_service_name;
            std::string _speech_service_name;
            std::string _message_service_name;
            std::string _transmite_service_name;
            std::string _friend_service_name;
            ServiceManager::ptr _mm_channels;             // 各子服务的 brpc channel 池(RR 选路)
            Discovery::ptr _service_discoverer;            // etcd 服务发现对象

            //--- WebSocket 连接映射(uid ↔ conn 双向,推送时按 uid 反查 conn) ---
            Connection::ptr _connections;

            //--- 网络服务对象 ---
            server_t _ws_server;                 // WebSocket 服务(websocketpp)
            httplib::Server _http_server;        // HTTP 服务(cpp-httplib)
            std::thread _http_thread;            // HTTP 独立线程(避免阻塞 WS 事件循环)
    };

    //网关构造器:按模块装配依赖(Redis → 服务发现 → 端口),最后 build() 出 GatewayServer 实例
    //设计动机:把"依赖准备"与"对象构造"分离,便于在 build() 前校验各模块是否就绪
    class GatewayServerBuilder {
        public:
            //装配 Redis 客户端:供 Session/Status 表读写
            void make_redis_object(const std::string &host,
                int port,
                int db,
                bool keep_alive) {
                _redis_client = RedisClientFactory::create(host, port, db, keep_alive);
            }
            //装配服务发现 + 信道管理:声明关注的 6 个子服务,并绑定 etcd 上下线回调
            //回调链:etcd Watcher 推送 → onServiceOnline/Offline → ServiceChannel.append/remove
            //       → 新建/移除 brpc channel(连接池在节点上线时预建,非请求时建)
            void make_discovery_object(const std::string &reg_host,
                const std::string &base_service_name,
                const std::string &file_service_name,
                const std::string &speech_service_name,
                const std::string &message_service_name,
                const std::string &friend_service_name,
                const std::string &user_service_name,
                const std::string &transmite_service_name) {
                _file_service_name      = file_service_name;
                _speech_service_name    = speech_service_name;
                _message_service_name   = message_service_name;
                _friend_service_name    = friend_service_name;
                _user_service_name      = user_service_name;
                _transmite_service_name = transmite_service_name;
                _mm_channels = std::make_shared<ServiceManager>();
                //声明关注的子服务,不声明的不维护其信道
                _mm_channels->declared(file_service_name);
                _mm_channels->declared(speech_service_name);
                _mm_channels->declared(message_service_name);
                _mm_channels->declared(friend_service_name);
                _mm_channels->declared(user_service_name);
                _mm_channels->declared(transmite_service_name);
                //用 std::bind 把成员函数适配为 Discovery 期望的回调签名
                auto put_cb = std::bind(&ServiceManager::onServiceOnline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
                auto del_cb = std::bind(&ServiceManager::onServiceOffline, _mm_channels.get(), std::placeholders::_1, std::placeholders::_2);
                _service_discoverer = std::make_shared<Discovery>(reg_host, base_service_name, put_cb, del_cb);
            }
            //装配 HTTP/WebSocket 监听端口
            void make_server_object(int websocket_port, int http_port) {
                _websocket_port = websocket_port;
                _http_port = http_port;
            }
            //构造网关实例:前置校验三大依赖(Redis/服务发现/信道管理)是否就绪
            //任一缺失直接 abort,避免运行时 NullPtr
            GatewayServer::ptr build() {
                if (!_redis_client) {
                    LOG_ERROR("还未初始化Redis客户端模块！");
                    abort();
                }
                if (!_service_discoverer) {
                    LOG_ERROR("还未初始化服务发现模块！");
                    abort();
                }
                if (!_mm_channels) {
                    LOG_ERROR("还未初始化信道管理模块！");
                    abort();
                }
                GatewayServer::ptr server = std::make_shared<GatewayServer>(
                    _websocket_port, _http_port, _redis_client, _mm_channels,
                    _service_discoverer, _user_service_name, _file_service_name,
                    _speech_service_name, _message_service_name,
                    _transmite_service_name, _friend_service_name);
                return server;
            }
        private:
            int _websocket_port;
            int _http_port;

            std::shared_ptr<sw::redis::Redis> _redis_client;

            std::string _file_service_name;
            std::string _speech_service_name;
            std::string _message_service_name;
            std::string _friend_service_name;
            std::string _user_service_name;
            std::string _transmite_service_name;
            ServiceManager::ptr _mm_channels;
            Discovery::ptr _service_discoverer;
    };
}
