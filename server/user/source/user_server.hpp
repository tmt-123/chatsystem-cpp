//用户管理子服务的核心实现文件
//本文件实现用户管理子服务的完整业务逻辑，包含三个核心类：
//  1. UserServiceImpl    —— 用户管理Rpc服务的业务实现类，继承自protobuf生成的UserService
//     提供用户注册/登录、手机号注册/登录、短信验证码获取、用户信息查询(单个/批量)、
//     用户资料设置(头像/昵称/签名/手机号)等接口
//  2. UserServer         —— 用户管理Rpc服务的运行容器，封装Rpc服务器的生命周期
//  3. UserServerBuilder  —— 建造者类，用于分步组装UserServer所需的各组成模块
//                            (DMS短信/ES/MySQL/Redis/服务发现/服务注册/RPC服务器)
#include <brpc/server.h>
#include <butil/logging.h>

#include "data_es.hpp"      // es数据管理客户端封装
#include "data_redis.hpp"      // redis数据管理客户端封装
#include "mysql_user.hpp"      // mysql数据管理客户端封装
#include "etcd.hpp"     // 服务注册模块封装
#include "logger.hpp"   // 日志模块封装
#include "utils.hpp"    // 基础工具接口
#include "dms.hpp"      // 短信平台SDK模块封装
#include "channel.hpp"  // 信道管理模块封装

#include "user.hxx"
#include "user-odb.hxx"

#include "user.pb.h"  // protobuf框架代码 - 用户子服务协议
#include "base.pb.h"  // protobuf框架代码 - 基础协议
#include "file.pb.h"  // protobuf框架代码 - 文件子服务协议

namespace bite_im{
//用户管理Rpc服务业务实现类
//继承自protobuf生成的bite_im::UserService基类，重写其全部rpc接口方法
class UserServiceImpl : public bite_im::UserService {
    public:
        //构造函数：注入各外部依赖(短信平台/ES/MySQL/Redis/信道管理/文件子服务名)
        UserServiceImpl(const DMSClient::ptr &dms_client,
            const std::shared_ptr<elasticlient::Client> &es_client,
            const std::shared_ptr<odb::core::database> &mysql_client,
            const std::shared_ptr<sw::redis::Redis> &redis_client,
            const ServiceManager::ptr &channel_manager,
            const std::string &file_service_name) :
            _es_user(std::make_shared<ESUser>(es_client)),
            _mysql_user(std::make_shared<UserTable>(mysql_client)),
            _redis_session(std::make_shared<Session>(redis_client)),
            _redis_status(std::make_shared<Status>(redis_client)),
            _redis_codes(std::make_shared<Codes>(redis_client)),
            _file_service_name(file_service_name),
            _mm_channels(channel_manager),
            _dms_client(dms_client){
            _es_user->createIndex();   //启动时确保ES用户索引已创建
        }
        ~UserServiceImpl(){}
        //校验昵称格式：长度需小于22个字符
        bool nickname_check(const std::string &nickname) {
            return nickname.size() < 22;
        }
        //校验密码格式：长度6~15，且只能包含字母、数字、下划线_、连字符-
        bool password_check(const std::string &password) {
            if (password.size() < 6 || password.size() > 15) {
                LOG_ERROR("密码长度不合法，长度={}", password.size());
                return false;
            }
            for (int i = 0; i < password.size(); i++) {
                if (!((password[i] >= 'a' && password[i] <= 'z') ||
                    (password[i] >= 'A' && password[i] <= 'Z') ||
                    (password[i] >= '0' && password[i] <= '9') ||
                    password[i] == '_' || password[i] == '-')) {
                    LOG_ERROR("密码包含不允许的字符");
                    return false;
                }
            }
            return true;
        }
        //用户名注册接口：根据昵称+密码注册新用户
        virtual void UserRegister(::google::protobuf::RpcController* controller,
            const ::bite_im::UserRegisterReq* request,
            ::bite_im::UserRegisterRsp* response,
            ::google::protobuf::Closure* done) {
            LOG_DEBUG("收到用户注册请求！");
            //ClosureGuard保证done在作用域结束时被调用(无论正常返回还是异常)
            brpc::ClosureGuard rpc_guard(done);
            //定义一个错误处理函数，当出错的时候被调用
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            //1. 从请求中取出昵称和密码
            std::string nickname = request->nickname();
            std::string password = request->password();
            //2. 检查昵称是否合法（只能包含字母，数字，连字符-，下划线_，长度限制 3~15 之间）
            bool ret = nickname_check(nickname);
            if (ret == false) {
                LOG_ERROR("{} - 用户名长度不合法！", request->request_id());
                return err_response(request->request_id(), "用户名长度不合法！");
            }
            //3. 检查密码是否合法（只能包含字母，数字，长度限制 6~15 之间）
            ret = password_check(password);
            if (ret == false) {
                LOG_ERROR("{} - 密码格式不合法！", request->request_id());
                return err_response(request->request_id(), "密码格式不合法！");
            }
            //4. 根据昵称在数据库进行判断是否昵称已存在
            auto user = _mysql_user->select_by_nickname(nickname);
            if (user) {
                LOG_ERROR("{} - 用户名被占用- {}！", request->request_id(), nickname);
                return err_response(request->request_id(), "用户名被占用!");
            }
            //5. 向数据库新增数据
            std::string password_hash;
            try {
                password_hash = hashPassword(password);
            } catch (const std::exception &e) {
                LOG_ERROR("{} - 密码哈希生成失败：{}", request->request_id(), e.what());
                return err_response(request->request_id(), "密码安全处理失败!");
            }
            std::string uid = uuid();
            user = std::make_shared<User>(uid, nickname, password_hash);
            ret = _mysql_user->insert(user);
            if (ret == false) {
                LOG_ERROR("{} - Mysql数据库新增数据失败！", request->request_id());
                return err_response(request->request_id(), "Mysql数据库新增数据失败!");
            }
            //6. 向 ES 服务器中新增用户信息
            ret = _es_user->appendData(uid, "", nickname, "", "");
            if (ret == false) {
                LOG_ERROR("{} - ES搜索引擎新增数据失败！", request->request_id());
                return err_response(request->request_id(), "ES搜索引擎新增数据失败！");
            }
            //7. 组织响应，进行成功与否的响应即可。
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //用户名登录接口：根据昵称+密码登录，并生成登录会话
        virtual void UserLogin(::google::protobuf::RpcController* controller,
            const ::bite_im::UserLoginReq* request,
            ::bite_im::UserLoginRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到用户登录请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            //1. 从请求中取出昵称和密码
            std::string nickname = request->nickname();
            std::string password = request->password();
            //2. 通过昵称获取用户信息，进行密码是否一致的判断
            auto user = _mysql_user->select_by_nickname(nickname);
            if (!user) {
                LOG_ERROR("{} - 用户名或密码错误，用户={}！", request->request_id(), nickname);
                return err_response(request->request_id(), "用户名或密码错误!");
            }
            const std::string stored_password = user->password();
            bool password_valid = verifyPassword(password, stored_password);
            // 兼容已有数据库中的历史明文密码，登录成功后立即升级为安全哈希。
            if (!passwordIsHashed(stored_password) && password == stored_password) {
                password_valid = true;
                try {
                    user->password(hashPassword(password));
                    if (!_mysql_user->update(user)) {
                        LOG_WARN("{} - 历史密码哈希升级失败，用户={}", request->request_id(), nickname);
                    }
                } catch (const std::exception &e) {
                    LOG_WARN("{} - 历史密码哈希升级异常，用户={}：{}", request->request_id(), nickname, e.what());
                }
            }
            if (!password_valid) {
                LOG_ERROR("{} - 用户名或密码错误，用户={}！", request->request_id(), nickname);
                return err_response(request->request_id(), "用户名或密码错误!");
            }
            //3. 根据 redis 中的登录标记信息是否存在判断用户是否已经登录。
            bool ret = _redis_status->exists(user->user_id());
            if (ret == true) {
                LOG_ERROR("{} - 用户已在其他地方登录 - {}！", request->request_id(), nickname);
                return err_response(request->request_id(), "用户已在其他地方登录!");
            }
            //4. 构造会话 ID，生成会话键值对，向 redis 中添加会话信息以及登录标记信息
            std::string ssid = uuid();
            _redis_session->append(ssid, user->user_id());
            //5. 添加用户登录信息
            _redis_status->append(user->user_id());
            //6. 组织响应，返回生成的会话 ID
            response->set_request_id(request->request_id());
            response->set_login_session_id(ssid);
            response->set_success(true);
        }
        //校验手机号码格式：必须为11位，首位1，第二位3~9，其余为数字
        bool phone_check(const std::string &phone) {
            if (phone.size() != 11) return false;
            if (phone[0] != '1') return false;
            if (phone[1] < '3' || phone[1] > '9') return false;
            for (int i = 2; i < 11; i++) {
                if (phone[i] < '0' || phone[i] > '9') return false;
            }
            return true;
        }
        //获取短信验证码接口：校验手机号后通过DMS发送验证码，并将验证码缓存到redis
        virtual void GetPhoneVerifyCode(::google::protobuf::RpcController* controller,
            const ::bite_im::PhoneVerifyCodeReq* request,
            ::bite_im::PhoneVerifyCodeRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到短信验证码获取请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出手机号码
            std::string phone = request->phone_number();
            // 2. 验证手机号码格式是否正确（必须以 1 开始，第二位 3~9 之间，后边 9 个数字字符）
            bool ret = phone_check(phone);
            if (ret == false) {
                LOG_ERROR("{} - 手机号码格式错误！", request->request_id());
                return err_response(request->request_id(), "手机号码格式错误!");
            }
            // 3. 生成 4 位随机验证码
            std::string code_id = uuid();
            std::string code = vcode();
            // 4. 基于短信平台 SDK 发送验证码
            ret = _dms_client->send(phone, code);
            if (ret == false) {
                LOG_ERROR("{} - 短信验证码发送失败！", request->request_id());
                return err_response(request->request_id(), "短信验证码发送失败!");
            }
            // 5. 构造验证码 ID，添加到 redis 验证码映射键值索引中
            _redis_codes->append(code_id, code);
            // 6. 组织响应，返回生成的验证码 ID
            response->set_request_id(request->request_id());
            response->set_success(true);
            response->set_verify_code_id(code_id);
            LOG_DEBUG("获取短信验证码处理完成！");
        }
        //手机号注册接口：校验手机号+验证码后注册新用户
        virtual void PhoneRegister(::google::protobuf::RpcController* controller,
            const ::bite_im::PhoneRegisterReq* request,
            ::bite_im::PhoneRegisterRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到手机号注册请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出手机号码和验证码,验证码ID
            std::string phone = request->phone_number();
            std::string code_id = request->verify_code_id();
            std::string code = request->verify_code();
            // 2. 检查注册手机号码是否合法
            bool ret = phone_check(phone);
            if (ret == false) {
                LOG_ERROR("{} - 手机号码格式错误！", request->request_id());
                return err_response(request->request_id(), "手机号码格式错误!");
            }
            // 3. 从 redis 数据库中进行验证码 ID-验证码一致性匹配
            auto vcode = _redis_codes->code(code_id);
            if (vcode != code) {
                LOG_ERROR("{} - 验证码错误！", request->request_id());
                return err_response(request->request_id(), "验证码错误!");
            }
            // 4. 通过数据库查询判断手机号是否已经注册过
            auto user = _mysql_user->select_by_phone(phone);
            if (user) {
                LOG_ERROR("{} - 该手机号已注册过用户！", request->request_id());
                return err_response(request->request_id(), "该手机号已注册过用户!");
            }
            // 5. 向数据库新增用户信息
            std::string uid = uuid();
            user = std::make_shared<User>(uid, phone);
            ret = _mysql_user->insert(user);
            if (ret == false) {
                LOG_ERROR("{} - 向数据库添加用户信息失败！", request->request_id());
                return err_response(request->request_id(), "向数据库添加用户信息失败!");
            }
            // 6. 向 ES 服务器中新增用户信息
            ret = _es_user->appendData(uid, phone, uid, "", "");
            if (ret == false) {
                LOG_ERROR("{} - ES搜索引擎新增数据失败！", request->request_id());
                return err_response(request->request_id(), "ES搜索引擎新增数据失败！");
            }
            //7. 组织响应，进行成功与否的响应即可。
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //手机号登录接口：校验手机号+验证码后登录，并生成登录会话
        virtual void PhoneLogin(::google::protobuf::RpcController* controller,
            const ::bite_im::PhoneLoginReq* request,
            ::bite_im::PhoneLoginRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到手机号登录请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出手机号码和验证码 ID，以及验证码。
            std::string phone = request->phone_number();
            std::string code_id = request->verify_code_id();
            std::string code = request->verify_code();
            // 2. 检查注册手机号码是否合法
            bool ret = phone_check(phone);
            if (ret == false) {
                LOG_ERROR("{} - 手机号码格式错误！", request->request_id());
                return err_response(request->request_id(), "手机号码格式错误!");
            }
            // 3. 根据手机号从数据数据进行用户信息查询，判断用用户是否存在
            auto user = _mysql_user->select_by_phone(phone);
            if (!user) {
                LOG_ERROR("{} - 该手机号未注册用户！", request->request_id());
                return err_response(request->request_id(), "该手机号未注册用户!");
            }
            // 4. 从 redis 数据库中进行验证码 ID-验证码一致性匹配
            auto vcode = _redis_codes->code(code_id);
            if (vcode != code) {
                LOG_ERROR("{} - 验证码错误！", request->request_id());
                return err_response(request->request_id(), "验证码错误!");
            }
            _redis_codes->remove(code_id);
            // 5. 根据 redis 中的登录标记信息是否存在判断用户是否已经登录。
            ret = _redis_status->exists(user->user_id());
            if (ret == true) {
                LOG_ERROR("{} - 用户已在其他地方登录！", request->request_id());
                return err_response(request->request_id(), "用户已在其他地方登录!");
            }
            //6. 构造会话 ID，生成会话键值对，向 redis 中添加会话信息以及登录标记信息
            std::string ssid = uuid();
            _redis_session->append(ssid, user->user_id());
            //7. 添加用户登录信息
            _redis_status->append(user->user_id());
            // 8. 组织响应，返回生成的会话 ID
            response->set_request_id(request->request_id());
            response->set_login_session_id(ssid);
            response->set_success(true);
        }
        //从这一步开始，用户登录之后才会进行的操作
        //获取单个用户信息接口：从数据库查用户信息，若存在头像则向文件子服务请求下载
        virtual void GetUserInfo(::google::protobuf::RpcController* controller,
            const ::bite_im::GetUserInfoReq* request,
            ::bite_im::GetUserInfoRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到获取单个用户信息请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出用户 ID
            std::string uid = request->user_id();
            // 2. 通过用户 ID，从数据库中查询用户信息
            auto user = _mysql_user->select_by_id(uid);
            if (!user) {
                LOG_ERROR("{} - 未找到用户信息 - {}！", request->request_id(), uid);
                return err_response(request->request_id(), "未找到用户信息!");
            }
            // 3. 根据用户信息中的头像 ID，从文件服务器获取头像文件数据，组织完整用户信息
            UserInfo *user_info = response->mutable_user_info();
            user_info->set_user_id(user->user_id());
            user_info->set_nickname(user->nickname());
            user_info->set_description(user->description());
            user_info->set_phone(user->phone());
            
            if (!user->avatar_id().empty()) {
                //从信道管理对象中，获取到连接了文件管理子服务的channel
                auto channel = _mm_channels->choose(_file_service_name);
                if (!channel) {
                    LOG_ERROR("{} - 未找到文件管理子服务节点 - {} - {}！", 
                        request->request_id(), _file_service_name, uid);
                    return err_response(request->request_id(), "未找到文件管理子服务节点!");
                }
                //进行文件子服务的rpc请求，进行头像文件下载
                bite_im::FileService_Stub stub(channel.get());
                bite_im::GetSingleFileReq req;
                bite_im::GetSingleFileRsp rsp;
                req.set_request_id(request->request_id());
                req.set_file_id(user->avatar_id());
                brpc::Controller cntl;
                stub.GetSingleFile(&cntl, &req, &rsp, nullptr);
                if (cntl.Failed() == true || rsp.success() == false) {
                    LOG_ERROR("{} - 文件子服务调用失败：{}！", request->request_id(), cntl.ErrorText());
                    return err_response(request->request_id(), "文件子服务调用失败!");
                }
                user_info->set_avatar(rsp.file_data().file_content());
            }
            // 4. 组织响应，返回用户信息
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //批量获取用户信息接口：批量从数据库查用户信息，并批量向文件子服务请求头像下载
        virtual void GetMultiUserInfo(::google::protobuf::RpcController* controller,
            const ::bite_im::GetMultiUserInfoReq* request,
            ::bite_im::GetMultiUserInfoRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到批量用户信息获取请求！");
            brpc::ClosureGuard rpc_guard(done);
            //1. 定义错误回调
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            //2. 从请求中取出用户ID --- 列表
            std::vector<std::string> uid_lists;
            for (int i = 0; i < request->users_id_size(); i++) {
                uid_lists.push_back(request->users_id(i));
            }
            //3. 从数据库进行批量用户信息查询
            auto users = _mysql_user->select_multi_users(uid_lists);
            if (users.size() != request->users_id_size()) {
                LOG_ERROR("{} - 从数据库查找的用户信息数量不一致 {}-{}！", 
                    request->request_id(), request->users_id_size(), users.size());
                return err_response(request->request_id(), "从数据库查找的用户信息数量不一致!");
            }
            //4. 批量从文件管理子服务进行文件下载
            auto channel = _mm_channels->choose(_file_service_name);
            if (!channel) {
                LOG_ERROR("{} - 未找到文件管理子服务节点 - {}！", request->request_id(), _file_service_name);
                return err_response(request->request_id(), "未找到文件管理子服务节点!");
            }
            bite_im::FileService_Stub stub(channel.get());
            bite_im::GetMultiFileReq req;
            bite_im::GetMultiFileRsp rsp;
            req.set_request_id(request->request_id());
            for (auto &user : users) {
                if (user.avatar_id().empty()) continue;
                req.add_file_id_list(user.avatar_id());
            }
            brpc::Controller cntl;
            stub.GetMultiFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed() == true || rsp.success() == false) {
                LOG_ERROR("{} - 文件子服务调用失败：{} - {}！", request->request_id(), 
                    _file_service_name, cntl.ErrorText());
                return err_response(request->request_id(), "文件子服务调用失败!");
            }
            //5. 组织响应（将用户信息与对应头像数据组装到响应的map中）
            for (auto &user : users) {
                auto user_map = response->mutable_users_info();//本次请求要响应的用户信息map
                auto file_map = rsp.mutable_file_data(); //这是批量文件请求响应中的map 
                UserInfo user_info;
                user_info.set_user_id(user.user_id());
                user_info.set_nickname(user.nickname());
                user_info.set_description(user.description());
                user_info.set_phone(user.phone());
                user_info.set_avatar((*file_map)[user.avatar_id()].file_content());
                (*user_map)[user_info.user_id()] = user_info;
            }
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //设置用户头像接口：将头像数据上传至文件子服务，并将返回的头像ID更新到数据库与ES
        virtual void SetUserAvatar(::google::protobuf::RpcController* controller,
            const ::bite_im::SetUserAvatarReq* request,
            ::bite_im::SetUserAvatarRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到用户头像设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出用户 ID 与头像数据
            std::string uid = request->user_id();
            // 2. 从数据库通过用户 ID 进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if (!user) {
                LOG_ERROR("{} - 未找到用户信息 - {}！", request->request_id(), uid);
                return err_response(request->request_id(), "未找到用户信息!");
            }
            // 3. 上传头像文件到文件子服务，
            auto channel = _mm_channels->choose(_file_service_name);
            if (!channel) {
                LOG_ERROR("{} - 未找到文件管理子服务节点 - {}！", request->request_id(), _file_service_name);
                return err_response(request->request_id(), "未找到文件管理子服务节点!");
            }
            bite_im::FileService_Stub stub(channel.get());
            bite_im::PutSingleFileReq req;
            bite_im::PutSingleFileRsp rsp;
            req.set_request_id(request->request_id());
            req.mutable_file_data()->set_file_name("");
            req.mutable_file_data()->set_file_size(request->avatar().size());
            req.mutable_file_data()->set_file_content(request->avatar());
            brpc::Controller cntl;
            stub.PutSingleFile(&cntl, &req, &rsp, nullptr);
            if (cntl.Failed() == true || rsp.success() == false) {
                LOG_ERROR("{} - 文件子服务调用失败：{}！", request->request_id(), cntl.ErrorText());
                return err_response(request->request_id(), "文件子服务调用失败!");
            }
            std::string avatar_id = rsp.file_info().file_id();
            // 4. 将返回的头像文件 ID 更新到数据库中
            user->avatar_id(avatar_id);
            bool ret = _mysql_user->update(user);
            if (ret == false) {
                LOG_ERROR("{} - 更新数据库用户头像ID失败 ：{}！", request->request_id(), avatar_id);
                return err_response(request->request_id(), "更新数据库用户头像ID失败!");
            }
            // 5. 更新 ES 服务器中用户信息
            ret = _es_user->appendData(user->user_id(), user->phone(),
                user->nickname(), user->description(), user->avatar_id());
            if (ret == false) {
                LOG_ERROR("{} - 更新搜索引擎用户头像ID失败 ：{}！", request->request_id(), avatar_id);
                return err_response(request->request_id(), "更新搜索引擎用户头像ID失败!");
            }
            // 6. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //设置用户昵称接口：校验昵称合法性后更新到数据库与ES
        virtual void SetUserNickname(::google::protobuf::RpcController* controller,
            const ::bite_im::SetUserNicknameReq* request,
            ::bite_im::SetUserNicknameRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到用户昵称设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出用户 ID 与新的昵称
            std::string uid = request->user_id();
            std::string new_nickname = request->nickname();
            // 2. 判断昵称格式是否正确
            bool ret = nickname_check(new_nickname);
            if (ret == false) {
                LOG_ERROR("{} - 用户名长度不合法！", request->request_id());
                return err_response(request->request_id(), "用户名长度不合法！");
            }
            // 3. 从数据库通过用户 ID 进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if (!user) {
                LOG_ERROR("{} - 未找到用户信息 - {}！", request->request_id(), uid);
                return err_response(request->request_id(), "未找到用户信息!");
            }
            // 4. 将新的昵称更新到数据库中
            user->nickname(new_nickname);
            ret = _mysql_user->update(user);
            if (ret == false) {
                LOG_ERROR("{} - 更新数据库用户昵称失败 ：{}！", request->request_id(), new_nickname);
                return err_response(request->request_id(), "更新数据库用户昵称失败!");
            }
            // 5. 更新 ES 服务器中用户信息
            ret = _es_user->appendData(user->user_id(), user->phone(),
                user->nickname(), user->description(), user->avatar_id());
            if (ret == false) {
                LOG_ERROR("{} - 更新搜索引擎用户昵称失败 ：{}！", request->request_id(), new_nickname);
                return err_response(request->request_id(), "更新搜索引擎用户昵称失败!");
            }
            // 6. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //设置用户签名接口：更新用户签名到数据库与ES
        virtual void SetUserDescription(::google::protobuf::RpcController* controller,
            const ::bite_im::SetUserDescriptionReq* request,
            ::bite_im::SetUserDescriptionRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到用户签名设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出用户 ID 与新的签名
            std::string uid = request->user_id();
            std::string new_description = request->description();
            // 2. 从数据库通过用户 ID 进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if (!user) {
                LOG_ERROR("{} - 未找到用户信息 - {}！", request->request_id(), uid);
                return err_response(request->request_id(), "未找到用户信息!");
            }
            // 3. 将新的签名更新到数据库中
            user->description(new_description);
            bool ret = _mysql_user->update(user);
            if (ret == false) {
                LOG_ERROR("{} - 更新数据库用户签名失败 ：{}！", request->request_id(), new_description);
                return err_response(request->request_id(), "更新数据库用户签名失败!");
            }
            // 4. 更新 ES 服务器中用户信息
            ret = _es_user->appendData(user->user_id(), user->phone(),
                user->nickname(), user->description(), user->avatar_id());
            if (ret == false) {
                LOG_ERROR("{} - 更新搜索引擎用户签名失败 ：{}！", request->request_id(), new_description);
                return err_response(request->request_id(), "更新搜索引擎用户签名失败!");
            }
            // 5. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
        //设置用户手机号接口：校验短信验证码后更新用户手机号到数据库与ES
        virtual void SetUserPhoneNumber(::google::protobuf::RpcController* controller,
            const ::bite_im::SetUserPhoneNumberReq* request,
            ::bite_im::SetUserPhoneNumberRsp* response,
            ::google::protobuf::Closure* done){
            LOG_DEBUG("收到用户手机号设置请求！");
            brpc::ClosureGuard rpc_guard(done);
            auto err_response = [this, response](const std::string &rid, 
                const std::string &errmsg) -> void {
                response->set_request_id(rid);
                response->set_success(false);
                response->set_errmsg(errmsg);
                return;
            };
            // 1. 从请求中取出用户 ID、新的手机号及验证码相关信息
            std::string uid = request->user_id();
            std::string new_phone = request->phone_number();
            std::string code = request->phone_verify_code();
            std::string code_id = request->phone_verify_code_id();
            // 2. 对验证码进行验证
            auto vcode = _redis_codes->code(code_id);
            if (vcode != code) {
                LOG_ERROR("{} - 验证码错误！", request->request_id());
                return err_response(request->request_id(), "验证码错误!");
            }
            // 3. 从数据库通过用户 ID 进行用户信息查询，判断用户是否存在
            auto user = _mysql_user->select_by_id(uid);
            if (!user) {
                LOG_ERROR("{} - 未找到用户信息 - {}！", request->request_id(), uid);
                return err_response(request->request_id(), "未找到用户信息!");
            }
            // 4. 将新的手机号更新到数据库中
            user->phone(new_phone);
            bool ret = _mysql_user->update(user);
            if (ret == false) {
                LOG_ERROR("{} - 更新数据库用户手机号失败！", request->request_id());
                return err_response(request->request_id(), "更新数据库用户手机号失败!");
            }
            // 5. 更新 ES 服务器中用户信息
            ret = _es_user->appendData(user->user_id(), user->phone(),
                user->nickname(), user->description(), user->avatar_id());
            if (ret == false) {
                LOG_ERROR("{} - 更新搜索引擎用户手机号失败！", request->request_id());
                return err_response(request->request_id(), "更新搜索引擎用户手机号失败!");
            }
            // 6. 组织响应，返回更新成功与否
            response->set_request_id(request->request_id());
            response->set_success(true);
        }
    private:
        //业务处理依赖的各数据/服务客户端对象
        ESUser::ptr _es_user;          //ES用户数据管理
        UserTable::ptr _mysql_user;     //MySQL用户数据管理
        Session::ptr _redis_session;    //Redis会话管理(会话ID->用户ID)
        Status::ptr _redis_status;      //Redis登录状态管理(用户ID->登录标记)
        Codes::ptr _redis_codes;        //Redis验证码管理(验证码ID->验证码)
        //这边是rpc调用客户端相关对象
        std::string _file_service_name;  //依赖的文件子服务名称
        ServiceManager::ptr _mm_channels;//信道管理对象(管理文件子服务的rpc信道)
        DMSClient::ptr _dms_client;       //短信平台客户端
};

//用户管理Rpc服务运行容器
//封装Rpc服务器及各依赖客户端的生命周期，仅对外暴露start()启动接口
class UserServer {
    public:
        using ptr = std::shared_ptr<UserServer>;
        //构造函数：注入服务发现对象、服务注册对象、各数据客户端及Rpc服务器
        UserServer(const Discovery::ptr service_discoverer, 
            const Registry::ptr &reg_client,
            const std::shared_ptr<elasticlient::Client> &es_client,
            const std::shared_ptr<odb::core::database> &mysql_client,
            std::shared_ptr<sw::redis::Redis> &redis_client,
            const std::shared_ptr<brpc::Server> &server):
            _service_discoverer(service_discoverer),
            _registry_client(reg_client),
            _es_client(es_client),
            _mysql_client(mysql_client),
            _redis_client(redis_client),
            _rpc_server(server){}
        ~UserServer(){}
        //搭建RPC服务器，并启动服务器
        void start() {
            _rpc_server->RunUntilAskedToQuit();
        }
    private:
        Discovery::ptr _service_discoverer;       //服务发现对象
        Registry::ptr _registry_client;            //服务注册对象
        std::shared_ptr<elasticlient::Client> _es_client;       //ES客户端
        std::shared_ptr<odb::core::database> _mysql_client;     //MySQL客户端
        std::shared_ptr<sw::redis::Redis> _redis_client;        //Redis客户端
        std::shared_ptr<brpc::Server> _rpc_server;              //Rpc服务器
};

//用户管理Rpc服务建造者类
//采用建造者模式，分步构造并组装UserServer所需的各组成模块
//使用流程：依次调用make_xxx_object构造各模块 -> make_rpc_server注册服务 -> build()组装出UserServer
class UserServerBuilder {
    public:
        //构造es客户端对象
        void make_es_object(const std::vector<std::string> host_list) {
            _es_client = ESClientFactory::create(host_list);
        }
        //构造阿里云短信平台(dms)客户端对象
        void make_dms_object(const std::string &access_key_id,
            const std::string &access_key_secret) {
            _dms_client = std::make_shared<DMSClient>(access_key_id, access_key_secret);
        }
        //构造mysql客户端对象
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
        //构造redis客户端对象
        void make_redis_object(const std::string &host,
            int port,
            int db,
            bool keep_alive) {
            _redis_client = RedisClientFactory::create(host, port, db, keep_alive);
        }
        //用于构造服务发现客户端&信道管理对象
        //同时声明关注的文件子服务，并绑定其上/下线回调
        void make_discovery_object(const std::string &reg_host,
            const std::string &base_service_name,
            const std::string &file_service_name) {
            _file_service_name = file_service_name;
            _mm_channels = std::make_shared<ServiceManager>();
            _mm_channels->declared(file_service_name);
            LOG_DEBUG("设置文件子服务为需添加管理的子服务：{}", file_service_name);
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
        //构造并启动Rpc服务器
        //会先校验所有依赖模块是否已就绪，再创建UserServiceImpl并注册到brpc服务器
        void make_rpc_server(uint16_t port, int32_t timeout, uint8_t num_threads) {
            if (!_es_client) {
                LOG_ERROR("还未初始化ES搜索引擎模块！");
                abort();
            }
            if (!_mysql_client) {
                LOG_ERROR("还未初始化Mysql数据库模块！");
                abort();
            }
            if (!_redis_client) {
                LOG_ERROR("还未初始化Redis数据库模块！");
                abort();
            }
            if (!_mm_channels) {
                LOG_ERROR("还未初始化信道管理模块！");
                abort();
            }
            if (!_dms_client) {
                LOG_ERROR("还未初始化短信平台模块！");
                abort();
            }
            _rpc_server = std::make_shared<brpc::Server>();

            UserServiceImpl *user_service = new UserServiceImpl(_dms_client, _es_client,
                _mysql_client, _redis_client, _mm_channels, _file_service_name);
            int ret = _rpc_server->AddService(user_service, 
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
        //构造RPC服务器对象
        //最终组装前再次校验服务发现、服务注册、Rpc服务器三模块是否就绪
        UserServer::ptr build() {
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
            UserServer::ptr server = std::make_shared<UserServer>(
                _service_discoverer, _registry_client,
                _es_client, _mysql_client, _redis_client, _rpc_server);
            return server;
        }
    private:
        Registry::ptr _registry_client;             //服务注册客户端

        std::shared_ptr<elasticlient::Client> _es_client;       //ES客户端
        std::shared_ptr<odb::core::database> _mysql_client;     //MySQL客户端
        std::shared_ptr<sw::redis::Redis> _redis_client;         //Redis客户端

        std::string _file_service_name;             //依赖的文件子服务名称
        ServiceManager::ptr _mm_channels;           //信道管理对象
        Discovery::ptr _service_discoverer;          //服务发现对象

        std::shared_ptr<DMSClient> _dms_client;     //短信平台客户端

        std::shared_ptr<brpc::Server> _rpc_server;   //Rpc服务器
};
}
