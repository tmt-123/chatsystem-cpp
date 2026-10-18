#include <brpc/server.h>
#include <fstream>
#include <algorithm>
#include "../gateway/source/gateway_server.hpp"

// Real Gateway + Redis + brpc transport, with a deterministic transmit backend.
// No MySQL/MQ/etcd writes, and no dependency on users or data in a running stack.
class TransmitFixture : public bite_im::MsgTransmitService {
public:
    std::string prefix;
    void GetTransmitTarget(google::protobuf::RpcController*, const bite_im::NewMessageReq* req,
                           bite_im::GetTransmitTargetRsp* rsp, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        rsp->set_success(true);
        auto message = rsp->mutable_message();
        message->set_message_id("message-" + req->client_message_id());
        message->set_client_message_id(req->client_message_id());
        message->set_chat_session_id(req->chat_session_id());
        message->mutable_sender()->set_user_id(req->user_id());
        message->mutable_message()->CopyFrom(req->message());
        rsp->add_target_id_list(prefix + "-a");
        rsp->add_target_id_list(prefix + "-b");
        if (req->chat_session_id() == "group") rsp->add_target_id_list(prefix + "-c");
    }
};
int main(int argc, char** argv) {
    if (argc != 3 && argc != 4) return 2; // redis host, metadata path (or test prefix + --cleanup)
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    auto redis = bite_im::RedisClientFactory::create(argv[1], 6379, 15, false);
    if (argc == 4) {
        std::string prefix = argv[2];
        if (std::string(argv[3]) != "--cleanup" || prefix.rfind("test-gateway-", 0) != 0
            || !std::all_of(prefix.begin() + 13, prefix.end(), [](char c) { return c >= '0' && c <= '9'; })) return 2;
        for (const auto& suffix : {"a", "b", "c"}) {
            std::string uid = prefix + "-" + suffix;
            redis->del({uid, uid + "-token", "session:user:" + uid});
        }
        return 0;
    }
    bite_im::Session sessions(redis);
    const std::string prefix = "test-gateway-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    TransmitFixture backend;
    backend.prefix = prefix;
    brpc::Server rpc;
    if (rpc.AddService(&backend, brpc::SERVER_DOESNT_OWN_SERVICE) || rpc.Start(19002, nullptr)) return 3;
    auto channels = std::make_shared<bite_im::ServiceManager>();
    channels->declared("/test/transmit");
    channels->onServiceOnline("/test/transmit/instance", "127.0.0.1:19002");
    auto gateway = new bite_im::GatewayServer(19001, 19000, redis, channels, nullptr,
        "unused", "unused", "unused", "unused", "/test/transmit", "unused");
    std::ofstream metadata(argv[2]);
    metadata << "{\"prefix\":\"" << prefix << "\",\"sessions\":{";
    bool first = true;
    for (const auto& suffix : {"a", "b", "c"}) {
        std::string uid = prefix + "-" + suffix, token = uid + "-token";
        if (!sessions.create(token, uid)) return 4;
        if (!first) metadata << ',';
        metadata << '"' << suffix << "\":\"" << token << '"';
        first = false;
    }
    metadata << "}}";
    metadata.close();
    gateway->start();
}
