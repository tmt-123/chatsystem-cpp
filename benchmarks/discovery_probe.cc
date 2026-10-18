#include <brpc/server.h>
#include <chrono>
#include <iostream>
#include <mutex>
#include <set>
#include <thread>
#include <json/json.h>
#include "etcd.hpp"
#include "channel.hpp"
#include "user.pb.h"

using Clock = std::chrono::steady_clock;
class UserNode : public bite_im::UserService {
public:
    std::string name;
    explicit UserNode(std::string n):name(std::move(n)){}
    void GetUserInfo(google::protobuf::RpcController*, const bite_im::GetUserInfoReq*,
                     bite_im::GetUserInfoRsp* response, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        response->set_success(true);
        response->mutable_user_info()->set_user_id(name);
    }
};
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    bite_im::init_logger(false,"",spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    const std::string prefix="/bench-discovery-"+std::to_string(Clock::now().time_since_epoch().count());
    const std::string service=prefix+"/user";
    auto manager=std::make_shared<bite_im::ServiceManager>(); manager->declared(service);
    std::mutex mutex;
    std::set<std::string> seen;
    bite_im::Discovery discovery(argv[1],prefix,
        [&](std::string key,std::string host) { manager->onServiceOnline(key,host); std::lock_guard<std::mutex> lock(mutex); seen.insert(key); },
        [&](std::string key,std::string host) { manager->onServiceOffline(key,host); std::lock_guard<std::mutex> lock(mutex); seen.erase(key); });
    auto wait=[&](size_t count) {
        auto start=Clock::now();
        while (std::chrono::duration<double>(Clock::now()-start).count()<7) {
            { std::lock_guard<std::mutex> lock(mutex); if (seen.size()==count) return true; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    };
    UserNode node1("node1"),node2("node2");
    brpc::Server first,second;
    first.AddService(&node1,brpc::SERVER_DOESNT_OWN_SERVICE);
    second.AddService(&node2,brpc::SERVER_DOESNT_OWN_SERVICE);
    if (first.Start(20101,nullptr) || second.Start(20102,nullptr)) return 3;
    auto one=std::make_shared<bite_im::Registry>(argv[1]);
    auto two=std::make_shared<bite_im::Registry>(argv[1]);
    one->registry(service+"/one","127.0.0.1:20101");
    two->registry(service+"/two","127.0.0.1:20102");
    Json::Value result;
    result["two_nodes_discovered"]=wait(2);
    auto request=[&]() {
        auto channel=manager->choose(service);
        if (!channel) return std::string("no-node");
        brpc::Controller controller; controller.set_timeout_ms(200);
        bite_im::GetUserInfoReq req; bite_im::GetUserInfoRsp rsp;
        bite_im::UserService_Stub stub(channel.get());
        stub.GetUserInfo(&controller,&req,&rsp,nullptr);
        return controller.Failed() ? std::string("rpc-error") : rsp.user_info().user_id();
    };
    int successes=0;
    for(int i=0;i<40;++i) if(request().rfind("node",0)==0) ++successes;
    result["initial_rpc_success"]=successes; result["initial_rpc_attempts"]=40;
    first.Stop(0); first.Join();
    auto begin=Clock::now(); one.reset();
    result["node_one_removed"]=wait(1);
    result["removal_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-begin).count();
    successes=0;
    for(int i=0;i<40;++i) if(request()=="node2") ++successes;
    result["remaining_node_rpc_success"]=successes; result["remaining_node_rpc_attempts"]=40;
    second.Stop(0); second.Join(); two.reset();
    result["all_nodes_removed"]=wait(0);
    result["no_channel_when_all_offline"]=!manager->choose(service);
    if (second.Start(20102,nullptr)) return 4;
    begin=Clock::now();
    two=std::make_shared<bite_im::Registry>(argv[1]);
    two->registry(service+"/two","127.0.0.1:20102");
    result["node_restored"]=wait(1);
    result["recovery_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-begin).count();
    result["rpc_after_restore"]=request()=="node2";
    two->registry(service+"/two","127.0.0.1:20102");
    two->registry(service+"/two","127.0.0.1:20102");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    two.reset(); second.Stop(0); second.Join();
    result["duplicate_put_cleanup_event_seen"]=wait(0);
    result["stale_channel_after_duplicate_put"]=bool(manager->choose(service));
    Json::StreamWriterBuilder builder; builder["indentation"]="  ";
    std::cout<<Json::writeString(builder,result)<<std::endl;
}
