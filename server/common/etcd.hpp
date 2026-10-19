#pragma once
#include <etcd/Client.hpp>
#include <etcd/KeepAlive.hpp>
#include <etcd/Response.hpp>
#include <etcd/Watcher.hpp>
#include <etcd/Value.hpp>
#include <functional>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "logger.hpp"

namespace bite_im{
//服务注册客户端类
class Registry {
    public:
        using ptr = std::shared_ptr<Registry>;
        Registry(const std::string &host):
            _client(std::make_shared<etcd::Client>(host)) ,
            _keep_alive(_client->leasekeepalive(3).get()),
            _lease_id(_keep_alive->Lease()){
            _client->set_grpc_timeout(std::chrono::seconds(2));
            _monitor = std::thread([this] { maintain(); });
        }
        ~Registry() {
            { std::lock_guard<std::mutex> lock(_mutex); _stopping = true; }
            _wake.notify_all();
            if (_monitor.joinable()) _monitor.join();
            _keep_alive->Cancel();
        }
        bool registry(const std::string &key, const std::string &val) {
            std::lock_guard<std::mutex> lock(_mutex);
            auto resp = _client->put(key, val, _lease_id).get();
            if (resp.is_ok() == false) {
                LOG_ERROR("注册数据失败：{}", resp.error_message());
                return false;
            }
            _entries[key] = val;
            return true;
        }
    private:
        void maintain() {
            std::unique_lock<std::mutex> lock(_mutex);
            while (!_wake.wait_for(lock, std::chrono::seconds(1), [this] { return _stopping; })) {
                try {
                    auto ttl = _client->leasetimetolive(_lease_id).get();
                    if (!ttl.is_ok()) continue; // transport failure: retry, don't flood new leases
                    if (ttl.value().ttl() > 0) {
                        if (_restore_pending) restore();
                        continue;
                    }
                    auto alive = _client->leasekeepalive(3).get();
                    _keep_alive->Cancel();
                    _keep_alive = std::move(alive);
                    _lease_id = _keep_alive->Lease();
                    LOG_WARN("服务注册租约失效，重新获取租约并恢复实例注册");
                    _restore_pending = true;
                    restore();
                } catch (const std::exception &e) {
                    LOG_WARN("检查服务注册租约失败: {}", e.what());
                    continue;
                }
            }
        }
        void restore() {
            bool complete = true;
            for (const auto &entry : _entries) {
                auto response = _client->put(entry.first, entry.second, _lease_id).get();
                complete = response.is_ok() && complete;
            }
            _restore_pending = !complete;
        }
        std::mutex _mutex;
        std::condition_variable _wake;
        std::thread _monitor;
        bool _stopping = false, _restore_pending = false;
        std::unordered_map<std::string, std::string> _entries;
        std::shared_ptr<etcd::Client> _client;
        std::shared_ptr<etcd::KeepAlive> _keep_alive;
        uint64_t _lease_id;
};

//服务发现客户端类
class Discovery {
    public:
        using ptr = std::shared_ptr<Discovery>;
        using NotifyCallback = std::function<void(std::string, std::string)>;
        Discovery(const std::string &host, 
            const std::string &basedir,
            const NotifyCallback &put_cb,
            const NotifyCallback &del_cb):
            _client(std::make_shared<etcd::Client>(host)) ,
            _put_cb(put_cb), _del_cb(del_cb){
            //先进行服务发现,先获取到当前已有的数据
            auto resp = _client->ls(basedir).get();
            if (resp.is_ok() == false) {
                LOG_ERROR("获取服务信息数据失败：{}", resp.error_message());
            }
            int sz = resp.keys().size();
            for (int i = 0; i < sz; ++i) {
                if (_put_cb) _put_cb(resp.key(i), resp.value(i).as_string());
            }
            //然后进行事件监控，监控数据发生的改变并调用回调进行处理
            _watcher = std::make_shared<etcd::Watcher>(*_client.get(), basedir,
                std::bind(&Discovery::callback, this, std::placeholders::_1), true);
        }
        ~Discovery() {
            _watcher->Cancel();
        }
    private:
        void callback(const etcd::Response &resp) {
            if (resp.is_ok() == false) {
                LOG_ERROR("收到一个错误的事件通知: {}", resp.error_message());
                return;
            }
            for (auto const& ev : resp.events()) {
                if (ev.event_type() == etcd::Event::EventType::PUT) {
                    if (_put_cb) _put_cb(ev.kv().key(), ev.kv().as_string());
                    LOG_DEBUG("新增服务：{}-{}", ev.kv().key(), ev.kv().as_string());
                }else if (ev.event_type() == etcd::Event::EventType::DELETE_) {
                    if (_del_cb) _del_cb(ev.prev_kv().key(), ev.prev_kv().as_string());
                    LOG_DEBUG("下线服务：{}-{}", ev.prev_kv().key(), ev.prev_kv().as_string());
                }
            }
        }
    private:
        NotifyCallback _put_cb;
        NotifyCallback _del_cb;
        std::shared_ptr<etcd::Client> _client;
        std::shared_ptr<etcd::Watcher> _watcher;
};
}
