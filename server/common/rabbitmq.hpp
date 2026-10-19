#pragma once
#include <ev.h>
#include <amqpcpp.h>
#include <amqpcpp/libev.h>
#include <openssl/ssl.h>
#include <openssl/opensslv.h>
#include <iostream>
#include <functional>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <vector>
#include "consume_result.hpp"
#include "logger.hpp"

namespace bite_im{
class MQClient {
    public:
        using MessageCallback = std::function<ConsumeResult(const char*, size_t)>;
        using PartitionedCallback = std::function<ConsumeResult(const char*, size_t, std::size_t)>;
        using Partitioner = std::function<std::size_t(const char*, size_t)>;
        using ptr = std::shared_ptr<MQClient>;
        MQClient(const std::string &user, 
            const std::string passwd,
            const std::string host) {
            _loop = ev_loop_new(EVFLAG_AUTO);
            if (!_loop) throw std::runtime_error("Cannot create MQ event loop");
            ev_async_init(&_async_watcher, watcher_callback);
            _async_watcher.data = this;
            ev_async_start(_loop, &_async_watcher);
            ev_timer_init(&_retry_watcher, retry_callback, .05, .05);
            _retry_watcher.data = this;
            ev_timer_start(_loop, &_retry_watcher);
            _handler = std::make_unique<AMQP::LibEvHandler>(_loop);
            //amqp://root:123456@127.0.0.1:5672/
            std::string url = "amqp://" + user + ":" + passwd + "@" + host + "/";
            AMQP::Address address(url);
            _connection = std::make_unique<AMQP::TcpConnection>(_handler.get(), address);
            _channel = std::make_unique<AMQP::TcpChannel>(_connection.get());

            _loop_thread = std::thread([this]() {
                ev_run(_loop, 0);
            });
        }
        ~MQClient() {
             shutdown();
             ev_timer_stop(_loop, &_retry_watcher);
             ev_async_stop(_loop, &_async_watcher);
             _channel.reset();
             _connection.reset();
             _handler.reset();
             ev_loop_destroy(_loop);
             _loop = nullptr;
        }
        void shutdown() {
            std::lock_guard<std::mutex> stopping(_shutdown_mutex);
            if (!_loop_thread.joinable()) return;
            {
                std::lock_guard<std::mutex> lock(_work_mutex);
                _worker_stopping = true;
            }
            _work_ready.notify_all();
            for (auto &worker : _workers) if (worker.joinable()) worker.join();
            // Unfinished deliveries remain unacknowledged and are requeued
            // by RabbitMQ when this connection is destroyed.
            {
                std::lock_guard<std::mutex> lock(_commands_mutex);
                _accepting_commands = false;
                _commands.push_back([this] { ev_break(_loop, EVBREAK_ALL); });
            }
            ev_async_send(_loop, &_async_watcher);
            _loop_thread.join();
        }
        void declareComponents(const std::string &exchange,
            const std::string &queue,
            const std::string &routing_key = "routing_key",
            AMQP::ExchangeType echange_type = AMQP::ExchangeType::direct) {
            dispatch([this, exchange, queue, routing_key, echange_type] {
            _channel->declareExchange(exchange, echange_type)
                .onError([](const char *message) {
                    LOG_ERROR("声明交换机失败：{}", message);
                    exit(0);
                })
                .onSuccess([exchange](){
                    LOG_ERROR("{} 交换机创建成功！", exchange);
                });
            _channel->declareQueue(queue)
                .onError([](const char *message) {
                    LOG_ERROR("声明队列失败：{}", message);
                    exit(0);
                })
                .onSuccess([queue](){
                    LOG_ERROR("{} 队列创建成功！", queue);
                });
            //6. 针对交换机和队列进行绑定
            _channel->bindQueue(exchange, queue, routing_key)
                .onError([exchange, queue](const char *message) {
                    LOG_ERROR("{} - {} 绑定失败：", exchange, queue);
                    exit(0);
                })
                .onSuccess([exchange, queue, routing_key](){
                    LOG_ERROR("{} - {} - {} 绑定成功！", exchange, queue, routing_key);
                });
            });
        }
        bool publish(const std::string &exchange, 
            const std::string &msg, 
            const std::string &routing_key = "routing_key") {
            LOG_DEBUG("向交换机 {}-{} 发布消息！", exchange, routing_key);
            bool ret = dispatch([this, &exchange, &routing_key, &msg] {
                return _channel->publish(exchange, routing_key, msg);
            });
            if (ret == false) {
                LOG_ERROR("{} 发布消息失败：", exchange);
                return false;
            }
            return true;
        }
        void consume(const std::string &queue, const MessageCallback &cb, uint16_t prefetch = 1) {
            consumePartitioned(queue, [cb](const char *body, size_t size, std::size_t) {
                return cb(body, size);
            }, 1, prefetch, [](const char*, size_t) { return 0; });
        }
        void consumePartitioned(const std::string &queue, const PartitionedCallback &cb,
                                std::size_t workers, uint16_t prefetch, const Partitioner &partition) {
            if (prefetch == 0) throw std::invalid_argument("MQ prefetch must be bounded");
            if (workers == 0 || workers > 64) throw std::invalid_argument("MQ workers must be 1-64");
            if (_consumer_started) {
                throw std::logic_error("MQClient supports one consumer per connection");
            }
            _consumer_started = true;
            _work.resize(workers);
            for (std::size_t i = 0; i < workers; ++i) {
                _workers.emplace_back([this, cb, i] { work(cb, i); });
            }
            LOG_DEBUG("开始订阅 {} 队列消息！", queue);
            dispatch([this, queue, prefetch, partition] {
            _channel->setQos(prefetch);
            _channel->consume(queue, "consume-tag")  //返回值 DeferredConsumer
                .onReceived([this, partition](const AMQP::Message &message,
                    uint64_t deliveryTag, 
                    bool redelivered) {
                    {
                        std::lock_guard<std::mutex> lock(_work_mutex);
                        if (_worker_stopping || message.bodySize() > max_work_bytes - _work_bytes) {
                            finish(deliveryTag, ConsumeResult::Retry);
                            return;
                        }
                        _work_bytes += message.bodySize();
                        const auto shard = partition(message.body(), message.bodySize()) % _work.size();
                        _work[shard].push_back({deliveryTag, std::string(message.body(), message.bodySize())});
                    }
                    _work_ready.notify_all();
                })
                .onError([queue](const char *message){
                    LOG_ERROR("订阅 {} 队列消息失败: {}", queue, message);
                    exit(0);
                });
            });
        }
    private:
        using Clock = std::chrono::steady_clock;
        template<class F> auto dispatch(F fn) -> decltype(fn()) {
            if (std::this_thread::get_id() == _loop_thread.get_id()) return fn();
            auto task = std::make_shared<std::packaged_task<decltype(fn())()>>(std::move(fn));
            auto result = task->get_future();
            post([task] { (*task)(); });
            return result.get();
        }
        void post(std::function<void()> fn) {
            {
                std::lock_guard<std::mutex> lock(_commands_mutex);
                if (!_accepting_commands) throw std::logic_error("MQ client is shut down");
                _commands.push_back(std::move(fn));
            }
            ev_async_send(_loop, &_async_watcher);
        }
        void work(const PartitionedCallback &cb, std::size_t shard) {
            for (;;) {
                Work item;
                {
                    std::unique_lock<std::mutex> lock(_work_mutex);
                    _work_ready.wait(lock, [this, shard] { return _worker_stopping || !_work[shard].empty(); });
                    if (_worker_stopping) return;
                    item = std::move(_work[shard].front()); _work[shard].pop_front();
                }
                auto result = ConsumeResult::Retry;
                try { result = cb(item.body.data(), item.body.size(), shard); }
                catch (...) { LOG_ERROR("MQ 消费回调异常，保留消息重试"); }
                {
                    std::lock_guard<std::mutex> lock(_work_mutex);
                    _work_bytes -= item.body.size();
                }
                post([this, tag = item.tag, result] { finish(tag, result); });
            }
        }
        void finish(uint64_t tag, ConsumeResult result) {
            // All AMQP operations, including completions from storage workers,
            // run on the connection's event loop. Never use cumulative ACKs.
            if (result == ConsumeResult::Ack) _channel->ack(tag);
            else if (result == ConsumeResult::Reject) _channel->reject(tag);
            else _retries[tag] = Clock::now() + std::chrono::seconds(1);
        }
        static void retry_callback(struct ev_loop *, ev_timer *watcher, int) {
            auto *self = static_cast<MQClient*>(watcher->data);
            const auto now = Clock::now();
            for (auto it = self->_retries.begin(); it != self->_retries.end();) {
                if (it->second > now) { ++it; continue; }
                if (self->_channel->reject(it->first, AMQP::requeue)) it = self->_retries.erase(it);
                else { it->second = now + std::chrono::seconds(1); ++it; }
            }
        }
        static void watcher_callback(struct ev_loop *loop, ev_async *watcher, int32_t revents) {
            auto *self = static_cast<MQClient*>(watcher->data);
            std::deque<std::function<void()>> commands;
            { std::lock_guard<std::mutex> lock(self->_commands_mutex); commands.swap(self->_commands); }
            for (auto &command : commands) command();
        }
    private:
        struct ev_async _async_watcher;
        ev_timer _retry_watcher;
        std::map<uint64_t, Clock::time_point> _retries;
        struct Work { uint64_t tag = 0; std::string body; };
        std::vector<std::deque<Work>> _work;
        static constexpr std::size_t max_work_bytes = 64 * 1024 * 1024;
        std::size_t _work_bytes = 0;
        std::mutex _work_mutex, _commands_mutex, _shutdown_mutex;
        std::condition_variable _work_ready;
        bool _worker_stopping = false;
        std::vector<std::thread> _workers;
        std::deque<std::function<void()>> _commands;
        bool _accepting_commands = true;
        bool _consumer_started = false;
        struct ev_loop *_loop;
        std::unique_ptr<AMQP::LibEvHandler> _handler;
        std::unique_ptr<AMQP::TcpConnection> _connection;
        std::unique_ptr<AMQP::TcpChannel> _channel;
        std::thread _loop_thread;
};
}
