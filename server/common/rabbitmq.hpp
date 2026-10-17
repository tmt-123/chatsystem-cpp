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
#include "consume_result.hpp"
#include "logger.hpp"

namespace bite_im{
class MQClient {
    public:
        using MessageCallback = std::function<ConsumeResult(const char*, size_t)>;
        using ptr = std::shared_ptr<MQClient>;
        MQClient(const std::string &user, 
            const std::string passwd,
            const std::string host) {
            _loop = ev_loop_new(EVFLAG_AUTO);
            if (!_loop) throw std::runtime_error("Cannot create MQ event loop");
            ev_async_init(&_async_watcher, watcher_callback);
            ev_async_start(_loop, &_async_watcher);
            ev_timer_init(&_retry_watcher, retry_callback, 0., 0.);
            _retry_watcher.data = this;
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
             ev_async_send(_loop, &_async_watcher);
             _loop_thread.join();
             ev_timer_stop(_loop, &_retry_watcher);
             ev_async_stop(_loop, &_async_watcher);
             _channel.reset();
             _connection.reset();
             _handler.reset();
             ev_loop_destroy(_loop);
             _loop = nullptr;
        }
        void declareComponents(const std::string &exchange,
            const std::string &queue,
            const std::string &routing_key = "routing_key",
            AMQP::ExchangeType echange_type = AMQP::ExchangeType::direct) {
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
        }
        bool publish(const std::string &exchange, 
            const std::string &msg, 
            const std::string &routing_key = "routing_key") {
            LOG_DEBUG("向交换机 {}-{} 发布消息！", exchange, routing_key);
            bool ret = _channel->publish(exchange, routing_key, msg);
            if (ret == false) {
                LOG_ERROR("{} 发布消息失败：", exchange);
                return false;
            }
            return true;
        }
        void consume(const std::string &queue, const MessageCallback &cb) {
            if (_consumer_started) {
                throw std::logic_error("MQClient supports one consumer per connection");
            }
            _consumer_started = true;
            LOG_DEBUG("开始订阅 {} 队列消息！", queue);
            // One outstanding delivery makes the retry timer bounded and applies
            // backpressure while a storage dependency is unavailable.
            _channel->setQos(1);
            _channel->consume(queue, "consume-tag")  //返回值 DeferredConsumer
                .onReceived([this, cb](const AMQP::Message &message, 
                    uint64_t deliveryTag, 
                    bool redelivered) {
                    ConsumeResult result = ConsumeResult::Retry;
                    try {
                        result = cb(message.body(), message.bodySize());
                    } catch (...) {
                        LOG_ERROR("MQ 消费回调抛出异常，稍后重投");
                    }
                    if (result == ConsumeResult::Ack) {
                        _channel->ack(deliveryTag);
                    } else if (result == ConsumeResult::Reject) {
                        // A configured broker DLX receives rejected poison messages.
                        _channel->reject(deliveryTag);
                    } else {
                        LOG_WARN("消息处理失败，延迟重投 tag={} redelivered={}",
                                 deliveryTag, redelivered);
                        _retry_delivery_tag = deliveryTag;
                        // Keep it unacknowledged for one second, without blocking
                        // the I/O loop, before asking the broker to requeue it.
                        ev_timer_set(&_retry_watcher, 1., 0.);
                        ev_timer_start(_loop, &_retry_watcher);
                    }
                })
                .onError([queue](const char *message){
                    LOG_ERROR("订阅 {} 队列消息失败: {}", queue, message);
                    exit(0);
                });
        }
    private:
        static void retry_callback(struct ev_loop *, ev_timer *watcher, int) {
            auto *self = static_cast<MQClient*>(watcher->data);
            if (!self->_channel->reject(self->_retry_delivery_tag, AMQP::requeue)) {
                LOG_ERROR("MQ 重投请求发送失败，消息仍未确认");
            }
        }
        static void watcher_callback(struct ev_loop *loop, ev_async *watcher, int32_t revents) {
            ev_break(loop, EVBREAK_ALL);
        }
    private:
        struct ev_async _async_watcher;
        ev_timer _retry_watcher;
        uint64_t _retry_delivery_tag = 0;
        bool _consumer_started = false;
        struct ev_loop *_loop;
        std::unique_ptr<AMQP::LibEvHandler> _handler;
        std::unique_ptr<AMQP::TcpConnection> _connection;
        std::unique_ptr<AMQP::TcpChannel> _channel;
        std::thread _loop_thread;
};
}
