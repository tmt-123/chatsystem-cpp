#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>
#include <mutex>

#include "consume_result.hpp"
#include "message_persistence.hpp"
#include "rabbitmq.hpp"

namespace {
volatile std::sig_atomic_t stop_requested = 0;
void stop(int) { stop_requested = 1; }
}

// Storage failures are injected, but delivery, ACK/reject, QoS and timers use
// the production MQClient against a real RabbitMQ broker.
int main(int argc, char** argv) {
    if (argc < 2) return 2;
    std::signal(SIGTERM, stop);
    std::signal(SIGINT, stop);
    const bool recovering = argc > 2 && std::string(argv[2]) == "recover";
    const uint16_t prefetch = argc > 3 ? static_cast<uint16_t>(std::stoi(argv[3])) : 1;
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);

    if (argc > 2 && std::string(argv[2]) == "publish") {
        bite_im::MQClient publisher("guest", "guest", "127.0.0.1:5672");
        std::atomic<int> errors{0};
        std::vector<std::thread> threads;
        for (int i = 0; i < 8; ++i) threads.emplace_back([&, i] {
            for (int j = 0; j < 100; ++j) {
                if (!publisher.publish("", "pub:" + std::to_string(i) + ":" + std::to_string(j), argv[1])) ++errors;
            }
        });
        for (auto &thread : threads) thread.join();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        publisher.shutdown();
        try { publisher.publish("", "must-not-hang", argv[1]); return 4; }
        catch (const std::logic_error&) {}
        return errors == 0 ? 0 : 3;
    }

    if (argc > 2 && std::string(argv[2]) == "partition") {
        bite_im::MQClient consumer("guest", "guest", "127.0.0.1:5672");
        std::atomic<int> active{0}, peak{0}, completed{0}, violations{0};
        std::vector<int> next(4, 0);
        std::mutex output;
        consumer.consumePartitioned(argv[1], [&](const char *body, size_t size, std::size_t shard) {
            bite_im::MessageInfo msg;
            if (!msg.ParseFromArray(body, static_cast<int>(size))) return bite_im::ConsumeResult::Reject;
            const int count = ++active;
            int observed = peak.load();
            while (observed < count && !peak.compare_exchange_weak(observed, count)) {}
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            if (std::stoi(msg.chat_session_id()) != static_cast<int>(shard) ||
                std::stoi(msg.message().string_message().content()) != next[shard]++) ++violations;
            { std::lock_guard<std::mutex> lock(output); std::cout << "STORED " << msg.client_message_id() << std::endl; }
            --active;
            ++completed;
            return bite_im::ConsumeResult::Ack;
        }, 4, 16, [](const char *body, size_t size) -> std::size_t {
            bite_im::MessageInfo msg;
            return msg.ParseFromArray(body, static_cast<int>(size)) ? std::stoul(msg.chat_session_id()) : 0;
        });
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
        while (completed < 160 && !stop_requested && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        consumer.shutdown();
        std::cout << "RESULT " << peak << ' ' << violations << ' ' << completed << std::endl;
        return violations == 0 && completed == 160 && peak > 1 ? 0 : 5;
    }

    std::map<std::string, int> attempts, text_writes, record_writes, file_writes;
    std::set<std::string> saved;
    bite_im::MessagePersistence persistence(
        [&](const bite_im::MessageInfo& msg) { return saved.count(msg.client_message_id()) != 0; },
        [&](const bite_im::MessageInfo& msg) {
            const auto& mode = msg.message().string_message().content();
            return !(mode == "es-retry" && ++text_writes[msg.client_message_id()] == 1);
        },
        [&](const std::string&, const std::string& bytes, std::int64_t, std::string& id) {
            if (bytes == "file-retry" && ++file_writes[bytes] == 1) return false;
            id = "file-id";
            return true;
        },
        [&](const bite_im::MessageInfo& msg, const bite_im::MessagePersistence::StoredBody&) {
            const auto& mode = msg.message().string_message().content();
            if (mode == "mysql-retry" && ++record_writes[msg.client_message_id()] == 1) return false;
            saved.insert(msg.client_message_id());
            std::cout << "STORED " << msg.client_message_id() << std::endl;
            return true;
        });

    bite_im::MQClient client("guest", "guest", "127.0.0.1:5672");
    client.consume(argv[1], [&](const char* body, std::size_t size) {
        bite_im::MessageInfo msg;
        const bool parsed = msg.ParseFromArray(body, static_cast<int>(size));
        const std::string key = parsed ? msg.client_message_id() : "invalid";
        const int attempt = ++attempts[key];
        const auto& mode = msg.message().string_message().content();
        if ((mode == "throw-once" || mode == "throw-nonstd-once") && attempt == 1) {
            std::cout << "EVENT " << key << ' ' << attempt << " exception" << std::endl;
            if (mode == "throw-once") throw std::runtime_error("injected callback error");
            throw 7;
        }
        const auto result = mode == "always-retry" && !recovering
            ? bite_im::ConsumeResult::Retry : persistence.process(body, size);
        const char* label = result == bite_im::ConsumeResult::Ack ? "ack" :
                            result == bite_im::ConsumeResult::Reject ? "reject" : "retry";
        std::cout << "EVENT " << key << ' ' << attempt << ' ' << label << std::endl;
        return result;
    }, prefetch);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(25);
    while (!stop_requested && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return 0;
}
