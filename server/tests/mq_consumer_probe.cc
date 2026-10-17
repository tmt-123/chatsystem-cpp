#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <thread>

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
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);

    std::map<std::string, int> attempts, text_writes, record_writes, file_writes;
    std::set<std::string> saved;
    bite_im::MessagePersistence persistence(
        [&](const std::string& id) { return saved.count(id) != 0; },
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
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(25);
    while (!stop_requested && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return 0;
}
