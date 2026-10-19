#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include "connection.hpp"
#include "delivery_receipts.hpp"
#include "channel.hpp"

int main() {
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    constexpr int threads = 12, loops = 10000;
    std::atomic<int> violations{0};
    bite_im::server_t endpoint;
    endpoint.init_asio();
    std::vector<bite_im::server_t::connection_ptr> sockets;
    for (int i = 0; i < threads*2; ++i) sockets.push_back(endpoint.get_connection());
    bite_im::Connection connections;
    std::vector<std::thread> workers;
    auto begin = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t) workers.emplace_back([&,t] {
        const std::string uid = "user-" + std::to_string(t);
        for (int i = 0; i < loops; ++i) {
            connections.insert(sockets[2*t], uid, "old");
            connections.insert(sockets[2*t+1], uid, "new");
            if (connections.remove(sockets[2*t])) ++violations;
            if (connections.connection(uid) != sockets[2*t+1]) ++violations;
            if (!connections.remove(sockets[2*t+1])) ++violations;
        }
    });
    for (auto &worker : workers) worker.join();
    workers.clear();
    for (int t = 0; t < threads; ++t) workers.emplace_back([&,t] {
        for (int i = 0; i < loops; ++i) {
            connections.insert(sockets[2*t], "shared-user", "session");
            connections.remove(sockets[2*t]);
        }
    });
    for (auto &worker : workers) worker.join();
    for (auto &socket : sockets) connections.remove(socket);
    if (connections.connection("shared-user")) ++violations;
    workers.clear();
    bite_im::DeliveryReceipts receipts(threads*loops + 1);
    for (int t = 0; t < threads; ++t) workers.emplace_back([&,t] {
        for (int i = 0; i < loops; ++i) {
            const std::string id = std::to_string(t) + "-" + std::to_string(i);
            receipts.track("sender", id, id, "chat", {"receiver"});
            if (receipts.acknowledge(id, "stranger")) ++violations;
            const auto ack = receipts.acknowledge(id, "receiver");
            if (!ack || ack->delivered != 1) ++violations;
            if (receipts.acknowledge(id, "receiver")) ++violations;
        }
    });
    for (auto &worker : workers) worker.join();
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now()-begin).count();
    bool stale_channel = false;
    bool channel_probe_executed = false;
#ifndef COMPONENTS_ONLY
    channel_probe_executed = true;
    bite_im::ServiceChannel channel("test-service");
    channel.append("127.0.0.1:29998");
    channel.append("127.0.0.1:29998");
    channel.remove("127.0.0.1:29998");
    stale_channel = bool(channel.choose());
#endif
    std::cout << "{\"threads\":" << threads << ",\"iterations_per_thread\":" << loops
        << ",\"connection_replacements\":" << threads*loops
        << ",\"same_user_cycles\":" << threads*loops
        << ",\"messages_acknowledged\":" << threads*loops
        << ",\"invariant_violations\":" << violations.load()
        << ",\"seconds\":" << seconds
        << ",\"channel_probe_executed\":" << (channel_probe_executed ? "true" : "false")
        << ",\"duplicate_registration_leaves_stale_channel\":" << (stale_channel ? "true" : "false") << "}\n";
    return violations ? 1 : 0;
}
