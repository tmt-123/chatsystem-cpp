#pragma once
#include <chrono>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace bite_im {
// Gateway-local, bounded receipts for live delivery. This is not a durable
// delivery ledger or a read receipt. Offline history does not create receipts.
class DeliveryReceipts {
public:
    using Clock = std::chrono::steady_clock;
    struct Receipt {
        std::string sender, client_id, message_id, chat_id;
        uint32_t delivered = 0;
    };
    explicit DeliveryReceipts(size_t capacity = 10000,
                              std::chrono::milliseconds ttl = std::chrono::minutes(10))
        : _capacity(capacity), _ttl(ttl) {}
    void track(const std::string &sender, const std::string &client_id,
               const std::string &message_id, const std::string &chat_id,
               const std::vector<std::string> &targets, Clock::time_point now = Clock::now()) {
        std::lock_guard<std::mutex> lock(_mutex);
        prune(now);
        if (!_capacity || targets.empty()) return;
        auto it = _pending.find(message_id);
        if (it == _pending.end()) {
            if (_pending.size() >= _capacity) {
                _pending.erase(_order.front().second);
                _order.pop_front();
            }
            it = _pending.emplace(message_id, Pending{{sender, client_id, message_id, chat_id, 0}, {}, {}}).first;
            _order.emplace_back(now + _ttl, message_id);
        }
        if (it->second.receipt.sender != sender || it->second.receipt.client_id != client_id
            || it->second.receipt.chat_id != chat_id) return;
        for (const auto &uid : targets) if (uid != sender) it->second.targets.insert(uid);
    }
    std::optional<Receipt> acknowledge(const std::string &message_id, const std::string &recipient,
                                       Clock::time_point now = Clock::now()) {
        std::lock_guard<std::mutex> lock(_mutex);
        prune(now);
        auto it = _pending.find(message_id);
        if (it == _pending.end() || !it->second.targets.count(recipient)
            || !it->second.acked.insert(recipient).second) return std::nullopt;
        it->second.receipt.delivered = static_cast<uint32_t>(it->second.acked.size());
        return it->second.receipt;
    }
private:
    struct Pending {
        Receipt receipt;
        std::unordered_set<std::string> targets, acked;
    };
    void prune(Clock::time_point now) {
        while (!_order.empty() && _order.front().first <= now) {
            _pending.erase(_order.front().second);
            _order.pop_front();
        }
    }
    size_t _capacity;
    std::chrono::milliseconds _ttl;
    std::mutex _mutex;
    std::unordered_map<std::string, Pending> _pending;
    std::deque<std::pair<Clock::time_point, std::string>> _order;
};
}
