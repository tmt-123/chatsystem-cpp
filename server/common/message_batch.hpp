#pragma once
#include <map>
#include <vector>
#include "message_persistence.hpp"

namespace bite_im {
// Text-only microbatch; media retains the existing per-message pipeline.
// Every result belongs to exactly one delivery, never to an ACK range.
class MessageBatch {
public:
    using BulkText = std::function<std::vector<bool>(const std::vector<MessageInfo>&)>;
    using Records = std::function<std::vector<bool>(const std::vector<MessageInfo>&)>;
    using Single = std::function<ConsumeResult(const std::string&)>;
    MessageBatch(MessagePersistence::Exists exists, BulkText text, Records records, Single single)
        : _exists(std::move(exists)), _text(std::move(text)),
          _records(std::move(records)), _single(std::move(single)) {}

    std::vector<ConsumeResult> process(const std::vector<std::string> &bodies) const {
        std::vector<ConsumeResult> result(bodies.size(), ConsumeResult::Retry);
        std::vector<MessageInfo> pending;
        std::vector<std::vector<std::size_t>> positions;
        std::map<std::pair<std::string, std::string>, std::size_t> keys;
        auto flush = [&] {
            if (pending.empty()) return;
            try {
                const auto indexed = _text(pending);
                if (indexed.size() == pending.size()) {
                    std::vector<MessageInfo> ready;
                    std::vector<std::size_t> ready_positions;
                    for (std::size_t i = 0; i < pending.size(); ++i) if (indexed[i]) {
                        ready.push_back(std::move(pending[i]));
                        ready_positions.push_back(i);
                    }
                    if (!ready.empty()) {
                        const auto committed = _records(ready);
                        if (committed.size() == ready.size())
                            for (std::size_t i = 0; i < ready.size(); ++i) if (committed[i])
                                for (auto original : positions[ready_positions[i]]) result[original] = ConsumeResult::Ack;
                    }
                }
            } catch (...) { LOG_WARN("批量持久化异常，未确认投递将重试"); }
            pending.clear(); positions.clear(); keys.clear();
        };
        for (std::size_t i = 0; i < bodies.size(); ++i) {
            MessageInfo message;
            if (!MessagePersistence::decode(bodies[i].data(), bodies[i].size(), message)) {
                result[i] = ConsumeResult::Reject;
                continue;
            }
            try {
                if (message.message().message_type() != MessageType::STRING) {
                    // A media operation is a barrier: don't commit later media
                    // ahead of earlier text just because text is aggregated.
                    flush();
                    result[i] = _single(bodies[i]);
                    continue;
                }
                const auto key = std::make_pair(message.sender().user_id(), message.client_message_id());
                auto found = keys.find(key);
                if (found != keys.end()) {
                    positions[found->second].push_back(i);
                    continue;
                }
                if (_exists(message)) { result[i] = ConsumeResult::Ack; continue; }
                keys.emplace(key, pending.size());
                pending.push_back(std::move(message));
                positions.push_back({i});
            } catch (...) { /* only this delivery retries */ }
        }
        flush();
        return result;
    }
private:
    MessagePersistence::Exists _exists;
    BulkText _text;
    Records _records;
    Single _single;
};
} // namespace bite_im
