#pragma once

#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <utility>

#include "base.pb.h"
#include "consume_result.hpp"
#include "logger.hpp"

namespace bite_im {

// The MQ persistence pipeline, independent of the RPC server and storage clients.
class MessagePersistence {
public:
    struct StoredBody {
        std::string content;
        std::string file_id;
        std::string file_name;
        std::int64_t file_size = 0;
    };

    using Exists = std::function<bool(const MessageInfo&)>;
    using SaveText = std::function<bool(const MessageInfo&)>;
    using SaveFile = std::function<bool(const std::string&, const std::string&,
                                      std::int64_t, std::string&)>;
    using SaveRecord = std::function<bool(const MessageInfo&, const StoredBody&)>;

    MessagePersistence(Exists exists, SaveText save_text, SaveFile save_file,
                       SaveRecord save_record)
        : _exists(std::move(exists)), _save_text(std::move(save_text)),
          _save_file(std::move(save_file)), _save_record(std::move(save_record)) {}

    ConsumeResult process(const char* body, std::size_t size) const {
        MessageInfo message;
        if (size > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
            !message.ParseFromArray(body, static_cast<int>(size)) ||
            message.message_id().empty() || message.chat_session_id().empty() ||
            message.sender().user_id().empty() || !validContent(message)) {
            LOG_ERROR("拒绝无效的 MQ 消息，不进行重投");
            return ConsumeResult::Reject;
        }
        if (message.client_message_id().empty()) {
            // Compatibility with messages queued before client IDs were added.
            message.set_client_message_id(message.message_id());
        }

        try {
            if (_exists(message)) {
                return ConsumeResult::Ack;
            }

            StoredBody stored;
            const auto& content = message.message();
            switch (content.message_type()) {
            case MessageType::STRING:
                stored.content = content.string_message().content();
                if (!_save_text(message)) return ConsumeResult::Retry;
                break;
            case MessageType::IMAGE: {
                const auto& bytes = content.image_message().image_content();
                if (!_save_file("", bytes, bytes.size(), stored.file_id)) {
                    return ConsumeResult::Retry;
                }
                break;
            }
            case MessageType::FILE: {
                const auto& file = content.file_message();
                stored.file_name = file.file_name();
                stored.file_size = file.file_size();
                if (!_save_file(stored.file_name, file.file_contents(),
                                stored.file_size, stored.file_id)) {
                    return ConsumeResult::Retry;
                }
                break;
            }
            case MessageType::SPEECH: {
                const auto& bytes = content.speech_message().file_contents();
                if (!_save_file("", bytes, bytes.size(), stored.file_id)) {
                    return ConsumeResult::Retry;
                }
                break;
            }
            default:
                return ConsumeResult::Reject;
            }

            return _save_record(message, stored) ? ConsumeResult::Ack : ConsumeResult::Retry;
        } catch (...) {
            // Never let a dependency exception turn into a successful MQ ACK.
            LOG_ERROR("消息持久化抛出异常，保留消息以便重试");
            return ConsumeResult::Retry;
        }
    }

private:
    static bool validContent(const MessageInfo& message) {
        if (!message.has_message()) return false;
        const auto& content = message.message();
        switch (content.message_type()) {
        case MessageType::STRING: return content.has_string_message();
        case MessageType::IMAGE: return content.has_image_message();
        case MessageType::FILE: return content.has_file_message();
        case MessageType::SPEECH: return content.has_speech_message();
        default: return false;
        }
    }

    Exists _exists;
    SaveText _save_text;
    SaveFile _save_file;
    SaveRecord _save_record;
};

} // namespace bite_im
