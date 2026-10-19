#include <gtest/gtest.h>
#include <set>
#include <stdexcept>

#include "message_persistence.hpp"

using bite_im::ConsumeResult;
using bite_im::MessageContent;
using bite_im::MessageInfo;
using bite_im::MessagePersistence;
using bite_im::MessageType;

class PersistenceTest : public testing::Test {
protected:
    bool text_ok = true, file_ok = true, record_ok = true;
    int lookups = 0, text_writes = 0, file_writes = 0, record_writes = 0;
    std::string throw_at, lookup_id, saved_id, uploaded_name, uploaded_bytes;
    std::int64_t uploaded_size = 0;
    std::set<std::string> saved;
    MessagePersistence::StoredBody stored;

    MessageInfo message(MessageType type = MessageType::STRING) {
        MessageInfo msg;
        msg.set_message_id("server-id");
        msg.set_client_message_id("client-id");
        msg.set_chat_session_id("session");
        msg.mutable_sender()->set_user_id("sender");
        msg.mutable_message()->set_message_type(type);
        switch (type) {
        case MessageType::STRING:
            msg.mutable_message()->mutable_string_message()->set_content("hello");
            break;
        case MessageType::IMAGE:
            msg.mutable_message()->mutable_image_message()->set_image_content("image");
            break;
        case MessageType::FILE: {
            auto* file = msg.mutable_message()->mutable_file_message();
            file->set_file_name("sample.txt");
            file->set_file_contents("file");
            file->set_file_size(4);
            break;
        }
        case MessageType::SPEECH:
            msg.mutable_message()->mutable_speech_message()->set_file_contents("speech");
            break;
        default: break;
        }
        return msg;
    }

    MessagePersistence pipeline() {
        return MessagePersistence(
            [this](const MessageInfo& msg) {
                ++lookups;
                lookup_id = msg.client_message_id();
                if (throw_at == "lookup") throw std::runtime_error("injected lookup error");
                return saved.count(msg.sender().user_id() + ":" + lookup_id) != 0;
            },
            [this](const MessageInfo&) {
                ++text_writes;
                if (throw_at == "text") throw std::runtime_error("injected ES error");
                return text_ok;
            },
            [this](const std::string& name, const std::string& bytes,
                   std::int64_t size, std::string& id) {
                ++file_writes;
                if (throw_at == "file") throw std::runtime_error("injected file error");
                uploaded_name = name;
                uploaded_bytes = bytes;
                uploaded_size = size;
                id = "file-id";
                return file_ok;
            },
            [this](const MessageInfo& msg, const MessagePersistence::StoredBody& body) {
                ++record_writes;
                if (throw_at == "record") throw std::runtime_error("injected DB error");
                if (!record_ok) return false;
                stored = body;
                saved_id = msg.message_id();
                saved.insert(msg.sender().user_id() + ":" + msg.client_message_id());
                return true;
            });
    }

    ConsumeResult process(const MessageInfo& msg) {
        const auto bytes = msg.SerializeAsString();
        return pipeline().process(bytes.data(), bytes.size());
    }
};

TEST_F(PersistenceTest, TextIsAcknowledgedOnlyAfterBothWrites) {
    EXPECT_EQ(process(message()), ConsumeResult::Ack);
    EXPECT_EQ(text_writes, 1);
    EXPECT_EQ(record_writes, 1);
    EXPECT_EQ(stored.content, "hello");
    EXPECT_EQ(stored.file_size, 0);
    EXPECT_EQ(file_writes, 0);
}

TEST_F(PersistenceTest, ElasticsearchFailureRetriesAndRecovers) {
    text_ok = false;
    EXPECT_EQ(process(message()), ConsumeResult::Retry);
    EXPECT_EQ(record_writes, 0);
    text_ok = true;
    EXPECT_EQ(process(message()), ConsumeResult::Ack);
    EXPECT_EQ(record_writes, 1);
}

TEST_F(PersistenceTest, MysqlFailureRetriesWithStableIdentity) {
    record_ok = false;
    EXPECT_EQ(process(message()), ConsumeResult::Retry);
    EXPECT_TRUE(saved.empty());
    record_ok = true;
    EXPECT_EQ(process(message()), ConsumeResult::Ack);
    EXPECT_EQ(record_writes, 2);
    EXPECT_EQ(saved_id, "server-id");
    EXPECT_EQ(saved.count("sender:client-id"), 1u);
}

TEST_F(PersistenceTest, RedeliveredPersistedMessageDoesNotWriteAgain) {
    EXPECT_EQ(process(message()), ConsumeResult::Ack);
    EXPECT_EQ(process(message()), ConsumeResult::Ack);
    EXPECT_EQ(text_writes, 1);
    EXPECT_EQ(record_writes, 1);
}

TEST_F(PersistenceTest, ClientIdIsScopedToSender) {
    auto first = message();
    auto second = first;
    second.set_message_id("another-server-id");
    second.mutable_sender()->set_user_id("another-sender");
    EXPECT_EQ(process(first), ConsumeResult::Ack);
    EXPECT_EQ(process(second), ConsumeResult::Ack);
    EXPECT_EQ(process(second), ConsumeResult::Ack);
    EXPECT_EQ(saved.size(), 2u);
    EXPECT_EQ(record_writes, 2);
}

TEST_F(PersistenceTest, LegacyMessageUsesServerIdForDeduplication) {
    auto msg = message();
    msg.clear_client_message_id();
    EXPECT_EQ(process(msg), ConsumeResult::Ack);
    EXPECT_EQ(lookup_id, "server-id");
    EXPECT_EQ(process(msg), ConsumeResult::Ack);
    EXPECT_EQ(record_writes, 1);
}

class FilePersistenceTest : public PersistenceTest,
                          public testing::WithParamInterface<MessageType> {};

TEST_P(FilePersistenceTest, FileServiceFailureRetriesBeforeMysql) {
    file_ok = false;
    EXPECT_EQ(process(message(GetParam())), ConsumeResult::Retry);
    EXPECT_EQ(record_writes, 0);
    file_ok = true;
    EXPECT_EQ(process(message(GetParam())), ConsumeResult::Ack);
    EXPECT_EQ(record_writes, 1);
    EXPECT_EQ(stored.file_id, "file-id");
    EXPECT_EQ(text_writes, 0);
    if (GetParam() == MessageType::FILE) {
        EXPECT_EQ(uploaded_name, "sample.txt");
        EXPECT_EQ(uploaded_bytes, "file");
        EXPECT_EQ(stored.file_name, "sample.txt");
        EXPECT_EQ(stored.file_size, 4);
    } else {
        EXPECT_TRUE(uploaded_name.empty());
        EXPECT_EQ(uploaded_size, static_cast<std::int64_t>(uploaded_bytes.size()));
        EXPECT_EQ(stored.file_size, 0);
    }
}
TEST_P(FilePersistenceTest, MysqlFailureAfterUploadStillRequiresRetry) {
    record_ok = false;
    EXPECT_EQ(process(message(GetParam())), ConsumeResult::Retry);
    EXPECT_EQ(file_writes, 1);
    EXPECT_TRUE(saved.empty());
    record_ok = true;
    EXPECT_EQ(process(message(GetParam())), ConsumeResult::Ack);
    EXPECT_EQ(record_writes, 2);
    EXPECT_EQ(saved_id, "server-id");
}
INSTANTIATE_TEST_SUITE_P(Media, FilePersistenceTest,
                        testing::Values(MessageType::IMAGE, MessageType::FILE, MessageType::SPEECH));

class DependencyExceptionTest : public PersistenceTest,
                                public testing::WithParamInterface<const char*> {};
TEST_P(DependencyExceptionTest, ExceptionsRetryAndAllowSubsequentRecovery) {
    throw_at = GetParam();
    const auto msg = message(throw_at == "file" ? MessageType::IMAGE : MessageType::STRING);
    EXPECT_EQ(process(msg), ConsumeResult::Retry);
    throw_at.clear();
    EXPECT_EQ(process(msg), ConsumeResult::Ack);
}
INSTANTIATE_TEST_SUITE_P(Dependencies, DependencyExceptionTest,
                        testing::Values("lookup", "text", "file", "record"));

TEST_F(PersistenceTest, MalformedProtobufIsRejectedBeforeStorage) {
    const std::string bytes(1, '\xff');
    EXPECT_EQ(pipeline().process(bytes.data(), bytes.size()), ConsumeResult::Reject);
    EXPECT_EQ(lookups, 0);
}

TEST_F(PersistenceTest, OversizedPayloadIsRejectedBeforeParsing) {
    EXPECT_EQ(pipeline().process("x", static_cast<std::size_t>(std::numeric_limits<int>::max()) + 1),
              ConsumeResult::Reject);
    EXPECT_EQ(lookups, 0);
}

TEST_F(PersistenceTest, EmptyProtobufIsNotTreatedAsATextMessage) {
    EXPECT_EQ(process(MessageInfo()), ConsumeResult::Reject);
    EXPECT_EQ(lookups, 0);
}

TEST_F(PersistenceTest, MissingIdentityIsRejectedBeforeStorage) {
    auto msg = message();
    msg.clear_message_id();
    EXPECT_EQ(process(msg), ConsumeResult::Reject);
    msg = message();
    msg.clear_chat_session_id();
    EXPECT_EQ(process(msg), ConsumeResult::Reject);
    msg = message();
    msg.clear_sender();
    EXPECT_EQ(process(msg), ConsumeResult::Reject);
    EXPECT_EQ(lookups, 0);
}

TEST_F(PersistenceTest, MissingOrMismatchedContentIsRejectedBeforeStorage) {
    auto msg = message();
    msg.clear_message();
    EXPECT_EQ(process(msg), ConsumeResult::Reject);
    msg = message();
    msg.mutable_message()->set_message_type(MessageType::IMAGE);
    EXPECT_EQ(process(msg), ConsumeResult::Reject);
    EXPECT_EQ(lookups, 0);
}

TEST_F(PersistenceTest, UnknownMessageTypeIsRejectedBeforeStorage) {
    auto msg = message();
    msg.mutable_message()->set_message_type(static_cast<MessageType>(99));
    EXPECT_EQ(process(msg), ConsumeResult::Reject);
    EXPECT_EQ(lookups, 0);
}

int main(int argc, char** argv) {
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
