#include <gtest/gtest.h>
#include <set>
#include "message_batch.hpp"

using namespace bite_im;
class BatchTest : public testing::Test {
protected:
    std::set<std::pair<std::string, std::string>> saved;
    std::vector<MessageInfo> indexed, records;
    std::vector<bool> es_result, db_result;
    bool throw_es = false, throw_db = false;
    int singles = 0;
    std::vector<std::string> operations;
    std::string message(std::string id = "one", std::string sender = "sender") {
        MessageInfo msg;
        msg.set_message_id(sender + "-" + id);
        msg.set_client_message_id(id);
        msg.set_chat_session_id("chat");
        msg.mutable_sender()->set_user_id(sender);
        msg.mutable_message()->set_message_type(MessageType::STRING);
        msg.mutable_message()->mutable_string_message()->set_content("中文\ntext");
        return msg.SerializeAsString();
    }
    std::vector<ConsumeResult> run(std::vector<std::string> bodies) {
        MessageBatch batch(
            [&](const MessageInfo &msg) { return saved.count({msg.sender().user_id(), msg.client_message_id()}) != 0; },
            [&](const std::vector<MessageInfo> &messages) {
                indexed = messages;
                if (throw_es) throw std::runtime_error("ES failed");
                return es_result.empty() ? std::vector<bool>(messages.size(), true) : es_result;
            },
            [&](const std::vector<MessageInfo> &messages) {
                records = messages;
                operations.push_back("text");
                if (throw_db) throw std::runtime_error("transaction rolled back");
                return db_result.empty() ? std::vector<bool>(messages.size(), true) : db_result;
            },
            [&](const std::string&) { ++singles; operations.push_back("media"); return ConsumeResult::Retry; });
        return batch.process(bodies);
    }
};
TEST_F(BatchTest, EmptyBatchDoesNotCallDependencies) {
    EXPECT_TRUE(run({}).empty()); EXPECT_TRUE(indexed.empty());
}
TEST_F(BatchTest, CommitsAllValidText) {
    EXPECT_EQ(run({message("a"), message("b")}), std::vector<ConsumeResult>(2, ConsumeResult::Ack));
    EXPECT_EQ(indexed.size(), 2u); EXPECT_EQ(records.size(), 2u);
}
TEST_F(BatchTest, PoisonIsRejectedWithoutPoisoningValidPeer) {
    EXPECT_EQ(run({"\xff", message()}), (std::vector<ConsumeResult>{ConsumeResult::Reject, ConsumeResult::Ack}));
}
TEST_F(BatchTest, DeduplicatesSenderScopedIdentityInsideBatch) {
    EXPECT_EQ(run({message(), message(), message("one", "other")}), std::vector<ConsumeResult>(3, ConsumeResult::Ack));
    EXPECT_EQ(indexed.size(), 2u); EXPECT_EQ(records.size(), 2u);
}
TEST_F(BatchTest, ExistingRecordSkipsBothWrites) {
    saved.emplace("sender", "one");
    EXPECT_EQ(run({message()}), std::vector<ConsumeResult>{ConsumeResult::Ack});
    EXPECT_TRUE(indexed.empty()); EXPECT_TRUE(records.empty());
}
TEST_F(BatchTest, PartialEsFailureOnlyCommitsSuccessfulItems) {
    es_result = {true, false, true};
    EXPECT_EQ(run({message("a"), message("b"), message("c")}),
        (std::vector<ConsumeResult>{ConsumeResult::Ack, ConsumeResult::Retry, ConsumeResult::Ack}));
    ASSERT_EQ(records.size(), 2u); EXPECT_EQ(records.back().client_message_id(), "c");
}
TEST_F(BatchTest, SqlRollbackRetriesAllUncommittedItemsAndDuplicates) {
    throw_db = true;
    EXPECT_EQ(run({message(), message(), message("b")}), std::vector<ConsumeResult>(3, ConsumeResult::Retry));
}
TEST_F(BatchTest, EsExceptionKeepsExistingAckAndPoisonReject) {
    saved.emplace("sender", "done"); throw_es = true;
    EXPECT_EQ(run({message("done"), message(), "\xff"}),
        (std::vector<ConsumeResult>{ConsumeResult::Ack, ConsumeResult::Retry, ConsumeResult::Reject}));
}
TEST_F(BatchTest, MalformedBulkLengthDoesNotCommitAnyCandidate) {
    es_result = {true};
    EXPECT_EQ(run({message("a"), message("b")}), std::vector<ConsumeResult>(2, ConsumeResult::Retry));
    EXPECT_TRUE(records.empty());
}
TEST_F(BatchTest, MalformedSqlLengthDoesNotAckAnyCandidate) {
    db_result = {true};
    EXPECT_EQ(run({message("a"), message("b")}), std::vector<ConsumeResult>(2, ConsumeResult::Retry));
}
TEST_F(BatchTest, PerRecordSqlFailureDoesNotAckItsDuplicates) {
    db_result = {false, true};
    EXPECT_EQ(run({message("a"), message("a"), message("b")}),
        (std::vector<ConsumeResult>{ConsumeResult::Retry, ConsumeResult::Retry, ConsumeResult::Ack}));
}
TEST_F(BatchTest, LegacyClientIdUsesStableServerIdentity) {
    MessageInfo msg; msg.ParseFromString(message()); msg.clear_client_message_id();
    EXPECT_EQ(run({msg.SerializeAsString()}), std::vector<ConsumeResult>{ConsumeResult::Ack});
    ASSERT_EQ(indexed.size(), 1u); EXPECT_EQ(indexed[0].client_message_id(), msg.message_id());
}
TEST_F(BatchTest, MediaFallsBackWithoutEnteringTextBulk) {
    MessageInfo msg; msg.ParseFromString(message());
    msg.mutable_message()->set_message_type(MessageType::IMAGE);
    msg.mutable_message()->mutable_image_message()->set_image_content("image");
    EXPECT_EQ(run({msg.SerializeAsString(), message("text")}),
        (std::vector<ConsumeResult>{ConsumeResult::Retry, ConsumeResult::Ack}));
    EXPECT_EQ(singles, 1); EXPECT_EQ(indexed.size(), 1u);
}
TEST_F(BatchTest, MediaIsABarrierBetweenTextSegments) {
    MessageInfo media; media.ParseFromString(message("image"));
    media.mutable_message()->set_message_type(MessageType::IMAGE);
    media.mutable_message()->mutable_image_message()->set_image_content("image");
    EXPECT_EQ(run({message("first"), media.SerializeAsString(), message("last")}),
        (std::vector<ConsumeResult>{ConsumeResult::Ack, ConsumeResult::Retry, ConsumeResult::Ack}));
    EXPECT_EQ(operations, (std::vector<std::string>{"text", "media", "text"}));
}
TEST_F(BatchTest, OversizedSqlFieldsAreRejectedWithoutPoisoningHealthyPeers) {
    MessageInfo text; text.ParseFromString(message("large"));
    text.mutable_message()->mutable_string_message()->set_content(std::string(65536, 'x'));
    EXPECT_EQ(run({text.SerializeAsString(), message("healthy"), message(std::string(65, 'i'))}),
        (std::vector<ConsumeResult>{ConsumeResult::Reject, ConsumeResult::Ack, ConsumeResult::Reject}));
    EXPECT_EQ(records.size(), 1u);
}
int main(int argc, char **argv) {
    init_logger(false, "", spdlog::level::critical);
    g_default_logger->set_level(spdlog::level::critical);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
