#include <gtest/gtest.h>
#include "delivery_receipts.hpp"
#include "connection.hpp"

using bite_im::DeliveryReceipts;
using namespace std::chrono_literals;

TEST(DeliveryReceipts, SocketWriteAloneIsNotDelivery) {
    DeliveryReceipts receipts;
    receipts.track("sender", "client", "message", "chat", {"recipient"});
    EXPECT_FALSE(receipts.acknowledge("message", "stranger"));
    EXPECT_FALSE(receipts.acknowledge("message", "sender"));
    EXPECT_FALSE(receipts.acknowledge("unknown", "recipient"));
    auto ack = receipts.acknowledge("message", "recipient");
    ASSERT_TRUE(ack);
    EXPECT_EQ(ack->delivered, 1);
    EXPECT_EQ(ack->sender, "sender");
    EXPECT_EQ(ack->client_id, "client");
    EXPECT_EQ(ack->chat_id, "chat");
}
TEST(DeliveryReceipts, GroupCountsUniqueRecipientsAcrossRetries) {
    DeliveryReceipts receipts;
    receipts.track("a", "c", "m", "chat", {"a", "b", "b", "d"});
    ASSERT_EQ(receipts.acknowledge("m", "b")->delivered, 1);
    EXPECT_FALSE(receipts.acknowledge("m", "b"));
    receipts.track("a", "c", "m", "chat", {"b", "d"});
    EXPECT_FALSE(receipts.acknowledge("m", "b"));
    ASSERT_EQ(receipts.acknowledge("m", "d")->delivered, 2);
}
TEST(DeliveryReceipts, ReusedIdCannotChangeOwnerOrTargets) {
    DeliveryReceipts receipts;
    receipts.track("a", "c", "m", "chat", {"b"});
    receipts.track("attacker", "c", "m", "chat", {"stranger"});
    EXPECT_FALSE(receipts.acknowledge("m", "stranger"));
    EXPECT_EQ(receipts.acknowledge("m", "b")->sender, "a");
}
TEST(DeliveryReceipts, ExpiredAndEvictedEntriesNeverConfirmDelivery) {
    DeliveryReceipts receipts(1, 10ms);
    auto now = DeliveryReceipts::Clock::now();
    receipts.track("a", "c", "first", "chat", {"b"}, now);
    receipts.track("a", "d", "second", "chat", {"b"}, now);
    EXPECT_FALSE(receipts.acknowledge("first", "b", now));
    EXPECT_FALSE(receipts.acknowledge("second", "b", now + 10ms));
}
TEST(Connections, ClosingOldSocketKeepsReplacement) {
    bite_im::server_t server;
    server.init_asio();
    websocketpp::lib::error_code ec;
    auto old = server.get_connection();
    auto current = server.get_connection();
    bite_im::Connection connections;
    connections.insert(old, "user", "old-session");
    EXPECT_EQ(connections.insert(current, "user", "new-session"), old);
    EXPECT_FALSE(connections.isCurrent(old));
    EXPECT_FALSE(connections.remove(old));
    EXPECT_EQ(connections.connection("user"), current);
    EXPECT_TRUE(connections.remove(current));
    EXPECT_FALSE(connections.connection("user"));
}
int main(int argc, char** argv) {
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
