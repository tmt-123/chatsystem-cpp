#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include "data_redis.hpp"

using namespace std::chrono_literals;
class SessionLeaseTest : public testing::Test {
protected:
    std::shared_ptr<sw::redis::Redis> redis;
    std::unique_ptr<bite_im::Session> sessions;
    std::string user, first, second;
    void SetUp() override {
        const char* host = std::getenv("TEST_REDIS_HOST");
        redis = bite_im::RedisClientFactory::create(host ? host : "127.0.0.1", 6379, 0, false);
        user = "test-lease-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
        first = user + "-first";
        second = user + "-second";
        sessions = std::make_unique<bite_im::Session>(redis, 80, 200, 1000);
    }
    void TearDown() override {
        redis->del({user, first, second, "session:user:" + user});
    }
};
TEST_F(SessionLeaseTest, ConcurrentLoginHasOneWinner) {
    std::atomic<int> winners{0};
    std::thread one([&] { if (sessions->create(first, user)) ++winners; });
    std::thread two([&] { if (sessions->create(second, user)) ++winners; });
    one.join(); two.join();
    EXPECT_EQ(winners.load(), 1);
    EXPECT_GT(redis->pttl(user), 0);
}
TEST_F(SessionLeaseTest, LoginWithoutWebsocketExpiresAndInvalidatesOldToken) {
    ASSERT_TRUE(sessions->create(first, user));
    EXPECT_FALSE(sessions->create(second, user));
    std::this_thread::sleep_for(120ms);
    ASSERT_TRUE(sessions->create(second, user));
    EXPECT_FALSE(sessions->uid(first));
    EXPECT_EQ(*sessions->uid(second), user);
}
TEST_F(SessionLeaseTest, OldCloseCannotReleaseNewLogin) {
    ASSERT_TRUE(sessions->create(first, user));
    sessions->release(first, user);
    ASSERT_TRUE(sessions->create(second, user));
    sessions->release(first, user);
    EXPECT_FALSE(sessions->renew(first, user));
    EXPECT_EQ(*redis->get(user), second);
    EXPECT_TRUE(sessions->renew(second, user));
}
TEST_F(SessionLeaseTest, DisconnectAllowsReconnectWithSameToken) {
    ASSERT_TRUE(sessions->create(first, user));
    ASSERT_TRUE(sessions->renew(first, user));
    sessions->release(first, user);
    EXPECT_TRUE(sessions->uid(first));
    EXPECT_TRUE(sessions->renew(first, user));
    EXPECT_FALSE(sessions->create(second, user));
}
TEST_F(SessionLeaseTest, LegacyPermanentMarkerDoesNotLockOutUser) {
    redis->set(user, "");
    ASSERT_TRUE(sessions->create(first, user));
    EXPECT_GT(redis->pttl(user), 0);
    EXPECT_GT(redis->pttl(first), 0);
}
TEST_F(SessionLeaseTest, TokenExpiryAndStaleRemovalAreSafe) {
    bite_im::Session short_session(redis, 50, 50, 70);
    ASSERT_TRUE(short_session.create(first, user));
    std::this_thread::sleep_for(100ms);
    EXPECT_FALSE(short_session.uid(first));
    EXPECT_FALSE(short_session.renew(first, user));
    ASSERT_TRUE(sessions->create(second, user));
    sessions->remove(first);
    EXPECT_EQ(*sessions->uid(second), user);
}
int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
