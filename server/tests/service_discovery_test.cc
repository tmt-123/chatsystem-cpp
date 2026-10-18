#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include "channel.hpp"
#include "etcd.hpp"

TEST(ServiceChannelTest, DuplicatePutDoesNotLeaveStaleChannel) {
    bite_im::ServiceChannel channels("service");
    for (int i = 0; i < 10; ++i) channels.append("instance", "127.0.0.1:20101");
    ASSERT_TRUE(channels.choose());
    channels.remove("instance");
    EXPECT_FALSE(channels.choose());
}
TEST(ServiceChannelTest, EndpointReplacementAndSharedHostUseInstanceIdentity) {
    bite_im::ServiceChannel channels("service");
    channels.append("one", "127.0.0.1:20101");
    const auto old = channels.choose();
    channels.append("one", "127.0.0.1:20102");
    EXPECT_NE(old, channels.choose());
    channels.append("two", "127.0.0.1:20102");
    channels.remove("one");
    EXPECT_TRUE(channels.choose());
    channels.remove("two");
    EXPECT_FALSE(channels.choose());
}
TEST(ServiceChannelTest, EmptyDeleteValueStillRemovesTheInstance) {
    bite_im::ServiceManager manager;
    manager.declared("/test/user");
    manager.onServiceOnline("/test/user/one", "127.0.0.1:20101");
    ASSERT_TRUE(manager.choose("/test/user"));
    manager.onServiceOffline("/test/user/one", "");
    EXPECT_FALSE(manager.choose("/test/user"));
}
TEST(ServiceRegistryTest, RevokedLeaseIsReacquiredWithoutRestart) {
    const auto host = std::getenv("TEST_ETCD_URL");
    if (!host) GTEST_SKIP() << "TEST_ETCD_URL is required for this isolated integration test";
    const auto key = "/test-registry/" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    etcd::Client observer(host);
    {
        bite_im::Registry registry(host);
        ASSERT_TRUE(registry.registry(key, "127.0.0.1:20101"));
        const auto initial = observer.get(key).get();
        ASSERT_TRUE(initial.is_ok());
        const auto old_lease = initial.value().lease();
        ASSERT_TRUE(observer.leaserevoke(old_lease).get().is_ok());
        bool recovered = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
        while (std::chrono::steady_clock::now() < deadline) {
            auto response = observer.get(key).get();
            if (response.is_ok() && response.value().lease() != old_lease &&
                response.value().as_string() == "127.0.0.1:20101") { recovered = true; break; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        EXPECT_TRUE(recovered);
    }
    observer.rm(key).get();
}
int main(int argc, char **argv) {
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
