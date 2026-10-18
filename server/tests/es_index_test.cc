#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <thread>
#include "icsearch.hpp"

using bite_im::ESIndex;
using Method = elasticlient::Client::HTTPMethod;
class ESIndexTest : public testing::Test {
protected:
    std::shared_ptr<elasticlient::Client> client;
    std::string name;
    void SetUp() override {
        const char *host = std::getenv("TEST_ES_URL");
        client = std::make_shared<elasticlient::Client>(std::vector<std::string>{host ? host : "http://127.0.0.1:9200/"});
        name = "test-init-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    }
    void TearDown() override { client->performRequest(Method::DELETE, name, ""); }
    Json::Value get(const std::string &path) {
        auto response = client->performRequest(Method::GET, path, "");
        EXPECT_EQ(response.status_code, 200);
        Json::Value value;
        EXPECT_TRUE(bite_im::UnSerialize(response.text, value));
        return value;
    }
};
TEST_F(ESIndexTest, CreatesRealMappingWithoutDummyDocument) {
    ESIndex index(client, name);
    ASSERT_TRUE(index.append("id", "keyword", "standard")
                    .append("time", "long", "standard", false)
                    .append("content", "text", "standard").create());
    auto mapping = get(name + "/_mapping")[name]["mappings"]["properties"];
    EXPECT_EQ(mapping["id"]["type"].asString(), "keyword");
    EXPECT_FALSE(mapping["id"].isMember("analyzer"));
    EXPECT_FALSE(mapping["time"]["index"].asBool());
    EXPECT_FALSE(mapping["time"].isMember("enabled"));
    EXPECT_EQ(get(name + "/_count")["count"].asInt(), 0);
    ASSERT_TRUE(bite_im::ESInsert(client, name).append("id", "alpha").append("content", "hello world").insert("doc"));
    client->performRequest(Method::POST, name + "/_refresh", "");
    EXPECT_EQ(bite_im::ESSearch(client, name).append_must_term("id", "alpha").append_must_match("content", "hello").search().size(), 1);
    EXPECT_TRUE(index.create());
    EXPECT_EQ(get(name + "/_count")["count"].asInt(), 1);
}
TEST_F(ESIndexTest, ReadsLegacyKeywordSubfieldWithoutChangingData) {
    ASSERT_EQ(client->performRequest(Method::PUT, name,
        R"({"mappings":{"properties":{"id":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"content":{"type":"text"}}}})").status_code, 200);
    ASSERT_TRUE(bite_im::ESInsert(client, name).append("id", "alpha-beta").append("content", "hello").insert("doc"));
    ESIndex index(client, name);
    ASSERT_TRUE(index.append("id", "keyword").append("content", "text", "standard").create());
    EXPECT_EQ(index.exactField("id"), "id.keyword");
    client->performRequest(Method::POST, name + "/_refresh", "");
    EXPECT_EQ(bite_im::ESSearch(client, name).append_must_term(index.exactField("id"), "alpha-beta").search().size(), 1);
    EXPECT_EQ(get(name + "/_mapping")[name]["mappings"]["properties"]["id"]["type"].asString(), "text");
}
TEST_F(ESIndexTest, IncompatibleMappingFailsWithoutOverwriting) {
    ASSERT_EQ(client->performRequest(Method::PUT, name,
        R"({"mappings":{"properties":{"id":{"type":"long"}}}})").status_code, 200);
    ESIndex index(client, name);
    EXPECT_FALSE(index.append("id", "keyword").create());
    EXPECT_EQ(get(name + "/_mapping")[name]["mappings"]["properties"]["id"]["type"].asString(), "long");
}
TEST_F(ESIndexTest, MissingAnalyzerFailsRatherThanWritingSettingsAsData) {
    ESIndex index(client, name);
    EXPECT_FALSE(index.append("content", "text", "missing_analyzer_for_test").create());
    EXPECT_EQ(client->performRequest(Method::HEAD, name, "").status_code, 404);
}
TEST_F(ESIndexTest, DisabledLegacyKeywordCannotBeUsedForExactSearch) {
    ASSERT_EQ(client->performRequest(Method::PUT, name,
        R"({"mappings":{"properties":{"id":{"type":"text","fields":{"keyword":{"type":"keyword","index":false}}}}}})").status_code, 200);
    ESIndex index(client, name);
    EXPECT_FALSE(index.append("id", "keyword").create());
}
TEST_F(ESIndexTest, ConcurrentInitializersAgree) {
    std::atomic<int> succeeded{0};
    auto create = [&]() {
        auto connection = std::make_shared<elasticlient::Client>(std::vector<std::string>{
            std::getenv("TEST_ES_URL") ? std::getenv("TEST_ES_URL") : "http://127.0.0.1:9200/"});
        if (ESIndex(connection, name).append("content", "text", "standard").create()) ++succeeded;
    };
    std::thread one(create), two(create);
    one.join(); two.join();
    EXPECT_EQ(succeeded.load(), 2);
}

TEST_F(ESIndexTest, SharedClientConcurrentWritesKeepDocumentIdentity) {
    ASSERT_TRUE(ESIndex(client, name).append("id", "keyword").append("content", "text", "standard").create());
    std::atomic<int> errors{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) threads.emplace_back([&, i] {
        for (int j = 0; j < 20; ++j) {
            const auto id = std::to_string(i) + "-" + std::to_string(j);
            if (!bite_im::ESInsert(client, name).append("id", id).append("content", id).insert(id)) ++errors;
        }
    });
    for (auto &thread : threads) thread.join();
    ASSERT_EQ(errors.load(), 0);
    for (int i = 0; i < 8; ++i) for (int j = 0; j < 20; ++j) {
        const auto id = std::to_string(i) + "-" + std::to_string(j);
        EXPECT_EQ(get(name + "/_doc/" + id)["_source"]["id"].asString(), id);
    }
}
int main(int argc, char** argv) {
    bite_im::init_logger(false, "", spdlog::level::critical);
    bite_im::g_default_logger->set_level(spdlog::level::critical);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
