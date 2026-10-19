#include "icsearch.hpp"
#include "es_bulk_result.hpp"
#include "user.hxx"
#include "message.hxx"

namespace bite_im {
    class ESClientFactory {
        public:
            static std::shared_ptr<elasticlient::Client> create(const std::vector<std::string> host_list) {
                return std::make_shared<elasticlient::Client>(host_list);
            }
    };
    class ESUser {
        public:
            using ptr = std::shared_ptr<ESUser>;
            ESUser(const std::shared_ptr<elasticlient::Client> &client):
                _index_name(ESIndex::environment("SOARIM_ES_USER_INDEX", "user")), _es_client(client){}
            bool createIndex() {
                ESIndex index(_es_client, _index_name);
                bool ret = index
                    .append("user_id", "keyword", "standard", true)
                    .append("nickname")
                    .append("phone", "keyword", "standard", true)
                    .append("description", "text", "standard", false)
                    .append("avatar_id", "keyword", "standard", false)
                    .create();
                if (ret == false) {
                    LOG_INFO("用户信息索引创建失败!");
                    return false;
                }
                LOG_INFO("用户信息索引创建成功!");
                _uid_field = index.exactField("user_id");
                _phone_field = index.exactField("phone");
                return true;
            }
            bool appendData(const std::string &uid,
                const std::string &phone,
                const std::string &nickname,
                const std::string &description,
                const std::string &avatar_id) {
                bool ret = ESInsert(_es_client, _index_name)
                    .append("user_id", uid)
                    .append("nickname", nickname)
                    .append("phone", phone)
                    .append("description", description)
                    .append("avatar_id", avatar_id)
                    .insert(uid);
                if (ret == false) {
                    LOG_ERROR("用户数据插入/更新失败!");
                    return false;
                }
                LOG_INFO("用户数据新增/更新成功!");
                return true;
            }
            std::vector<User> search(const std::string &key, const std::vector<std::string> &uid_list) {
                std::vector<User> res;
                Json::Value json_user = ESSearch(_es_client, _index_name)
                    .append_should_match(_phone_field, key)
                    .append_should_match(_uid_field, key)
                    .append_should_match("nickname", key)
                    .append_must_not_terms(_uid_field, uid_list)
                    .search();
                if (json_user.isArray() == false) {
                    LOG_ERROR("用户搜索结果为空，或者结果不是数组类型");
                    return res;
                }
                int sz = json_user.size();
                LOG_DEBUG("检索结果条目数量：{}", sz);
                for (int i = 0; i < sz; i++) {
                    User user;
                    user.user_id(json_user[i]["_source"]["user_id"].asString());
                    user.nickname(json_user[i]["_source"]["nickname"].asString());
                    user.description(json_user[i]["_source"]["description"].asString());
                    user.phone(json_user[i]["_source"]["phone"].asString());
                    user.avatar_id(json_user[i]["_source"]["avatar_id"].asString());
                    res.push_back(user);
                }
                return res;
            }
        private:
            // const std::string _uid_key = "user_id";
            // const std::string _desc_key = "user_id";
            // const std::string _phone_key = "user_id";
            // const std::string _name_key = "user_id";
            // const std::string _avatar_key = "user_id";
            std::string _index_name, _uid_field = "user_id", _phone_field = "phone";
            std::shared_ptr<elasticlient::Client> _es_client;
    };

    class ESMessage {
        public:
            using ptr = std::shared_ptr<ESMessage>;
            struct TextDocument {
                std::string user_id, message_id, session_id, content;
                int64_t timestamp;
            };
            std::vector<bool> appendBatch(const std::vector<TextDocument> &documents) {
                std::vector<bool> failed(documents.size(), false);
                if (documents.empty()) return failed;
                Json::StreamWriterBuilder writer;
                writer["indentation"] = "";
                writer["emitUTF8"] = true;
                std::string body;
                std::vector<std::string> ids;
                for (const auto &document : documents) {
                    Json::Value action, source;
                    action["index"]["_id"] = document.message_id;
                    source["user_id"] = document.user_id;
                    source["message_id"] = document.message_id;
                    source["chat_session_id"] = document.session_id;
                    source["content"] = document.content;
                    source["create_time"] = static_cast<Json::Int64>(document.timestamp);
                    body += Json::writeString(writer, action) + "\n" + Json::writeString(writer, source) + "\n";
                    ids.push_back(document.message_id);
                }
                try {
                    const auto mutex = esClientMutex(_es_client);
                    std::lock_guard<std::mutex> lock(*mutex);
                    const auto response = _es_client->performRequest(
                        elasticlient::Client::HTTPMethod::POST, _index_name + "/_bulk", body);
                    if (response.status_code < 200 || response.status_code >= 300) return failed;
                    Json::Value decoded;
                    if (!UnSerialize(response.text, decoded)) return failed;
                    return esBulkResult(decoded, ids);
                } catch (...) {
                    LOG_WARN("ES Bulk 请求失败，保留消息重试");
                    return failed;
                }
            }
            ESMessage(const std::shared_ptr<elasticlient::Client> &es_client):
                _index_name(ESIndex::environment("SOARIM_ES_MESSAGE_INDEX", "message")), _es_client(es_client){}
            bool createIndex() {
                ESIndex index(_es_client, _index_name);
                bool ret = index
                    .append("user_id", "keyword", "standard", false)
                    .append("message_id", "keyword", "standard", false)
                    .append("create_time", "long", "standard", false)
                    .append("chat_session_id", "keyword", "standard", true)
                    .append("content")
                    .create();
                if (ret == false) {
                    LOG_INFO("消息信息索引创建失败!");
                    return false;
                }
                LOG_INFO("消息信息索引创建成功!");
                _session_field = index.exactField("chat_session_id");
                return true;
            }
            bool appendData(const std::string &user_id,
                const std::string &message_id,
                const long create_time,
                const std::string &chat_session_id,
                const std::string &content) {
                bool ret = ESInsert(_es_client, _index_name)
                    .append("message_id", message_id)
                    .append("create_time", create_time)
                    .append("user_id", user_id)
                    .append("chat_session_id", chat_session_id)
                    .append("content", content)
                    .insert(message_id);
                if (ret == false) {
                    LOG_ERROR("消息数据插入/更新失败!");
                    return false;
                }
                LOG_INFO("消息数据新增/更新成功!");
                return true;
            }
            bool remove(const std::string &mid) {
                bool ret = ESRemove(_es_client, _index_name).remove(mid);
                if (ret == false) {
                    LOG_ERROR("消息数据删除失败!");
                    return false;
                }
                LOG_INFO("消息数据删除成功!");
                return true;
            }
            std::vector<bite_im::Message> search(const std::string &key, const std::string &ssid) {
                std::vector<bite_im::Message> res;
                Json::Value json_user = ESSearch(_es_client, _index_name)
                    .append_must_term(_session_field, ssid)
                    .append_must_match("content", key)
                    .search();
                if (json_user.isArray() == false) {
                    LOG_ERROR("用户搜索结果为空，或者结果不是数组类型");
                    return res;
                }
                int sz = json_user.size();
                LOG_DEBUG("检索结果条目数量：{}", sz);
                for (int i = 0; i < sz; i++) {
                    bite_im::Message message;
                    message.user_id(json_user[i]["_source"]["user_id"].asString());
                    message.message_id(json_user[i]["_source"]["message_id"].asString());
                    boost::posix_time::ptime ctime(boost::posix_time::from_time_t(
                        json_user[i]["_source"]["create_time"].asInt64()));
                    message.create_time(ctime);
                    message.session_id(json_user[i]["_source"]["chat_session_id"].asString());
                    message.content(json_user[i]["_source"]["content"].asString());
                    res.push_back(message);
                }
                return res;
            }
        private:
            std::string _index_name, _session_field = "chat_session_id";
            std::shared_ptr<elasticlient::Client> _es_client;
    };
}
