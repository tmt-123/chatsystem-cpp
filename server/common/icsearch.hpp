#pragma once
#include <elasticlient/client.h>
#include <cpr/cpr.h>
#include <json/json.h>
#include <iostream>
#include <memory>
#include <cstdlib>
#include <unordered_map>
#include <mutex>
#include "logger.hpp"


namespace bite_im{
// elasticlient::Client owns a mutable cpr::Session. Serialize all users of
// the same client, rather than letting concurrent RPCs overwrite its URL/body.
inline std::shared_ptr<std::mutex> esClientMutex(const std::shared_ptr<elasticlient::Client>& client) {
    static std::mutex registry_mutex;
    static std::unordered_map<elasticlient::Client*, std::weak_ptr<std::mutex>> registry;
    std::lock_guard<std::mutex> lock(registry_mutex);
    auto& entry = registry[client.get()];
    auto mutex = entry.lock();
    if (!mutex) { mutex = std::make_shared<std::mutex>(); entry = mutex; }
    return mutex;
}
inline bool Serialize(const Json::Value &val, std::string &dst)
{
    //先定义Json::StreamWriter 工厂类 Json::StreamWriterBuilder 
    Json::StreamWriterBuilder swb;
    swb.settings_["emitUTF8"] = true;
    std::unique_ptr<Json::StreamWriter> sw(swb.newStreamWriter());
    //通过Json::StreamWriter中的write接口进行序列化
    std::stringstream ss;
    int ret = sw->write(val, &ss);
    if (ret != 0) {
        std::cout << "Json反序列化失败！\n";
        return false;
    }
    dst = ss.str();
    return true;
}
inline bool UnSerialize(const std::string &src, Json::Value &val)
{
    Json::CharReaderBuilder crb;
    std::unique_ptr<Json::CharReader> cr(crb.newCharReader());
    std::string err;
    bool ret = cr->parse(src.c_str(), src.c_str() + src.size(), &val, &err);
    if (ret == false) {
        std::cout << "json反序列化失败: " << err << std::endl;
        return false;
    }
    return true;
}

class ESIndex {
    public:
        static std::string environment(const char *key, const char *fallback) {
            const char *value = std::getenv(key);
            return value && *value ? value : fallback;
        }
        ESIndex(std::shared_ptr<elasticlient::Client> &client, 
            const std::string &name, 
            const std::string &type = "_doc"):
            _name(name), _type(type), _client(client) {}
        ESIndex& append(const std::string &key, 
            const std::string &type = "text", 
            const std::string &analyzer = "",
            bool enabled = true) {
            Json::Value fields;
            fields["type"] = type;
            if (type == "text") fields["analyzer"] = analyzer.empty()
                ? environment("SOARIM_ES_TEXT_ANALYZER", "standard") : analyzer;
            if (!enabled) fields["index"] = false;
            _properties[key] = fields;
            return *this;
        }
        std::string exactField(const std::string &field) const {
            auto it = _exact_fields.find(field);
            return it == _exact_fields.end() ? field : it->second;
        }
        bool create() {
            const auto mutex = esClientMutex(_client);
            std::lock_guard<std::mutex> lock(*mutex);
            Json::Value mappings;
            mappings["dynamic"] = true;
            mappings["properties"] = _properties;
            _index["mappings"] = mappings;

            std::string body;
            bool ret = Serialize(_index, body);
            if (ret == false) {
                LOG_ERROR("索引序列化失败！");
                return false;
            }
            LOG_DEBUG("{}", body);
            //2. 发起搜索请求
            try {
                auto head = _client->performRequest(elasticlient::Client::HTTPMethod::HEAD, _name, "");
                if (head.status_code == 200) return validateExisting();
                if (head.status_code != 404) {
                    LOG_ERROR("检查 ES 索引 {} 失败: HTTP {}", _name, head.status_code);
                    return false;
                }
                // PUT /index creates an index. Client::index() writes a document.
                auto rsp = _client->performRequest(elasticlient::Client::HTTPMethod::PUT, _name, body);
                Json::Value error;
                if (rsp.status_code == 400 && UnSerialize(rsp.text, error)
                    && error["error"]["type"].asString() == "resource_already_exists_exception") {
                    return validateExisting(); // concurrent service startup
                }
                if (rsp.status_code < 200 || rsp.status_code >= 300) {
                    LOG_ERROR("创建 ES 索引 {} 失败: HTTP {}。检查 SOARIM_ES_TEXT_ANALYZER 及分词插件", _name, rsp.status_code);
                    return false;
                }
            } catch(std::exception &e) {
                LOG_ERROR("创建ES索引 {} 失败: {}", _name, e.what());
                return false;
            }
            return true;
        }
    private:
        bool validateExisting() {
            auto rsp = _client->performRequest(elasticlient::Client::HTTPMethod::GET, _name + "/_mapping", "");
            Json::Value mapping;
            if (rsp.status_code != 200 || !UnSerialize(rsp.text, mapping) || !mapping.isObject()
                || mapping.size() != 1) return false;
            const auto &properties = (*mapping.begin())["mappings"]["properties"];
            for (const auto &field : _properties.getMemberNames()) {
                const auto &expected = _properties[field];
                const auto &actual = properties[field];
                // Legacy dynamic mappings use text + keyword subfields. Never
                // overwrite a live mapping or require deletion of existing data.
                if (expected["type"] == "keyword" && actual["type"] == "text"
                    && actual["fields"]["keyword"]["type"] == "keyword"
                    && actual["fields"]["keyword"].get("index", true).asBool()) {
                    _exact_fields[field] = field + ".keyword";
                    continue;
                }
                const bool indexed = !expected.isMember("index") || expected["index"].asBool();
                const std::string actualAnalyzer = actual.get("analyzer", "standard").asString();
                if (actual["type"] != expected["type"]
                    || (indexed && actual.isMember("index") && !actual["index"].asBool())
                    || (expected["type"] == "text" && indexed
                        && actualAnalyzer != expected["analyzer"].asString())) {
                    LOG_ERROR("ES 索引 {} 字段 {} 的 mapping 不兼容；请备份并 reindex 到新索引，禁止直接覆盖", _name, field);
                    return false;
                }
            }
            return true;
        }
        std::unordered_map<std::string, std::string> _exact_fields;
        std::string _name;
        std::string _type;
        Json::Value _properties;
        Json::Value _index;
        std::shared_ptr<elasticlient::Client> _client;
};

class ESInsert {
    public:
        ESInsert(std::shared_ptr<elasticlient::Client> &client, 
            const std::string &name, 
            const std::string &type = "_doc"):
            _name(name), _type(type), _client(client){}
        template<typename T>
        ESInsert &append(const std::string &key, const T &val){
            _item[key] = val;
            return *this;
        }
        bool insert(const std::string id = "") {
            const auto mutex = esClientMutex(_client);
            std::lock_guard<std::mutex> lock(*mutex);
            std::string body;
            bool ret = Serialize(_item, body);
            if (ret == false) {
                LOG_ERROR("索引序列化失败！");
                return false;
            }
            LOG_DEBUG("{}", body);
            //2. 发起搜索请求
            try {
                auto rsp = _client->index(_name, _type, id, body);
                if (rsp.status_code < 200 || rsp.status_code >= 300) {
                    LOG_ERROR("新增数据 {} 失败，响应状态码异常: {}", body, rsp.status_code);
                    return false;
                }
            } catch(std::exception &e) {
                LOG_ERROR("新增数据 {} 失败: {}", body, e.what());
                return false;
            }
            return true;
        }
    private:
        std::string _name;
        std::string _type;
        Json::Value _item;
        std::shared_ptr<elasticlient::Client> _client;
};

class ESRemove {
    public:
        ESRemove(std::shared_ptr<elasticlient::Client> &client, 
            const std::string &name, 
            const std::string &type = "_doc"):
            _name(name), _type(type), _client(client){}
        bool remove(const std::string &id) {
            const auto mutex = esClientMutex(_client);
            std::lock_guard<std::mutex> lock(*mutex);
            try {
                auto rsp = _client->remove(_name, _type, id);
                if (rsp.status_code < 200 || rsp.status_code >= 300) {
                    LOG_ERROR("删除数据 {} 失败，响应状态码异常: {}", id, rsp.status_code);
                    return false;
                }
            } catch(std::exception &e) {
                LOG_ERROR("删除数据 {} 失败: {}", id, e.what());
                return false;
            }
            return true;
        }
    private:
        std::string _name;
        std::string _type;
        std::shared_ptr<elasticlient::Client> _client;
};

class ESSearch {
    public:
        ESSearch(std::shared_ptr<elasticlient::Client> &client, 
            const std::string &name, 
            const std::string &type = "_doc"):
            _name(name), _type(type), _client(client){}
        ESSearch& append_must_not_terms(const std::string &key, const std::vector<std::string> &vals) {
            Json::Value fields;
            for (const auto& val : vals){
                fields[key].append(val);
            }
            Json::Value terms;
            terms["terms"] = fields;
            _must_not.append(terms);
            return *this;
        }
        ESSearch& append_should_match(const std::string &key, const std::string &val) {
            Json::Value field;
            field[key] = val;
            Json::Value match;
            match["match"] = field;
            _should.append(match);
            return *this;
        }
        ESSearch& append_must_term(const std::string &key, const std::string &val) {
            Json::Value field;
            field[key] = val;
            Json::Value term;
            term["term"] = field;
            _must.append(term);
            return *this;
        }
        ESSearch& append_must_match(const std::string &key, const std::string &val){
            Json::Value field;
            field[key] = val;
            Json::Value match;
            match["match"] = field;
            _must.append(match);
            return *this;
        }
        Json::Value search(){
            const auto mutex = esClientMutex(_client);
            std::lock_guard<std::mutex> lock(*mutex);
            Json::Value cond;
            if (_must_not.empty() == false) cond["must_not"] = _must_not;
            if (_should.empty() == false) cond["should"] = _should;
            if (_must.empty() == false) cond["must"] = _must;
            Json::Value query;
            query["bool"] = cond;
            Json::Value root;
            root["query"] = query;

            std::string body;
            bool ret = Serialize(root, body);
            if (ret == false) {
                LOG_ERROR("索引序列化失败！");
                return Json::Value();
            }
            LOG_DEBUG("{}", body);
            //2. 发起搜索请求
            cpr::Response rsp;
            try {
                rsp = _client->search(_name, _type, body);
                if (rsp.status_code < 200 || rsp.status_code >= 300) {
                    LOG_ERROR("检索数据 {} 失败，响应状态码异常: {}", body, rsp.status_code);
                    return Json::Value();
                }
            } catch(std::exception &e) {
                LOG_ERROR("检索数据 {} 失败: {}", body, e.what());
                return Json::Value();
            }
            //3. 需要对响应正文进行反序列化
            LOG_DEBUG("检索响应正文: [{}]", rsp.text);
            Json::Value json_res;
            ret = UnSerialize(rsp.text, json_res);
            if (ret == false) {
                LOG_ERROR("检索数据 {} 结果反序列化失败", rsp.text);
                return Json::Value();
            }
            return json_res["hits"]["hits"];
        }
    private:
        std::string _name;
        std::string _type;
        Json::Value _must_not;
        Json::Value _should;
        Json::Value _must;
        std::shared_ptr<elasticlient::Client> _client;
};
}
