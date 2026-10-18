#pragma once
#include <sw/redis++/redis.h>
#include <iostream>
#include <chrono>

namespace bite_im {
    class RedisClientFactory {
        public:
            static std::shared_ptr<sw::redis::Redis> create(
                const std::string &host,
                int port,
                int db,
                bool keep_alive) {
                sw::redis::ConnectionOptions opts;
                opts.host = host;
                opts.port = port;
                opts.db = db;
                opts.keep_alive = keep_alive;
                auto res = std::make_shared<sw::redis::Redis>(opts);
                return res;
            }
    };
    class Session {
        public:
            using ptr = std::shared_ptr<Session>;
            Session(const std::shared_ptr<sw::redis::Redis> &redis_client,
                    long long pending_ms = 30000, long long online_ms = 90000,
                    long long session_ms = 86400000):
                _redis_client(redis_client), _pending_ms(pending_ms),
                _online_ms(online_ms), _session_ms(session_ms){}
            // One atomic operation: acquire the short login lease, replace any
            // disconnected session, and create an expiring bearer token.
            bool create(const std::string &ssid, const std::string &uid) {
                return _redis_client->eval<long long>(R"(
                    local lock = redis.call('GET', KEYS[2])
                    if lock and lock ~= '' then return 0 end
                    local old = redis.call('GET', KEYS[3])
                    if old and old ~= ARGV[1] then redis.call('DEL', old) end
                    redis.call('SET', KEYS[1], ARGV[2], 'PX', ARGV[4])
                    redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[3])
                    redis.call('SET', KEYS[3], ARGV[1], 'PX', ARGV[4])
                    return 1
                )", {ssid, uid, ownerKey(uid)},
                    {ssid, uid, std::to_string(_pending_ms), std::to_string(_session_ms)}) == 1;
            }
            void append(const std::string &ssid, const std::string &uid) {
                if (!create(ssid, uid)) throw std::runtime_error("user already logged in");
            }
            void remove(const std::string &ssid) {
                auto user = uid(ssid);
                if (!user) return;
                _redis_client->eval<long long>(R"(
                    if redis.call('GET', KEYS[3]) == ARGV[1] then
                        redis.call('DEL', KEYS[3])
                    end
                    if redis.call('GET', KEYS[2]) == ARGV[1] then redis.call('DEL', KEYS[2]) end
                    return redis.call('DEL', KEYS[1])
                )", {ssid, *user, ownerKey(*user)}, {ssid});
            }
            sw::redis::OptionalString uid(const std::string &ssid) {
                if (ssid.empty()) return sw::redis::OptionalString();
                return _redis_client->eval<sw::redis::OptionalString>(R"(
                    local uid = redis.call('GET', KEYS[1])
                    if uid and redis.call('GET', 'session:user:' .. uid) == ARGV[1] then return uid end
                    return false
                )", {ssid}, {ssid});
            }
            bool renew(const std::string &ssid, const std::string &uid) {
                return _redis_client->eval<long long>(R"(
                    if redis.call('GET', KEYS[1]) ~= ARGV[2] or
                       redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
                    local lock = redis.call('GET', KEYS[2])
                    if lock and lock ~= ARGV[1] then return 0 end
                    redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[3])
                    redis.call('PEXPIRE', KEYS[1], ARGV[4])
                    redis.call('PEXPIRE', KEYS[3], ARGV[4])
                    return 1
                )", {ssid, uid, ownerKey(uid)},
                    {ssid, uid, std::to_string(_online_ms), std::to_string(_session_ms)}) == 1;
            }
            // A disconnected socket releases only its own lease. Keep its token
            // for reconnect; a later password login invalidates that old token.
            void release(const std::string &ssid, const std::string &uid) {
                _redis_client->eval<long long>(R"(
                    if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
                    return 0
                )", {uid}, {ssid});
            }
        private:
            static std::string ownerKey(const std::string &uid) { return "session:user:" + uid; }
            std::shared_ptr<sw::redis::Redis> _redis_client;
            long long _pending_ms, _online_ms, _session_ms;
    };
    class Codes {
        public:
            using ptr = std::shared_ptr<Codes>;
            Codes(const std::shared_ptr<sw::redis::Redis> &redis_client):
                _redis_client(redis_client){}
            void append(const std::string &cid, const std::string &code, 
                const std::chrono::milliseconds &t = std::chrono::milliseconds(300000)) {
                _redis_client->set(cid, code, t);
            }
            void remove(const std::string &cid) {
                _redis_client->del(cid);
            }
            sw::redis::OptionalString code(const std::string &cid)  {
                return _redis_client->get(cid);
            }
        private:
            std::shared_ptr<sw::redis::Redis> _redis_client;
    };
}
