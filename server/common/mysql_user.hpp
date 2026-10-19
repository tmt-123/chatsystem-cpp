#include "mysql.hpp"
#include "user.hxx"
#include "user-odb.hxx"
#include <functional>

namespace bite_im {
class UserTable {
    public:
        using ptr = std::shared_ptr<UserTable>;
        UserTable(const std::shared_ptr<odb::core::database> &db):_db(db){}
        bool insert(const std::shared_ptr<User> &user) {
            try {
                odb::transaction trans(_db->begin());
                _db->persist(*user);
                queueIndex(user->user_id());
                trans.commit();
            }catch (std::exception &e) {
                LOG_ERROR("新增用户失败 {}:{}！", user->nickname(),e.what());
                return false;
            }
            return true;
        }
        bool update(const std::shared_ptr<User> &user) {
            try {
                odb::transaction trans(_db->begin());
                _db->update(*user);
                queueIndex(user->user_id());
                trans.commit();
            }catch (std::exception &e) {
                LOG_ERROR("更新用户失败 {}:{}！", user->nickname(), e.what());
                return false;
            }
            return true;
        }
        std::shared_ptr<User> select_by_nickname(const std::string &nickname) {
            std::shared_ptr<User> res;
            try {
                odb::transaction trans(_db->begin());
                typedef odb::query<User> query;
                typedef odb::result<User> result;
                res.reset(_db->query_one<User>(query::nickname == nickname));
                trans.commit();
            }catch (std::exception &e) {
                LOG_ERROR("通过昵称查询用户失败 {}:{}！", nickname, e.what());
            }
            return res;
        }
        std::shared_ptr<User> select_by_phone(const std::string &phone) {
            std::shared_ptr<User> res;
            try {
                odb::transaction trans(_db->begin());
                typedef odb::query<User> query;
                typedef odb::result<User> result;
                res.reset(_db->query_one<User>(query::phone == phone));
                trans.commit();
            }catch (std::exception &e) {
                LOG_ERROR("通过手机号查询用户失败：{}！", e.what());
            }
            return res;
        }
        std::shared_ptr<User> select_by_id(const std::string &user_id) {
            std::shared_ptr<User> res;
            try {
                odb::transaction trans(_db->begin());
                typedef odb::query<User> query;
                typedef odb::result<User> result;
                res.reset(_db->query_one<User>(query::user_id == user_id));
                trans.commit();
            }catch (std::exception &e) {
                LOG_ERROR("通过用户ID查询用户失败 {}:{}！", user_id, e.what());
            }
            return res;
        }
        std::vector<User> select_multi_users(const std::vector<std::string> &id_list) {
            // select * from user where id in ('id1', 'id2', ...)
            if (id_list.empty()) {
                return std::vector<User>();
            }
            std::vector<User> res;
            try {
                odb::transaction trans(_db->begin());
                typedef odb::query<User> query;
                typedef odb::result<User> result;
                std::stringstream ss;
                ss << "user_id in (";
                for (const auto &id : id_list) {
                    ss << "'" << id << "',";
                }
                std::string condition = ss.str();
                condition.pop_back();
                condition += ")";
                result r(_db->query<User>(condition));
                for (result::iterator i(r.begin()); i != r.end(); ++i) {
                    res.push_back(*i);
                }
                trans.commit();
            }catch (std::exception &e) {
                LOG_ERROR("通过用户ID批量查询用户失败:{}！", e.what());
            }
            return res;
        }
        // Lock user first, just like update(), then process its outbox task.
        // This serializes indexers for a user and indexes the latest committed
        // profile. Append-only tasks avoid absent-row/gap-lock insert deadlocks.
        bool indexPending(const std::function<bool(User&)> &index) {
            try {
                std::vector<std::pair<unsigned long, std::string>> ids;
                {
                    odb::transaction trans(_db->begin());
                    auto rows = _db->query<UserIndexTask>("ORDER BY id LIMIT 16");
                    for (const auto &row : rows) ids.emplace_back(row.id(), row.user_id());
                    trans.commit();
                }
                for (const auto &entry : ids) {
                    odb::transaction trans(_db->begin());
                    using user_query = odb::query<User>;
                    std::unique_ptr<User> user(_db->query_one<User>(
                        (user_query::user_id == entry.second) + " FOR UPDATE"));
                    std::unique_ptr<UserIndexTask> task(_db->find<UserIndexTask>(entry.first));
                    if (!task) { trans.commit(); continue; }
                    if (user && !index(*user)) return false; // rollback leaves durable retry task
                    _db->erase<UserIndexTask>(entry.first);
                    trans.commit();
                }
                return true;
            } catch (const std::exception &e) {
                LOG_WARN("用户索引同步失败，将从 outbox 重试: {}", e.what());
                return false;
            }
        }
    private:
        void queueIndex(const std::string &uid) {
            UserIndexTask task(uid);
            _db->persist(task);
        }
        std::shared_ptr<odb::core::database> _db;
};
}
