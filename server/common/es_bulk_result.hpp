#pragma once
#include <json/json.h>
#include <string>
#include <vector>

namespace bite_im {
// An HTTP 200 can contain partial failures. Missing, malformed or mismatched
// items must not become successful storage confirmations.
inline std::vector<bool> esBulkResult(const Json::Value &response,
                                      const std::vector<std::string> &ids) {
    std::vector<bool> success(ids.size(), false);
    if (!response.isObject() || !response["errors"].isBool() ||
        !response["items"].isArray() || response["items"].size() != ids.size()) return success;
    for (Json::ArrayIndex i = 0; i < ids.size(); ++i) {
        const auto &row = response["items"][i];
        if (!row.isObject()) continue;
        const auto &item = row["index"];
        if (item.isObject() && item["status"].isInt() && item["_id"].isString() &&
            item["_id"].asString() == ids[i] && !item.isMember("error")) {
            const auto status = item["status"].asInt();
            success[i] = status >= 200 && status < 300;
        }
    }
    return success;
}
} // namespace bite_im
