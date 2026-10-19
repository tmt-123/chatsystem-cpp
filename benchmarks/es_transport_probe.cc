#include <elasticlient/client.h>
#include <cpr/cpr.h>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <vector>

// A small isolated transport profile, not an IM throughput benchmark.
int main(int argc, char **argv) {
    if (argc != 2) return 2;
    elasticlient::Client client(std::vector<std::string>{argv[1]});
    using Method = elasticlient::Client::HTTPMethod;
    const auto index = "test-transport-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    if (client.performRequest(Method::PUT, index, "{}").status_code != 200) return 3;
    std::vector<double> samples;
    for (int i = 0; i < 30; ++i) {
        const auto response = client.index(index, "_doc", std::to_string(i), R"({"content":"transport profile"})");
        if (response.status_code != 200 && response.status_code != 201) return 4;
        samples.push_back(response.elapsed * 1000);
    }
    std::sort(samples.begin(), samples.end());
    double total = 0;
    for (auto value : samples) total += value;
    std::cout << "{\"operations\":30,\"mean_ms\":" << total / samples.size()
              << ",\"p50_ms\":" << samples[14] << ",\"p95_ms\":" << samples[27] << "}\n";
    client.performRequest(Method::DELETE, index, "");
}
