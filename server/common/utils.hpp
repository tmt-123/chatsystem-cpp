//实现项目中一些公共的工具类接口
//1. 生成一个唯一ID的接口
//2. 文件的读写操作接口

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <atomic>
#include <random>
#include <iomanip>
#include <vector>
#include <stdexcept>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include "logger.hpp"

namespace bite_im {

inline std::string hexEncode(const unsigned char* data, size_t size) {
    static const char* digits = "0123456789abcdef";
    std::string output;
    output.reserve(size * 2);
    for (size_t i = 0; i < size; ++i) {
        output.push_back(digits[(data[i] >> 4) & 0x0f]);
        output.push_back(digits[data[i] & 0x0f]);
    }
    return output;
}

inline bool hexDecode(const std::string& input, std::vector<unsigned char>& output) {
    if (input.size() % 2 != 0) return false;
    output.clear();
    output.reserve(input.size() / 2);
    auto value = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    for (size_t i = 0; i < input.size(); i += 2) {
        int high = value(input[i]);
        int low = value(input[i + 1]);
        if (high < 0 || low < 0) return false;
        output.push_back(static_cast<unsigned char>((high << 4) | low));
    }
    return true;
}

inline bool passwordIsHashed(const std::string& value) {
    return value.rfind("pbkdf2_sha256$", 0) == 0;
}

inline std::string hashPassword(const std::string& password) {
    constexpr int iterations = 210000;
    constexpr size_t saltSize = 16;
    constexpr size_t digestSize = 32;

    std::vector<unsigned char> salt(saltSize);
    std::vector<unsigned char> digest(digestSize);
    if (RAND_bytes(salt.data(), static_cast<int>(salt.size())) != 1) {
        throw std::runtime_error("无法生成密码盐值");
    }
    if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
            salt.data(), static_cast<int>(salt.size()), iterations, EVP_sha256(),
            static_cast<int>(digest.size()), digest.data()) != 1) {
        throw std::runtime_error("无法生成密码哈希");
    }

    return "pbkdf2_sha256$" + std::to_string(iterations) + "$" +
        hexEncode(salt.data(), salt.size()) + "$" +
        hexEncode(digest.data(), digest.size());
}

inline bool verifyPassword(const std::string& password, const std::string& encoded) {
    if (!passwordIsHashed(encoded)) return false;

    const size_t first = encoded.find('$');
    const size_t second = encoded.find('$', first + 1);
    const size_t third = encoded.find('$', second + 1);
    if (first == std::string::npos || second == std::string::npos || third == std::string::npos) {
        return false;
    }

    int iterations = 0;
    try {
        iterations = std::stoi(encoded.substr(first + 1, second - first - 1));
    } catch (...) {
        return false;
    }
    if (iterations < 100000 || iterations > 2000000) return false;

    std::vector<unsigned char> salt;
    std::vector<unsigned char> expected;
    if (!hexDecode(encoded.substr(second + 1, third - second - 1), salt) ||
        !hexDecode(encoded.substr(third + 1), expected) || expected.empty()) {
        return false;
    }

    std::vector<unsigned char> actual(expected.size());
    if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
            salt.data(), static_cast<int>(salt.size()), iterations, EVP_sha256(),
            static_cast<int>(actual.size()), actual.data()) != 1) {
        return false;
    }
    return CRYPTO_memcmp(actual.data(), expected.data(), expected.size()) == 0;
}

inline std::string stableMessageId(const std::string& senderId, const std::string& clientMessageId) {
    const std::string source = senderId + ":" + clientMessageId;
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(source.data()), source.size(), digest);
    return "M" + hexEncode(digest, 16);
}

std::string uuid() {
    //生成一个由16位随机字符组成的字符串作为唯一ID
    // 1. 生成6个0~255之间的随机数字(1字节-转换为16进制字符)--生成12位16进制字符
    std::random_device rd;//实例化设备随机数对象-用于生成设备随机数
    std::mt19937 generator(rd());//以设备随机数为种子，实例化伪随机数对象
    std::uniform_int_distribution<int> distribution(0,255); //限定数据范围

    std::stringstream ss;
    for (int i = 0; i < 6; i++) {
        if (i == 2) ss << "-";
        ss << std::setw(2) << std::setfill('0') << std::hex << distribution(generator);
    }
    ss << "-";
    // 2. 通过一个静态变量生成一个2字节的编号数字--生成4位16进制数字字符
    static std::atomic<short> idx(0);
    short tmp = idx.fetch_add(1);
    ss << std::setw(4) << std::setfill('0') << std::hex << tmp;
    return ss.str();
}

std::string vcode() {
    std::random_device rd;//实例化设备随机数对象-用于生成设备随机数
    std::mt19937 generator(rd());//以设备随机数为种子，实例化伪随机数对象
    std::uniform_int_distribution<int> distribution(0,9); //限定数据范围

    std::stringstream ss;
    for (int i = 0; i < 4; i++) {
        ss << distribution(generator);
    }
    return ss.str();
}

bool readFile(const std::string &filename, std::string &body){
    //实现读取一个文件的所有数据，放入body中
    std::ifstream ifs(filename, std::ios::binary | std::ios::in);
    if (ifs.is_open() == false) {
        LOG_ERROR("打开文件 {} 失败！", filename);
        return false;
    }
    ifs.seekg(0, std::ios::end);//跳转到文件末尾
    size_t flen = ifs.tellg();  //获取当前偏移量-- 文件大小
    ifs.seekg(0, std::ios::beg);//跳转到文件起始
    body.resize(flen);
    ifs.read(&body[0], flen);
    if (ifs.good() == false) {
        LOG_ERROR("读取文件 {} 数据失败！", filename);
        ifs.close();
        return false;
    }
    ifs.close();
    return true;
}
bool writeFile(const std::string &filename, const std::string &body){
    //实现将body中的数据，写入filename对应的文件中
    std::ofstream ofs(filename, std::ios::out | std::ios::binary | std::ios::trunc);
    if (ofs.is_open() == false) {
        LOG_ERROR("打开文件 {} 失败！", filename);
        return false;
    }
    ofs.write(body.c_str(), body.size());
    if (ofs.good() == false) {
        LOG_ERROR("读取文件 {} 数据失败！", filename);
        ofs.close();
        return false;
    }
    ofs.close();
    return true;
}

}
