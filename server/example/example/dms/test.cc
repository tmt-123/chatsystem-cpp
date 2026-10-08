#include "../../common/dms.hpp"
#include "gflags/gflags.h"

DEFINE_string(key_id, "", "阿里云访问密钥 ID（请通过命令行或 flagfile 提供）");
DEFINE_string(key_secret, "", "阿里云访问密钥 Secret（请通过命令行或 flagfile 提供）");

DEFINE_bool(run_mode, false, "程序的运行模式，false-调试； true-发布；");
DEFINE_string(log_file, "", "发布模式下，用于指定日志的输出文件");
DEFINE_int32(log_level, 0, "发布模式下，用于指定日志输出等级");

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_logger(FLAGS_run_mode, FLAGS_log_file, FLAGS_log_level);

    if (FLAGS_key_id.empty() || FLAGS_key_secret.empty()) {
        LOG_ERROR("必须通过命令行或 flagfile 提供阿里云访问密钥");
        return 1;
    }

    DMSClient client(FLAGS_key_id, FLAGS_key_secret);
    client.send("15929917272", "5678");

    return 0;
}
