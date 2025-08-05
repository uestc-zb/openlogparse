#include <algorithm>  // 包含标准算法库
#include <csignal>    // 包含信号处理库
#include <pthread.h>  // 包含POSIX线程库
#include <regex>      // 包含正则表达式库
#include <sys/utsname.h>  // 包含系统信息结构体
#include <thread>     // 包含线程库
#include <unistd.h>   // 包含标准符号常量和类型

#include "OpenLogReplicator.h"  // 包含主程序头文件
#include "common/ClockHW.h"     // 包含硬件时钟相关定义
#include "common/Ctx.h"         // 包含上下文类定义
#include "common/Thread.h"      // 包含线程相关定义
#include "common/exception/ConfigurationException.h"  // 包含配置异常类
#include "common/exception/DataException.h"            // 包含数据异常类
#include "common/exception/RuntimeException.h"         // 包含运行时异常类
#include "common/types/Data.h"  // 包含数据类型定义
#include "metadata/Metadata.h"  // 包含元数据相关定义
#include "ReplicatorManager.h"  // 包含复制管理器定义
#include "ReplicatorHttpServer.h"  // 包含HTTP服务器定义

// 条件编译：检查是否链接OCI库
#ifdef LINK_LIBRARY_OCI
#define HAS_OCI " OCI"  // 定义OCI支持标识
#else
#define HAS_OCI ""     // 不支持OCI
#endif /* LINK_LIBRARY_OCI */

// 条件编译：检查是否链接Protobuf库
#ifdef LINK_LIBRARY_PROTOBUF
#define HAS_PROTOBUF " Protobuf"  // 定义Protobuf支持标识
// 检查是否链接ZeroMQ库
#ifdef LINK_LIBRARY_ZEROMQ
#define HAS_ZEROMQ " ZeroMQ"     // 定义ZeroMQ支持标识
#else
#define HAS_ZEROMQ ""           // 不支持ZeroMQ
#endif /* LINK_LIBRARY_ZEROMQ */
#else
#define HAS_PROTOBUF ""         // 不支持Protobuf
#define HAS_ZEROMQ ""           // 不支持ZeroMQ
#endif /* LINK_LIBRARY_PROTOBUF */

// 条件编译：检查是否链接Kafka库
#ifdef LINK_LIBRARY_RDKAFKA
#define HAS_KAFKA " Kafka"       // 定义Kafka支持标识
#else
#define HAS_KAFKA ""            // 不支持Kafka
#endif /* LINK_LIBRARY_RDKAFKA */

// 条件编译：检查是否链接Prometheus库
#ifdef LINK_LIBRARY_PROMETHEUS
#define HAS_PROMETHEUS " Prometheus"  // 定义Prometheus支持标识
#else
#define HAS_PROMETHEUS ""            // 不支持Prometheus
#endif /* LINK_LIBRARY_PROMETHEUS */

// 条件编译：检查是否静态链接
#ifdef LINK_STATIC
#define HAS_STATIC " static"        // 定义静态链接标识
#else
#define HAS_STATIC ""              // 动态链接
#endif /* LINK_STATIC */

// 条件编译：检查是否启用线程信息
#ifdef THREAD_INFO
#define HAS_THREAD_INFO " thread-info"  // 定义线程信息标识
#else
#define HAS_THREAD_INFO ""              // 不启用线程信息
#endif /* THREAD_INFO */

// 匿名命名空间，用于封装内部实现细节
namespace
{
    // 定义上下文映射表，用于管理多个上下文实例
    std::map<std::string, std::shared_ptr<OpenLogReplicator::Ctx>> mainCtxMap;
    // 添加一个全局变量来控制程序退出
    volatile bool shouldTerminate = false;  // 退出标志，volatile确保多线程可见性

    // 打印所有上下文的堆栈跟踪信息
    void printStacktrace() {
        // 遍历所有上下文实例并打印堆栈跟踪
        for (const auto &[id, ctx]: mainCtxMap) {
            ctx->printStacktrace();  // 调用上下文的堆栈打印方法
        }
    }

    // 信号处理函数，处理各种系统信号
    void signalHandler(int s) {
        // 在接收到SIGINT信号时设置终止标志
        if (s == SIGINT) {  // SIGINT通常是Ctrl+C产生的信号
            shouldTerminate = true;  // 设置退出标志
            std::cout << "\n收到终止信号，程序即将退出...\n";  // 输出提示信息
            // 添加对HTTP服务器的关闭调用
            ReplicatorHttpServer::shutdown();  // 关闭HTTP服务器
        }
        
        // 遍历所有上下文并调用其信号处理方法
        for (auto &[id, ctx]: mainCtxMap) {
            ctx->signalHandler(s);  // 调用上下文的信号处理方法
        }
    }

    // 崩溃信号处理函数
    void signalCrash(int sig __attribute__((unused))) {
        printStacktrace();  // 打印堆栈跟踪信息
        exit(1);            // 退出程序
    }

    // 转储信号处理函数
    void signalDump(int sig __attribute__((unused))) {
        printStacktrace();  // 打印堆栈跟踪信息
        // 遍历所有上下文并调用其转储方法
        for (auto &[id, ctx]: mainCtxMap) {
            ctx->signalDump();  // 调用上下文的转储方法
        }
    }
}

// 主函数，程序入口点
int main(int argc, char **argv) {
    // 注册信号处理函数
    signal(SIGINT, signalHandler);   // 注册中断信号处理
    signal(SIGPIPE, signalHandler);  // 注册管道信号处理
    signal(SIGSEGV, signalCrash);    // 注册段错误信号处理
    signal(SIGUSR1, signalDump);     // 注册用户自定义信号处理

    // 在独立线程中启动HTTP服务器
    std::thread httpThread([]() {
        ReplicatorHttpServer::registerServer();  // 注册并启动HTTP服务器
    });

    // 创建一个新的上下文实例
    std::shared_ptr<OpenLogReplicator::Ctx> ctx = std::make_shared<OpenLogReplicator::Ctx>();

    // 获取本地化设置
    std::string olrLocales;
    const char *olrLocalesStr = getenv("OLR_LOCALES");  // 获取环境变量
    if (olrLocalesStr != nullptr)
        olrLocales = olrLocalesStr;  // 设置本地化字符串
    if (olrLocales == "MOCK")
        OLR_LOCALES = OpenLogReplicator::Ctx::LOCALES::MOCK;  // 设置模拟本地化模式
    
    // 设置上下文ID并将其添加到映射表中
    std::string id = "1";
    mainCtxMap[id] = ctx;
    
    // 构造配置文件路径
    std::string fileName = "scripts/OpenLogReplicator" + id + ".json";
    const char *ps[2] = {"-f", fileName.c_str()};  // 构造命令行参数
    
    // 主循环，等待终止信号
    while (!shouldTerminate) {
        // 添加短暂的睡眠，避免CPU占用过高
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    
    // 等待HTTP服务器线程结束
    if (httpThread.joinable()) {
        httpThread.join();  // 等待线程完成
    }
    
    // 清理信号处理函数
    signal(SIGINT, nullptr);    // 取消注册中断信号处理
    signal(SIGPIPE, nullptr);   // 取消注册管道信号处理
    signal(SIGSEGV, nullptr);   // 取消注册段错误信号处理
    signal(SIGUSR1, nullptr);   // 取消注册用户自定义信号处理
    
    // 清理上下文映射表
    mainCtxMap.clear();
    
    // 返回程序执行结果
    return 0;
}
