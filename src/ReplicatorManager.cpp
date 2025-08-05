//
// Created by tangh on 2025/5/23.
//

#include <iostream>      // 包含标准输入输出流库，用于控制台输入输出操作
#include <thread>       // 包含线程库，用于创建和管理多线程
#include <atomic>       // 包含原子操作库，用于线程安全的原子变量操作
#include <unordered_map> // 包含无序映射容器库，用于存储键值对数据结构
#include <string>       // 包含字符串处理库，用于字符串操作
#include <sstream>      // 包含字符串流库，用于字符串流操作
#include <mutex>        // 包含互斥锁库，用于线程同步和数据保护
#include <algorithm>
#include <csignal>      // 包含信号处理库，用于处理系统信号(如Ctrl+C)
#include <pthread.h>    // 包含POSIX线程库，用于线程管理
#include <regex>        // 包含正则表达式库，用于模式匹配
#include <sys/utsname.h> // 包含系统信息库，用于获取操作系统信息
#include <unistd.h>     // 包含UNIX标准函数库，提供系统调用接口

#include "OpenLogReplicator.h"          // 包含OpenLogReplicator主类头文件
#include "common/ClockHW.h"             // 包含硬件时钟相关功能头文件
#include "common/Ctx.h"                 // 包含上下文管理类头文件
#include "common/Thread.h"              // 包含线程管理类头文件
#include "common/exception/ConfigurationException.h" // 包含配置异常类头文件
#include "common/exception/RuntimeException.h"        // 包含运行时异常类头文件
#include "metadata/Metadata.h"           // 包含元数据管理类头文件

#include "ReplicatorManager.h"          // 包含复制管理器类头文件
#include "common/Ctx.h"                 // 重复包含上下文管理类头文件
#include "rapidjson/stringbuffer.h"     // 包含RapidJSON字符串缓冲区头文件
#include "rapidjson/writer.h"           // 包含RapidJSON写入器头文件

// 条件编译宏定义，用于根据链接的库定义版本信息字符串
#ifdef LINK_LIBRARY_OCI
#define HAS_OCI " OCI"      // 如果链接了OCI库，则在版本信息中添加" OCI"
#else
#define HAS_OCI ""          // 如果没有链接OCI库，则为空字符串
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#define HAS_PROTOBUF " Protobuf"  // 如果链接了Protobuf库，则在版本信息中添加" Protobuf"
#ifdef LINK_LIBRARY_ZEROMQ
#define HAS_ZEROMQ " ZeroMQ"      // 如果链接了ZeroMQ库，则在版本信息中添加" ZeroMQ"
#else
#define HAS_ZEROMQ ""             // 如果没有链接ZeroMQ库，则为空字符串
#endif /* LINK_LIBRARY_ZEROMQ */
#else
#define HAS_PROTOBUF ""           // 如果没有链接Protobuf库，则为空字符串
#define HAS_ZEROMQ ""             // 如果没有链接ZeroMQ库，则为空字符串
#endif /* LINK_LIBRARY_PROTOBUF */

#ifdef LINK_LIBRARY_RDKAFKA
#define HAS_KAFKA " Kafka"        // 如果链接了Kafka库，则在版本信息中添加" Kafka"
#else
#define HAS_KAFKA ""              // 如果没有链接Kafka库，则为空字符串
#endif /* LINK_LIBRARY_RDKAFKA */

#ifdef LINK_LIBRARY_PROMETHEUS
#define HAS_PROMETHEUS " Prometheus"  // 如果链接了Prometheus库，则在版本信息中添加" Prometheus"
#else
#define HAS_PROMETHEUS ""             // 如果没有链接Prometheus库，则为空字符串
#endif /* LINK_LIBRARY_PROMETHEUS */

#ifdef LINK_STATIC
#define HAS_STATIC " static"      // 如果是静态链接，则在版本信息中添加" static"
#else
#define HAS_STATIC ""             // 如果不是静态链接，则为空字符串
#endif /* LINK_STATIC */

#ifdef THREAD_INFO
#define HAS_THREAD_INFO " thread-info"  // 如果启用了线程信息，则在版本信息中添加" thread-info"
#else
#define HAS_THREAD_INFO ""              // 如果没有启用线程信息，则为空字符串
#endif /* THREAD_INFO */

namespace ReplicatorManager {
    /**
     * @brief 线程任务函数，用于启动和管理单个复制任务
     * 
     * 该函数负责为每个复制任务创建独立的执行环境，包括：
     * 1. 检查并设置区域设置环境变量
     * 2. 构造特定于任务的配置文件路径
     * 3. 模拟命令行参数调用主函数
     * 
     * @param id 任务ID，用于标识不同的复制任务
     * @param info 线程信息结构体，包含上下文和运行状态
     */
    static void thread_task(std::string id, const ThreadInfo &info) {
        /*
        检查 OLR_LOCALES 环境变量
        如果设置为 "MOCK"，则配置应用使用模拟区域设置
        */
        std::string olrLocales;
        const char *olrLocalesStr = getenv("OLR_LOCALES");
        if (olrLocalesStr != nullptr)
            olrLocales = olrLocalesStr;
        if (olrLocales == "MOCK")
            OLR_LOCALES = OpenLogReplicator::Ctx::LOCALES::MOCK;

        /*
        基于任务ID创建特定的配置文件路径
        每个复制任务使用单独的配置文件 (scripts/OpenLogReplicator{ID}.json)
        */
        std::string fileName = "scripts/OpenLogReplicator" + id + ".json";
        /*
        构造命令行参数数组，模拟独立应用程序的启动
        包含程序名、-f 参数和配置文件路径
        */
        const char *ps[3] = {"main", "-f", fileName.c_str()};
        // std::thread threadF(mainFunction, argc, ps, &ctx);
        mainFunction(3, ps, info.ctx.get());
    }

    /**
     * @brief 退出函数，用于停止所有复制任务线程
     * 
     * 该函数负责安全地停止所有正在运行的复制任务线程，包括：
     * 1. 获取线程映射表的互斥锁
     * 2. 遍历所有线程，设置运行状态为false
     * 3. 等待线程执行完毕(join)
     * 4. 清空线程映射表
     */
    void ReplicatorManager::exit() {
        // 获取线程映射表的互斥锁，确保线程安全
        std::lock_guard<std::mutex> lock(map_mutex);
        // 遍历所有线程
        for (auto &[id, info]: threads) {
            // 设置线程运行状态为false，通知线程停止
            info.running = false;
            // 检查线程是否可join，避免调用join时出现异常
            if (info.thread->joinable()) {
                // 等待线程执行完毕
                info.thread->join();
            }
        }
        // 清空线程映射表
        threads.clear();
    }
    // 这个函数负责创建和启动一个新的Oracle日志复制任务线程
    void ReplicatorManager::start(std::string id, const std::string &config_basic_string) {
        // 创建新任务线程
        /*
        在 threads 哈希表中创建或获取与指定ID关联的 ThreadInfo 对象
        如果ID不存在，会自动创建一个新的 ThreadInfo 结构体实例
        使用引用 &info 以便直接修改哈希表中的对象
        */
        auto &info = threads[id];
        //这个原子变量用于控制线程的生命周期，可在其他地方检查以决定线程是否应继续运行
        info.running = true;
        /*
        使用智能指针创建一个新的任务上下文对象
        Ctx 类包含了复制任务所需的所有状态和配置信息
        std::make_unique 确保内存管理安全，避免内存泄漏
        尖括号 <> - 指定要创建的对象类型
        圆括号 () - 传递给构造函数的参数
        */
        info.ctx = std::make_unique<OpenLogReplicator::Ctx>();//创建一个 Ctx 类的实例，用于存储复制任务的配置和状态
        /*
        将传入的JSON配置字符串保存到上下文对象中
        这个配置定义了Oracle连接参数、过滤条件和输出目标等
        */
        info.ctx->config = config_basic_string;
        /*
        创建一个新的 std::thread 对象执行 thread_task 函数
        传递两个参数：任务ID和ThreadInfo结构体的引用
        std::ref(info) 确保传递引用而非副本，使线程可以修改共享的ThreadInfo
        线程创建后立即开始执行
        */
       // std::thread 提供多种构造函数,这里使用的是函数指针 + 参数列表
        info.thread = std::make_unique<std::thread>(thread_task, id, std::ref(info));
        std::cout << "Started thread " << id << std::endl;
    }

    void ReplicatorManager::stop(std::string id) {
        auto it = threads.find(id);
        it->second.running = false;
        it->second.ctx->stopHard();
        // it->second
        if (it->second.thread->joinable()) {
            it->second.thread->join();
        }
        threads.erase(it);
        std::cout << "Stopped thread " << id << std::endl;
    }


    void ReplicatorManager::updateConfig(std::string id, const std::string &newConfig) {
        std::lock_guard<std::mutex> lock(map_mutex);
        
        auto it = threads.find(id);
        if (it == threads.end()) {
            throw std::runtime_error("Thread " + id + " not found!");
        }
        
        // 检查配置格式是否有效
        rapidjson::Document testDoc;
        if (testDoc.Parse(newConfig.c_str()).HasParseError()) {
            throw std::runtime_error("Invalid JSON configuration");
        }
        
        // 更新线程的配置
        it->second.ctx->config = newConfig;
        // 设置更新标志，通知复制任务配置已变更
        it->second.ctx->configUpdated = true;
        
        std::cout << "Updated config for thread " << id << std::endl;
    }


    std::string ReplicatorManager::getStatus(const std::string& id) {
        std::lock_guard<std::mutex> lock(map_mutex);
        
        auto it = threads.find(id);
        if (it == threads.end()) {
            throw std::runtime_error("Thread " + id + " not found!");
        }
        
        // 创建JSON结构存储任务状态信息
        rapidjson::Document status;
        status.SetObject();
        rapidjson::Document::AllocatorType& allocator = status.GetAllocator();
        
        // 添加基本信息
        status.AddMember("id", rapidjson::Value(id.c_str(), allocator).Move(), allocator);
        status.AddMember("running", it->second.running.load(), allocator);  // 使用 .load()
        
        // 解析配置并添加到响应中
        rapidjson::Document config;
        config.Parse(it->second.ctx->config.c_str());
        if (!config.HasParseError()) {
            status.AddMember("config", config, allocator);
        }
        
        // 添加运行时状态信息
        rapidjson::Value runtimeInfo(rapidjson::kObjectType);
        runtimeInfo.AddMember("configUpdated", it->second.ctx->configUpdated.load(), allocator);  // 使用 .load()
        runtimeInfo.AddMember("hardShutdown", it->second.ctx->hardShutdown, allocator);    // 使用 .load()
        runtimeInfo.AddMember("softShutdown", it->second.ctx->softShutdown, allocator);    // 使用 .load()
        
        status.AddMember("runtimeInfo", runtimeInfo, allocator);
        
        // 序列化为JSON字符串
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        status.Accept(writer);
        
        return buffer.GetString();
    }

    int mainFunction(int argc, const char **argv, OpenLogReplicator::Ctx *mainCtx) {
        int ret = 1;
        struct utsname name{};
        if (uname(&name) != 0) exit(-1);
        std::string buildArch;
        if (strlen(OpenLogReplicator_CMAKE_BUILD_TIMESTAMP) > 0)
            buildArch = ", build-arch: " OpenLogReplicator_CPU_ARCH;

        mainCtx->welcome("OpenLogReplicator v" + std::to_string(OpenLogReplicator_VERSION_MAJOR) + "." +
                         std::to_string(OpenLogReplicator_VERSION_MINOR) + "." + std::to_string(
                             OpenLogReplicator_VERSION_PATCH) +
                         " (C) 2018-2025 by Adam Leszczynski (aleszczynski@bersler.com), see LICENSE file for licensing information");
        mainCtx->welcome("arch: " + std::string(name.machine) + buildArch + ", system: " + name.sysname +
                         ", release: " + name.release + ", build: " +
                         OpenLogReplicator_CMAKE_BUILD_TYPE + ", compiled: " + OpenLogReplicator_CMAKE_BUILD_TIMESTAMP +
                         ", modules:"
                         HAS_KAFKA HAS_OCI HAS_PROMETHEUS HAS_PROTOBUF HAS_ZEROMQ HAS_STATIC HAS_THREAD_INFO);

        const char *fileName = "scripts/OpenLogReplicator.json";
        try {
            bool forceRoot = true;
            const std::regex regexTest(".*");
            const std::string regexString("check if matches!");
            const bool regexWorks = regex_search(regexString, regexTest);
            if (!regexWorks)
                throw OpenLogReplicator::RuntimeException(
                    10019, "binaries are build with no regex implementation, check if you have gcc version >= 4.9");

            for (int i = 1; i < argc; ++i) {
                const std::string arg = argv[i];
                if (arg == "-v" || arg == "--version") {
                    // Print banner and exit
                    return 0;
                }

                if (arg == "-r" || arg == "--root") {
                    // Allow bad practice to run as root
                    forceRoot = true;
                    continue;
                }

                if (i + 1 < argc && (arg == "-f" || arg == "--file")) {
                    // Custom config path
                    fileName = argv[i + 1];
                    ++i;
                    continue;
                }

                if (geteuid() == 0) {
                    if (!forceRoot)
                        throw OpenLogReplicator::RuntimeException(
                            10020, "program is run as root, you should never do that");
                    mainCtx->warning(10020, "program is run as root, you should never do that");
                }

                throw OpenLogReplicator::ConfigurationException(
                    30002, "invalid arguments, run: " + std::string(argv[0]) + " [-v|--version] [-f|--file CONFIG] "
                           "[-p|--process PROCESSNAME] [-r|--root]");
            }
        } catch (OpenLogReplicator::ConfigurationException &ex) {
            mainCtx->error(ex.code, ex.msg);
            return 1;
        } catch (OpenLogReplicator::RuntimeException &ex) {
            mainCtx->error(ex.code, ex.msg);
            return 1;
        }
        //优先使用内存配置：在mainFunction中，检查上下文是否已有配置。OpenLogReplicator::WEB_CONFIG_FILE_NAME="WebRequest"。
        OpenLogReplicator::OpenLogReplicator openLogReplicator = mainCtx->config.empty()
                                                                     ? OpenLogReplicator::OpenLogReplicator(
                                                                         fileName, mainCtx)
                                                                     : OpenLogReplicator::OpenLogReplicator(
                                                                         mainCtx->config.c_str(),
                                                                         OpenLogReplicator::WEB_CONFIG_FILE_NAME,
                                                                         mainCtx);
        try {
            ret = openLogReplicator.run();
        } catch (OpenLogReplicator::ConfigurationException &ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (OpenLogReplicator::DataException &ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (OpenLogReplicator::RuntimeException &ex) {
            mainCtx->error(ex.code, ex.msg);
            mainCtx->stopHard();
        } catch (std::bad_alloc &ex) {
            mainCtx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            mainCtx->stopHard();
        }

        return ret;
    }

};
