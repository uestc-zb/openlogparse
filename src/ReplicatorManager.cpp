//
// Created by tangh on 2025/5/23.
//

#include <iostream>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <string>
#include <sstream>
#include <mutex>

#include <algorithm>
#include <csignal>
#include <pthread.h>
#include <regex>
#include <sys/utsname.h>
#include <unistd.h>

#include "OpenLogReplicator.h"
#include "common/ClockHW.h"
#include "common/Ctx.h"
#include "common/Thread.h"
#include "common/exception/ConfigurationException.h"
#include "common/exception/RuntimeException.h"
#include "metadata/Metadata.h"

#include "ReplicatorManager.h"
#include "common/Ctx.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#ifdef LINK_LIBRARY_OCI
#define HAS_OCI " OCI"
#else
#define HAS_OCI ""
#endif /* LINK_LIBRARY_OCI */

#ifdef LINK_LIBRARY_PROTOBUF
#define HAS_PROTOBUF " Protobuf"
#ifdef LINK_LIBRARY_ZEROMQ
#define HAS_ZEROMQ " ZeroMQ"
#else
#define HAS_ZEROMQ ""
#endif /* LINK_LIBRARY_ZEROMQ */
#else
#define HAS_PROTOBUF ""
#define HAS_ZEROMQ ""
#endif /* LINK_LIBRARY_PROTOBUF */

#ifdef LINK_LIBRARY_RDKAFKA
#define HAS_KAFKA " Kafka"
#else
#define HAS_KAFKA ""
#endif /* LINK_LIBRARY_RDKAFKA */

#ifdef LINK_LIBRARY_PROMETHEUS
#define HAS_PROMETHEUS " Prometheus"
#else
#define HAS_PROMETHEUS ""
#endif /* LINK_LIBRARY_PROMETHEUS */

#ifdef LINK_STATIC
#define HAS_STATIC " static"
#else
#define HAS_STATIC ""
#endif /* LINK_STATIC */

#ifdef THREAD_INFO
#define HAS_THREAD_INFO " thread-info"
#else
#define HAS_THREAD_INFO ""
#endif /* THREAD_INFO */

namespace ReplicatorManager {
    // �߳�������
    static void thread_task(std::string id, const ThreadInfo &info) {
        // const char *logTimezone = std::getenv("OLR_LOG_TIMEZONE");
        // if (logTimezone != nullptr)
        //     if (!OpenLogReplicator::Data::parseTimezone(logTimezone, ctx->logTimezone))
        //         ctx->warning(10070, "invalid environment variable OLR_LOG_TIMEZONE value: " + std::string(logTimezone));

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

        // const int ret = mainFunction(argc, argv, &ctx);
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

        // mainFunction(argc, ps, &ctx);
        // mainCtxMap[id] = &ctx;
        // while (running) {
        //     std::this_thread::sleep_for(std::chrono::seconds(1));
        //     std::cout << "I am Thread " << id << std::endl;
        // }
        // std::cout << "Thread " << id << " stopped." << std::endl;
    }

    // 用于处理命令行输入的命令，主要支持启动和停止复制任务
    void ReplicatorManager::process_command(const std::string &cmd) {
        /*
        使用 istringstream 将输入的命令字符串解析成两部分：动作(action)和任务ID
        期望的格式是 "动作 ID"，例如 "start 1" 或 "stop 2"
        */
        std::istringstream iss(cmd);
        std::string action;
        std::string id;

        if (!(iss >> action >> id)) {
            std::cout << "Invalid command!" << std::endl;
            return;
        }

        std::lock_guard<std::mutex> lock(map_mutex); // 保护线程map的互斥锁

        if (action == "start") {
            /*
            首先检查同ID的任务是否已存在
            定义一个默认的JSON配置，包含:
            Oracle数据库连接信息
            表过滤规则
            输出目的地和格式设置
            内存配置
            调用 start 方法创建新任务线程
            */
            if (threads.find(id) != threads.end()) {
                std::cout << "Thread " << id << " already exists!" << std::endl;
                return;
            }
            std::string config = R"({
              "version": "1.8.5",
              "trace": 0,
              "source": [
                {
                  "alias": "S1",
                  "name": "DB1",
                  "reader": {
                    "type": "online",
                    "user": "USR1",
                    "password": "USR1PWD",
                    "server": "//172.17.0.1:4000/XE"
                  },
                  "format": {
                    "type": "json",
                    "column": 2
                  },
                  "flags": 96,
                  "memory": {
                    "min-mb": 32,
                    "max-mb": 1024
                  },
                  "filter": {
                    "table": [
                      {
                        "owner": "USR1",
                        "table": ".*"
                      }
                    ]
                  }
                }
              ],
              "target": [
                {
                  "alias": "T1",
                  "source": "S1",
                  "writer": {
                    "type": "file",
                    "output": "./output1_%i.json",
                    "max-file-size": 5000000
                  }
                }
              ]
            }
            )";
            //创建新任务线程
            start(id, std::move(config));
        } else if (action == "stop") {
            auto it = threads.find(id);
            if (it == threads.end()) {
                std::cout << "Thread " << id << " not found!" << std::endl;
                return;
            }
            stop(id);

            // ָֹͣ���߳�

        } else {
            std::cout << "Unknown command: " << action << std::endl;
        }
    }

    void ReplicatorManager::exit() {
        // ֹͣ�����߳�
        std::lock_guard<std::mutex> lock(map_mutex);
        for (auto &[id, info]: threads) {
            info.running = false;
            if (info.thread->joinable()) {
                info.thread->join();
            }
        }
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
        it->second.running = false; // ����ֹͣ��־
        it->second.ctx->stopHard();
        // it->second
        if (it->second.thread->joinable()) {
            it->second.thread->join(); // �ȴ��߳̽���
        }
        threads.erase(it); // ��map���Ƴ�
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

                //                 if (i + 1 < argc && (arg == "-p" || arg == "--process")) {
                //                     // Custom process name
                // #if __linux__
                //                     pthread_setname_np(pthread_self(), argv[i + 1]);
                // #endif
                // #if __APPLE__
                //                     pthread_setname_np(argv[i + 1]);
                // #endif
                //                     ++i;
                //                     continue;
                //                 }

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
