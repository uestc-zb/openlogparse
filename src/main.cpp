/* Main program
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#include <algorithm>
#include <csignal>
#include <pthread.h>
#include <regex>
#include <sys/utsname.h>
#include <thread>
#include <unistd.h>

#include "OpenLogReplicator.h"
#include "common/ClockHW.h"
#include "common/Ctx.h"
#include "common/Thread.h"
#include "common/exception/ConfigurationException.h"
#include "common/exception/DataException.h"
#include "common/exception/RuntimeException.h"
#include "common/types/Data.h"
#include "metadata/Metadata.h"
#include "ReplicatorManager.h"
#include "ReplicatorHttpServer.h"

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

namespace {
    std::map<std::string, std::shared_ptr<OpenLogReplicator::Ctx>> mainCtxMap;
    // 添加一个全局变量来控制程序退出
    volatile bool shouldTerminate = false;

    void printStacktrace() {
        for (const auto &[id, ctx]: mainCtxMap) {
            ctx->printStacktrace();
        }
    }

    void signalHandler(int s) {
        // 在接收到SIGINT信号时设置终止标志
        if (s == SIGINT) {
            shouldTerminate = true;
            std::cout << "\n收到终止信号，程序即将退出...\n";
            // 添加对HTTP服务器的关闭调用
            ReplicatorHttpServer::shutdown();
        }
        
        for (auto &[id, ctx]: mainCtxMap) {
            ctx->signalHandler(s);
        }
    }

    void signalCrash(int sig __attribute__((unused))) {
        printStacktrace();
        exit(1);
    }

    void signalDump(int sig __attribute__((unused))) {
        printStacktrace();
        for (auto &[id, ctx]: mainCtxMap) {
            ctx->signalDump();
        }
    }

int main(int argc, char **argv) {
    signal(SIGINT, signalHandler);
    signal(SIGPIPE, signalHandler);
    signal(SIGSEGV, signalCrash);
    signal(SIGUSR1, signalDump);
    
    // 在独立线程中启动HTTP服务器
    std::thread httpThread([]() {
        ReplicatorHttpServer::registerServer();
    });
    //httpThread.detach(); // 或者在程序结束时join
    /*
    std::string input;
    std::cout << "Command format:\n"
            << "  start <id>  \n"
            << "  stop <id>   \n"
            << "  exit        \n";


    ReplicatorManager::ReplicatorManager replicator_manager;
    while (true) {
        std::cout << "> ";
        std::getline(std::cin, input);

        if (input == "exit") {
            replicator_manager.exit();
            break;
        }

        replicator_manager.process_command(input);
    }
    */
    std::shared_ptr<OpenLogReplicator::Ctx> ctx = std::make_shared<OpenLogReplicator::Ctx>();


    // const char *logTimezone = std::getenv("OLR_LOG_TIMEZONE");
    // if (logTimezone != nullptr)
    //     if (!OpenLogReplicator::Data::parseTimezone(logTimezone, ctx->logTimezone))
    //         ctx->warning(10070, "invalid environment variable OLR_LOG_TIMEZONE value: " + std::string(logTimezone));

    std::string olrLocales;
    const char *olrLocalesStr = getenv("OLR_LOCALES");
    if (olrLocalesStr != nullptr)
        olrLocales = olrLocalesStr;
    if (olrLocales == "MOCK")
        OLR_LOCALES = OpenLogReplicator::Ctx::LOCALES::MOCK;
    std::string id = "1";
    mainCtxMap[id] = ctx;
    // const int ret = mainFunction(argc, argv, &ctx);
    std::string fileName = "scripts/OpenLogReplicator" + id + ".json";
    const char *ps[2] = {"-f", fileName.c_str()};
    //std::thread threadF(mainFunction, argc, ps, &ctx);
    //const int ret = mainFunction(argc, ps, &ctx);
    while (!shouldTerminate) {
        // 添加短暂的睡眠，避免CPU占用过高
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        // 可以在这里添加需要周期性执行的代码
    }
    //  等待线程结束
    if (httpThread.joinable()) {
        httpThread.join();
    }
    signal(SIGINT, nullptr);
    signal(SIGPIPE, nullptr);
    signal(SIGSEGV, nullptr);
    signal(SIGUSR1, nullptr);
    mainCtxMap.clear();
    //exit(0);
    return 0;
}
