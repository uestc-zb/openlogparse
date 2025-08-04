//
// Created by tangh on 2025/5/23.
//

#ifndef REPLICATORMANAGER_H
#define REPLICATORMANAGER_H

#include "ReplicatorManager.h"
#include <iostream>
#include <thread>
#include <atomic>
#include <unordered_map>
#include <string>
#include <sstream>
#include <mutex>

#include "common/Ctx.h"


namespace ReplicatorManager {
    // 表示单个复制任务的线程信息结构体
    struct ThreadInfo {
        std::atomic<bool> running; // // 线程运行状态标志
        std::unique_ptr<std::thread> thread; // 实际线程对象
        std::unique_ptr<OpenLogReplicator::Ctx> ctx;  // 复制任务上下文
    };

    static void thread_task(std::string id, const ThreadInfo &info);

    int mainFunction(int argc, const char **argv, OpenLogReplicator::Ctx *mainCtx);

    class ReplicatorManager final {
    protected:
        std::mutex map_mutex; //// 保护线程map的互斥锁

    public:
        // 存储所有复制任务的哈希表，键为任务ID
        std::unordered_map<std::string, ThreadInfo> threads; 
        // 处理控制命令
        void process_command(const std::string &cmd);
        // 启动新复制任务，参数为任务ID和JSON配置
        void start(std::string id, const std::string &config_basic_string);
        // 停止指定ID的复制任务
        void stop(std::string id);
        // 更新运行中任务的配置
        void updateConfig(std::string id, const std::string &newConfig);
        // 获取指定任务的状态信息（包括配置）
        std::string getStatus(const std::string& id);
        // 退出，停止所有任务
        void exit();
    };
};


#endif //REPLICATORMANAGER_H
