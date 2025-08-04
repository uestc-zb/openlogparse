//
// Created by tangh on 2025/5/28.
//

#ifndef REPLICATORHTTPSERVER_H
#define REPLICATORHTTPSERVER_H
#include <rapidjson/document.h>

// 添加前向声明
namespace hv {
    class HttpServer;
}

namespace ReplicatorHttpServer {
    extern std::string DEFAULT_JSON_CONFIG;
    extern volatile bool serverRunning;
    extern hv::HttpServer* httpServer; // 添加全局指针
    void shutdown(); // 添加关闭方法
    void registerServer();
    void mergeConfigJson(rapidjson::Value& target, const rapidjson::Value& source, rapidjson::Document::AllocatorType& allocator);
    class ReplicatorHttpServer {
    };
}


#endif //REPLICATORHTTPSERVER_H
