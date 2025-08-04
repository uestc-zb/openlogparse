//
// Created by tangh on 2025/5/28.
//

#ifndef REPLICATORHTTPSERVER_H  // 防止头文件重复包含的预处理指令开始
#define REPLICATORHTTPSERVER_H  // 定义REPLICATORHTTPSERVER_H宏，防止重复包含
#include <rapidjson/document.h>  // 包含RapidJSON库的文档处理头文件

// 添加前向声明，避免包含整个hv命名空间的头文件，减少编译依赖
namespace hv {
    class HttpServer;  // 前向声明hv命名空间中的HttpServer类
}

// 定义ReplicatorHttpServer命名空间，将相关功能封装在一起
namespace ReplicatorHttpServer {
    extern std::string DEFAULT_JSON_CONFIG;  // 声明外部链接的默认JSON配置字符串
    extern volatile bool serverRunning;      // 声明外部链接的服务器运行状态标志，volatile确保多线程可见性
    extern hv::HttpServer* httpServer;       // 声明外部链接的HTTP服务器实例指针
    void shutdown();                         // 声明关闭HTTP服务器的函数
    void registerServer();                   // 声明注册并启动HTTP服务器的函数
    void mergeConfigJson(rapidjson::Value& target, const rapidjson::Value& source, rapidjson::Document::AllocatorType& allocator);  // 声明合并JSON配置的函数
    class ReplicatorHttpServer {             // 定义ReplicatorHttpServer类（当前为空类）
    };
}

#endif //REPLICATORHTTPSERVER_H  // 结束防止头文件重复包含的预处理指令