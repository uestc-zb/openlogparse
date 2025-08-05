#include <hv/HttpServer.h>  // 包含HTTP服务器库
#include "ReplicatorHttpServer.h"  // 包含HTTP服务器头文件
#include "ReplicatorManager.h"     // 包含复制管理器头文件
#include <rapidjson/document.h>     // 包含RapidJSON文档处理库
#include "rapidjson/stringbuffer.h"  // 包含RapidJSON字符串缓冲区库
#include "rapidjson/writer.h"       // 包含RapidJSON写入器库

// 定义ReplicatorHttpServer命名空间
namespace ReplicatorHttpServer {
    // 添加全局变量
    volatile bool serverRunning = true;  // 服务器运行状态标志，volatile确保多线程可见性
    hv::HttpServer* httpServer = nullptr;  // HTTP服务器实例指针


    // 注册并启动HTTP服务器
    void registerServer() {
        // 创建复制管理器和HTTP路由器
        ReplicatorManager::ReplicatorManager replicator_manager;  // 创建复制管理器实例
        HttpService router;  // 创建HTTP路由器实例
        
        // 注册GET /ping端点，用于健康检查
        router.GET("/ping", [](HttpRequest *req, HttpResponse *resp) {
            return resp->String("pong");  // 返回"pong"字符串响应
        });

        // 注册GET /data端点，返回测试数据
        router.GET("/data", [](HttpRequest *req, HttpResponse *resp) {
            static char data[] = "0123456789";  // 定义测试数据
            return resp->Data(data, 10);  // 返回测试数据
        });

        // 注册GET /paths端点，返回所有已注册的路由路径
        router.GET("/paths", [&router](HttpRequest *req, HttpResponse *resp) {
            return resp->Json(router.Paths());  // 以JSON格式返回路由路径
        });

        // 注册GET /get端点，返回请求的详细信息
        router.GET("/get", [](HttpRequest *req, HttpResponse *resp) {
            resp->json["origin"] = req->client_addr.ip;     // 客户端IP地址
            resp->json["url"] = req->url;                   // 请求URL
            resp->json["args"] = req->query_params;         // 查询参数
            resp->json["headers"] = req->headers;           // 请求头
            return 200;  // 返回HTTP状态码200
        });

        // 注册POST /start/{id}端点，用于启动指定ID的复制任务
        router.POST("/start/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            // 1. 检查是否已存在同ID的任务
            if (replicator_manager.threads.find(ctx->param("id")) != replicator_manager.threads.end()) {
                return ctx->send("Thread " + ctx->param("id") + " already exists!", ctx->type());
            }

            // 2. 配置合并流程
            rapidjson::Document config;
            config.Parse(DEFAULT_JSON_CONFIG.c_str()); // 加载默认配置
            rapidjson::Document mergedConfig;
            mergedConfig.Parse(ctx->body().c_str());  // 解析用户提交的配置

            // 3. 合并配置（保留默认配置，覆盖用户自定义项）
            mergeConfigJson(config, mergedConfig, config.GetAllocator());

            // 4. 序列化合并后的配置
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            config.Accept(writer);

            // 5. 启动复制任务
            replicator_manager.start(ctx->param("id"), buffer.GetString());
            return ctx->send(R"({"msg":"success"})", ctx->type());
        });

        // 注册GET /stop/{id}端点，用于停止指定ID的复制任务
        router.GET("/stop/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            // 1. 检查指定ID的任务是否存在
            auto it = replicator_manager.threads.find(ctx->param("id"));
            if (it == replicator_manager.threads.end()) {
                return ctx->send("Thread " + ctx->param("id") + " not found!", ctx->type());
            }
            // 2. 停止该复制任务
            replicator_manager.stop(ctx->param("id"));
            return ctx->send(R"({"msg":"success"})", ctx->type());
        });

        // 注册POST /stop/{id}端点，用于停止指定ID的复制任务
        router.POST("/stop/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            auto it = replicator_manager.threads.find(ctx->param("id"));
            if (it == replicator_manager.threads.end()) {
                return ctx->send("Thread " + ctx->param("id") + " not found!", ctx->type());
            }
            replicator_manager.stop(ctx->param("id"));
            return ctx->send(R"({"msg":"success"})", ctx->type());
        });

        // 新增配置更新接口
        // 注册POST /update/{id}端点，用于更新指定ID的复制任务配置
        router.POST("/update/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            // 1. 检查指定ID的任务是否存在
            auto it = replicator_manager.threads.find(ctx->param("id"));
            if (it == replicator_manager.threads.end()) {
                return ctx->send("Thread " + ctx->param("id") + " not found!", ctx->type());
            }

            // 2. 配置合并流程
            rapidjson::Document oldconfig;
            oldconfig.Parse(it->second.ctx->config.c_str());  // 解析旧配置
            rapidjson::Document newconfig;
            newconfig.Parse(ctx->body().c_str());  // 解析用户提交的配置

            // 3. 合并配置（保留旧配置，覆盖用户自定义项）
            mergeConfigJson(oldconfig, newconfig, oldconfig.GetAllocator());

            // 4. 序列化合并后的配置
            rapidjson::StringBuffer buffer;
            rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
            oldconfig.Accept(writer);

            // 5. 更新复制任务配置
            try {
                replicator_manager.updateConfig(ctx->param("id"), buffer.GetString());
                return ctx->send(R"({"msg":"success"})", ctx->type());
            } catch (const std::exception& ex) {
                return ctx->send(
                    R"({"error":")" + std::string(ex.what()) + R"("})", 
                    ctx->type());
            }
        });

        // 新增状态检查接口
        // 注册GET /status/{id}端点，用于获取指定ID的复制任务状态
        router.GET("/status/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            try {
                // 获取指定任务的状态信息
                std::string status = replicator_manager.getStatus(ctx->param("id"));
                return ctx->send(status, ctx->type());
            } catch (const std::exception& ex) {
                return ctx->send(
                    R"({"error":")" + std::string(ex.what()) + R"("})", 
                    ctx->type());
            }
        });

        // 注册POST /echo端点，用于回显请求体内容
        router.POST("/echo", [](const HttpContextPtr &ctx) {
            return ctx->send(ctx->body(), ctx->type());
        });
        
        /*
        // 创建和配置HTTP服务器（旧版本阻塞式实现）
        hv::HttpServer server(&router);
        server.setPort(8078);       // 设置端口号
        server.setThreadNum(4);     // 设置线程数
        server.run();  // 阻塞式启动服务器
        */
        
        // 创建和配置HTTP服务器（新版本非阻塞式实现）
        httpServer = new hv::HttpServer(&router);  // 创建HTTP服务器实例
        httpServer->setPort(8078);                 // 设置端口号为8078
        httpServer->setThreadNum(4);               // 设置线程数为4
        
        // 修改为检查标志的循环，实现非阻塞式服务器运行
        while (serverRunning) {  // 当服务器运行标志为true时继续循环
            // 使用正确的调用方式
            httpServer->run(); // 不使用超时参数，改用短循环
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 短暂休眠，避免CPU占用过高
        }
        
        // 清理资源
        if (httpServer) {  // 如果HTTP服务器实例存在
            delete httpServer;    // 释放HTTP服务器实例
            httpServer = nullptr; // 将指针置为nullptr
        }
    }

    // 添加关闭函数，用于优雅地关闭HTTP服务器
    void shutdown() {
        serverRunning = false;  // 设置服务器运行标志为false，使主循环退出
        // 如果HTTP服务器支持立即停止，也可以调用相关方法
        if (httpServer) {  // 如果HTTP服务器实例存在
            httpServer->stop();  // 调用HTTP服务器的停止方法
        }
    }


    // 合并两个JSON配置对象的函数
    void mergeConfigJson(rapidjson::Value &target, const rapidjson::Value &source,
                   rapidjson::Document::AllocatorType &allocator) {
        // 检查目标和源是否都是对象类型
        if (!target.IsObject() || !source.IsObject()) {
            return;  // 如果不是对象类型，直接返回
        }
        
        // 遍历源对象中的每个键值对
        for (rapidjson::Value::ConstMemberIterator it = source.MemberBegin(); it != source.MemberEnd(); ++it) {
            const rapidjson::Value &srcKey = it->name;   // 源键名
            const rapidjson::Value &srcVal = it->value;  // 源键值

            // 查找目标对象中是否存在相同键
            rapidjson::Value::MemberIterator targetIt = target.FindMember(srcKey);
            if (targetIt != target.MemberEnd()) {  // 如果目标对象中存在相同键
                // 特殊处理source和target字段
                if (srcKey == "source" || srcKey == "target") {
                    mergeConfigJson(targetIt->value.GetArray()[0], srcVal.GetArray()[0], allocator);
                }
                // Key exists in both objects
                else if (targetIt->value.IsObject() && srcVal.IsObject()) {
                    // Recursively merge nested objects
                    mergeConfigJson(targetIt->value, srcVal, allocator);  // 递归合并嵌套对象
                } else {
                    // Replace with new value
                    target.EraseMember(targetIt);  // 删除目标对象中的旧值
                    target.AddMember(
                        rapidjson::Value(srcKey, allocator).Move(),     // 添加新的键
                        rapidjson::Value(srcVal, allocator).Move(),     // 添加新的值
                        allocator
                    );
                }
            } else {
                // Add new key-value pair
                target.AddMember(
                    rapidjson::Value(srcKey, allocator).Move(),   // 添加新的键
                    rapidjson::Value(srcVal, allocator).Move(),   // 添加新的值
                    allocator
                );
            }
        }
    }


    // 默认JSON配置字符串
    std::string DEFAULT_JSON_CONFIG = R"({
              "version": "1.8.5",
              "trace": 0,
              "source": [
                {
                  "alias": "S1",
                  "name": "DB1",
                  "reader": {
                    "type": "online",
                    "asm": {}
                  },
                  "format": {
                    "type": "json",
                    "column": 2,
                    "timestamp-all":1
                  },
                  "flags": 96,
                  "memory": {
                    "min-mb": 32,
                    "max-mb": 1024
                  }
                }
              ],
              "target": [
                {
                  "alias": "T1",
                  "source": "S1",
                  "writer": {
                    "type": "file",
                    "output": "./output_%i.json",
                    "max-file-size": 50000000
                  }
                }
              ]
            }
            )";

}
