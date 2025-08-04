#include <hv/HttpServer.h>
#include "ReplicatorHttpServer.h"
#include "ReplicatorManager.h"
#include <rapidjson/document.h>
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace ReplicatorHttpServer {
    // 添加全局变量
    volatile bool serverRunning = true;
    hv::HttpServer* httpServer = nullptr;


    void registerServer() {
        // 创建复制管理器和HTTP路由器
        ReplicatorManager::ReplicatorManager replicator_manager;
        HttpService router;
        router.GET("/ping", [](HttpRequest *req, HttpResponse *resp) {
            return resp->String("pong");
        });

        router.GET("/data", [](HttpRequest *req, HttpResponse *resp) {
            static char data[] = "0123456789";
            return resp->Data(data, 10);
        });

        router.GET("/paths", [&router](HttpRequest *req, HttpResponse *resp) {
            return resp->Json(router.Paths());
        });

        router.GET("/get", [](HttpRequest *req, HttpResponse *resp) {
            resp->json["origin"] = req->client_addr.ip;
            resp->json["url"] = req->url;
            resp->json["args"] = req->query_params;
            resp->json["headers"] = req->headers;
            return 200;
        });

        // router.GET("/start/{id}", [](HttpRequest *req, HttpResponse *resp) {
        //     return resp->String(req->json);
        // });

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
            // std::cout << buffer.GetString() << std::endl;
            // 5. 启动复制任务
            replicator_manager.start(ctx->param("id"), buffer.GetString());
            return ctx->send(R"({"msg":"success"})", ctx->type());
        });

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

        router.POST("/stop/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            auto it = replicator_manager.threads.find(ctx->param("id"));
            if (it == replicator_manager.threads.end()) {
                return ctx->send("Thread " + ctx->param("id") + " not found!", ctx->type());
            }
            replicator_manager.stop(ctx->param("id"));
            return ctx->send(R"({"msg":"success"})", ctx->type());
        });

        // 新增配置更新接口
        router.POST("/update/{id}", [&replicator_manager](const HttpContextPtr &ctx) {
            // 1. 检查指定ID的任务是否存在
            auto it = replicator_manager.threads.find(ctx->param("id"));
            if (it == replicator_manager.threads.end()) {
                return ctx->send("Thread " + ctx->param("id") + " not found!", ctx->type());
            }

            // 2. 配置合并流程
            rapidjson::Document oldconfig;
            oldconfig.Parse(it->second.ctx->config.c_str());
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

        router.POST("/echo", [](const HttpContextPtr &ctx) {
            return ctx->send(ctx->body(), ctx->type());
        });
        /*
        // 创建和配置HTTP服务器
        hv::HttpServer server(&router);
        server.setPort(8078);
        server.setThreadNum(4);
        server.run();  // 阻塞式启动服务器
        */
        // 创建和配置HTTP服务器
        httpServer = new hv::HttpServer(&router);
        httpServer->setPort(8078);
        httpServer->setThreadNum(4);
        
        // 修改为检查标志的循环
        while (serverRunning) {
            // 使用正确的调用方式
            httpServer->run(); // 不使用超时参数，改用短循环
            std::this_thread::sleep_for(std::chrono::milliseconds(100)); // 短暂休眠
        }
        
        // 清理资源
        if (httpServer) {
            delete httpServer;
            httpServer = nullptr;
        }
    }

    // 添加关闭函数
    void shutdown() {
        serverRunning = false;
        // 如果HTTP服务器支持立即停止，也可以调用相关方法
        if (httpServer) {
            httpServer->stop();
        }
    }


    void mergeConfigJson(rapidjson::Value &target, const rapidjson::Value &source,
                   rapidjson::Document::AllocatorType &allocator) {
        if (!target.IsObject() || !source.IsObject()) {
            return;
        }
        // 遍历源对象中的每个键值对
        for (rapidjson::Value::ConstMemberIterator it = source.MemberBegin(); it != source.MemberEnd(); ++it) {
            const rapidjson::Value &srcKey = it->name;
            const rapidjson::Value &srcVal = it->value;

            // 查找目标对象中是否存在相同键
            rapidjson::Value::MemberIterator targetIt = target.FindMember(srcKey);
            if (targetIt != target.MemberEnd()) {
                if (srcKey == "source" || srcKey == "target") {
                    mergeConfigJson(targetIt->value.GetArray()[0], srcVal.GetArray()[0], allocator);
                }
                // Key exists in both objects
                else if (targetIt->value.IsObject() && srcVal.IsObject()) {
                    // Recursively merge nested objects
                    mergeConfigJson(targetIt->value, srcVal, allocator);
                } else {
                    // Replace with new value
                    target.EraseMember(targetIt);
                    target.AddMember(
                        rapidjson::Value(srcKey, allocator).Move(),
                        rapidjson::Value(srcVal, allocator).Move(),
                        allocator
                    );
                }
            } else {
                // Add new key-value pair
                target.AddMember(
                    rapidjson::Value(srcKey, allocator).Move(),
                    rapidjson::Value(srcVal, allocator).Move(),
                    allocator
                );
            }
        }
    }


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
