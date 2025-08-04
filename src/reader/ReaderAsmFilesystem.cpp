

#define _LARGEFILE_SOURCE
enum {
_FILE_OFFSET_BITS = 64
};

#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "ReaderAsmFilesystem.h"

namespace OpenLogReplicator
{
    ReaderAsmFilesystem::ReaderAsmFilesystem(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum) :
            ReaderFilesystem(newCtx, std::move(newAlias), std::move(newDatabase), newGroup, newConfiguredBlockSum) {
        // 设置 ASM 配置参数（SSH 连接和 Docker 环境）
        setAsmConfig(
            "192.168.101.66",  // SSH 服务器主机地址
            "root",            // SSH 登录用户名
            "yunzx123",        // SSH 登录密码
            22,                // SSH 服务器端口号
            "racnode1",     // Docker 容器名称
            "/u01/app/oracle/product/21.3.0/dbhome_1", // Oracle 主目录路径
            "ORCLCDB1"        // Oracle 实例 SID
        );

        // 记录当前实例的配置信息
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "initialized ReaderAsmFilesystem for instance " +
                         std::to_string(newGroup) + ", container: " + "racnode1" +
                         ", sid: " + "ORCLCDB1");
    }

    ReaderAsmFilesystem::~ReaderAsmFilesystem() {
        ReaderAsmFilesystem::redoClose();
    }

    // 设置 ASM 配置参数的方法
    void ReaderAsmFilesystem::setAsmConfig(const std::string& host, const std::string& user,
                                       const std::string& password, int port,
                                       const std::string& container, const std::string& oraHome,
                                       const std::string& oraSid) {
        sshHost = host;              // 保存 SSH 主机地址
        sshUser = user;              // 保存 SSH 用户名
        sshPassword = password;      // 保存 SSH 密码
        sshPort = port;              // 保存 SSH 端口号
        dockerContainer = container; // 保存 Docker 容器名
        oracleHome = oraHome;        // 保存 Oracle 主目录路径
        oracleSid = oraSid;         // 保存 Oracle 实例 SID
    }

    void ReaderAsmFilesystem::redoClose() {
        // ASM 模式：释放内存缓冲区和 SSH 连接
        fileBuffer.reset();  // 释放文件缓冲区内存
        bufferSize = 0;      // 重置缓冲区大小
        dataLength = 0;      // 重置数据长度
        closeSshConnection(); // 关闭 SSH 连接

    }

    Reader::REDO_CODE ReaderAsmFilesystem::redoOpen() {
        // ASM 模式：建立 SSH 连接并传输文件到内存
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))  // 检查是否启用文件跟踪日志
            ctx->logTrace(Ctx::TRACE::FILE, "opening ASM file: " + fileName);  // 记录打开 ASM 文件的日志

        // 步骤1：建立 SSH 连接
        REDO_CODE result = setupSshConnection();  // 调用建立 SSH 连接的方法
        if (result != REDO_CODE::OK) {  // 检查连接是否成功
            return result;  // 连接失败则返回错误码
        }

        // 步骤2：传输 ASM 文件到内存
        result = transferAsmFileToMemory();  // 调用文件传输方法
        if (result != REDO_CODE::OK) {  // 检查传输是否成功
            closeSshConnection();  // 传输失败则关闭 SSH 连接
            return result;  // 返回错误码
        }

        // 步骤3：设置文件大小
        fileSize = dataLength;  // 设置文件大小为实际传输的数据长度

        // 记录成功加载文件的信息日志
        ctx->info(0, "ASM file loaded to memory: " + fileName +
                     ", size: " + std::to_string(dataLength) + " bytes");

        return REDO_CODE::OK;  // 返回成功状态
    }

    // 建立 SSH 连接的方法
    Reader::REDO_CODE ReaderAsmFilesystem::setupSshConnection() {
        contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别操作

        // 创建新的 SSH 会话
        sshSession = ssh_new();  // 调用 libssh 创建新会话
        if (sshSession == nullptr) {  // 检查创建是否成功
            contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别
            ctx->error(10001, "failed to create SSH session");  // 记录错误日志
            return REDO_CODE::ERROR;  // 返回错误码
        }

        // 设置 SSH 连接参数
        ssh_options_set(sshSession, SSH_OPTIONS_HOST, sshHost.c_str());     // 设置主机地址
        ssh_options_set(sshSession, SSH_OPTIONS_PORT, &sshPort);            // 设置端口号
        ssh_options_set(sshSession, SSH_OPTIONS_USER, sshUser.c_str());     // 设置用户名

        // 连接到 SSH 服务器
        int rc = ssh_connect(sshSession);  // 执行连接操作
        if (rc != SSH_OK) {  // 检查连接是否成功
            contextSet(CONTEXT::CPU);  // 恢复上下文
            ctx->error(10002, "SSH connection failed: " + std::string(ssh_get_error(sshSession)));  // 记录连接错误
            ssh_free(sshSession);  // 释放 SSH 会话资源
            sshSession = nullptr;  // 重置会话指针
            return REDO_CODE::ERROR;  // 返回错误码
        }
        int none_result = ssh_userauth_none(sshSession, nullptr);
        int auth_methods = ssh_userauth_list(sshSession, nullptr);

        if (auth_methods == 0) {
            contextSet(CONTEXT::CPU);
            ctx->error(10003, "获取服务器认证方法失败，可能是认证协商问题");
            ssh_disconnect(sshSession);
            ssh_free(sshSession);
            sshSession = nullptr;
            return REDO_CODE::ERROR;
        }

        if (none_result == SSH_AUTH_SUCCESS) {
            ctx->info(0, "SSH无密码认证成功");
            contextSet(CONTEXT::CPU);
            ctx->info(0, "SSH认证成功，连接已建立到: " + sshHost);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
                ctx->logTrace(Ctx::TRACE::FILE, "SSH connection established to: " + sshHost);
            return REDO_CODE::OK;
        }

        // 公钥认证
        if (auth_methods & SSH_AUTH_METHOD_PUBLICKEY) {
            ctx->info(0, "服务器支持公钥认证，尝试使用公钥...");
            rc = ssh_userauth_publickey_auto(sshSession, nullptr, nullptr);
            if (rc == SSH_AUTH_SUCCESS) {
                ctx->info(0, "SSH公钥认证成功");
                contextSet(CONTEXT::CPU);
                ctx->info(0, "SSH认证成功，连接已建立到: " + sshHost);
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
                    ctx->logTrace(Ctx::TRACE::FILE, "SSH connection established to: " + sshHost);
                return REDO_CODE::OK;
            } else {
                std::string error_msg = ssh_get_error(sshSession);
                ctx->error(10004, "SSH公钥认证失败: " + error_msg);
            }
        }

        if (!(auth_methods & SSH_AUTH_METHOD_PASSWORD)) {
            contextSet(CONTEXT::CPU);
            ctx->error(10003, "服务器不支持密码认证，支持的方法: " + std::to_string(auth_methods));
            ssh_disconnect(sshSession);
            ssh_free(sshSession);
            sshSession = nullptr;
            return REDO_CODE::ERROR;
        }

        // 进行密码认证
        rc = ssh_userauth_password(sshSession, nullptr, sshPassword.c_str());  // 使用密码进行用户认证
        if (rc != SSH_AUTH_SUCCESS) {  // 检查认证是否成功
            contextSet(CONTEXT::CPU);  // 恢复上下文
            ctx->error(10003, "SSH authentication failed");  // 记录认证失败错误
            ssh_disconnect(sshSession);  // 断开 SSH 连接
            ssh_free(sshSession);  // 释放 SSH 会话资源
            sshSession = nullptr;  // 重置会话指针
            return REDO_CODE::ERROR;  // 返回错误码
        }

        contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别

        ctx->info(0, "SSH认证成功，连接已建立到: " + sshHost);
        // 记录成功建立连接的跟踪日志
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "SSH connection established to: " + sshHost);

        return REDO_CODE::OK;  // 返回成功状态
    }

    // 将 ASM 文件传输到内存的方法
    Reader::REDO_CODE ReaderAsmFilesystem::transferAsmFileToMemory() {
        // 构建文件传输命令
        std::ostringstream cmdStream;  // 创建字符串流用于构建命令
        if (fileName.find('+') == 0) {  // 检查文件名是否以 '+' 开头（ASM 文件标识）
            // ASM 文件：使用 asmcmd 和 FIFO 管道传输
            cmdStream << "docker exec " << dockerContainer << " bash -c '"  // 执行 Docker 命令
                     << "export ORACLE_HOME=" << oracleHome << "; "          // 设置 Oracle 主目录环境变量
                     << "export ORACLE_SID=" << oracleSid << "; "           // 设置 Oracle 实例 SID 环境变量
                     << "export PATH=$ORACLE_HOME/bin:$PATH; "              // 更新 PATH 环境变量
                     << "export LD_LIBRARY_PATH=$ORACLE_HOME/lib:$LD_LIBRARY_PATH; "  // 更新库路径环境变量
                     << "fifo_name=\"/tmp/asm_fifo_" << oracleSid << "_$(date +%s%N | cut -b1-19)_$$\"; "  // 创建唯一的 FIFO 文件名（包含实例SID、纳秒时间戳和进程ID）
                     << "mkfifo \"$fifo_name\"; "                           // 创建命名管道
                     << "cat \"$fifo_name\" & "                             // 后台启动读取管道的进程
                     << "cat_pid=$!; "                                      // 保存 cat 进程的 PID
                     << "asmcmd cp " << fileName << " \"$fifo_name\" >&2; " // 使用 asmcmd 复制文件到管道
                     << "wait $cat_pid; "                                   // 等待 cat 进程完成
                     << "rm -f \"$fifo_name\"'";                           // 删除临时 FIFO 文件
        } else {
            // 普通文件系统文件：直接使用 cat 输出
            cmdStream << "docker exec --user oracle " << dockerContainer   // 以 oracle 用户执行 Docker 命令
                     << " bash -c 'cat " << fileName << "'";               // 使用 cat 命令读取文件内容
        }

        std::string command = cmdStream.str();  // 将字符串流转换为字符串

        // 记录要执行的命令（如果启用跟踪）
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "executing command: " + command);

        contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别

        if (sshSession == nullptr || ssh_is_connected(sshSession) == 0) {
            ctx->error(10010, "SSH session is not connected");
            return REDO_CODE::ERROR;
        }
        // 创建 SSH 通道
        sshChannel = ssh_channel_new(sshSession);  // 基于 SSH 会话创建新的通道
        if (sshChannel == nullptr) {  // 检查通道创建是否成功
            contextSet(CONTEXT::CPU);  // 恢复上下文
            ctx->error(10004, "failed to create SSH channel");  // 记录错误日志
            return REDO_CODE::ERROR;  // 返回错误码
        }

        // 打开 SSH 通道会话
        int rc = ssh_channel_open_session(sshChannel);  // 打开通道会话
        if (rc != SSH_OK) {  // 检查打开是否成功
            contextSet(CONTEXT::CPU);  // 恢复上下文
            ctx->error(10005, "failed to open SSH channel");  // 记录错误日志
            ssh_channel_free(sshChannel);  // 释放通道资源
            sshChannel = nullptr;  // 重置通道指针
            return REDO_CODE::ERROR;  // 返回错误码
        }

        // 在 SSH 通道中执行命令
        rc = ssh_channel_request_exec(sshChannel, command.c_str());  // 请求执行命令
        if (rc != SSH_OK) {  // 检查执行请求是否成功
            contextSet(CONTEXT::CPU);  // 恢复上下文
            ctx->error(10006, "failed to execute SSH command");  // 记录错误日志
            ssh_channel_close(sshChannel);  // 关闭 SSH 通道
            ssh_channel_free(sshChannel);   // 释放通道资源
            sshChannel = nullptr;  // 重置通道指针
            return REDO_CODE::ERROR;  // 返回错误码
        }

        contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别

        // 初始化内存缓冲区参数
        const size_t initialBufferSize = 64 * 1024 * 1024; // 64MB 初始缓冲区大小
        const size_t chunkSize = 64 * 1024;              // 1MB 每次读取块大小

        bufferSize = initialBufferSize;  // 设置缓冲区大小
        fileBuffer = std::make_unique<uint8_t[]>(bufferSize);  // 分配内存缓冲区
        dataLength = 0;  // 初始化数据长度为 0

        // 循环读取数据直到完成或被中断
        while (!ctx->hardShutdown) {  // 检查是否收到硬停止信号
            // 检查缓冲区是否需要扩容
            if (dataLength + chunkSize > bufferSize) {  // 当前数据加新读取的数据超过缓冲区大小
                uint64_t newBufferSize = bufferSize * 2;  // 新缓冲区大小为当前的两倍
                auto newBuffer = std::make_unique<uint8_t[]>(newBufferSize);  // 分配新的更大缓冲区
                memcpy(newBuffer.get(), fileBuffer.get(), dataLength);  // 复制现有数据到新缓冲区
                fileBuffer = std::move(newBuffer);  // 使用新缓冲区替换旧缓冲区
                bufferSize = newBufferSize;  // 更新缓冲区大小记录

                // 记录缓冲区扩展的跟踪日志
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
                    ctx->logTrace(Ctx::TRACE::FILE, "expanded buffer to: " + std::to_string(bufferSize));
            }

            // 将新读取的数据复制到主缓冲区
            contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别
            int bytesRead = ssh_channel_read(sshChannel, fileBuffer.get() + dataLength,
                                       std::min(chunkSize, bufferSize - dataLength), 0);
            contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别

            if (bytesRead == 0) {  // 检查是否读取到文件末尾
                break; // EOF - 文件结束，退出循环
            }

            if (bytesRead < 0) {  // 检查是否发生读取错误
                ctx->error(10007, "SSH channel read error");  // 记录读取错误
                return REDO_CODE::ERROR;  // 返回错误码
            }
            dataLength += bytesRead;  // 更新总数据长度

            // 每 10MB 记录一次进度日志
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)) && dataLength % (10 * 1024 * 1024) == 0)
                ctx->logTrace(Ctx::TRACE::FILE, "loaded: " + std::to_string(dataLength / 1024 / 1024) + "MB");
        }

        if (dataLength == 0) {
            ctx->error(10009, "transferred data is empty, possible error during read");
            return REDO_CODE::ERROR;
        }

        // 检查命令执行的退出状态
        contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别
        int exitStatus = ssh_channel_get_exit_status(sshChannel);  // 获取命令执行的退出状态码
        contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别

        if (exitStatus != 0) {  // 检查命令是否成功执行（退出码为 0 表示成功）
            ctx->error(10008, "command execution failed with exit code: " + std::to_string(exitStatus));  // 记录命令执行失败
            return REDO_CODE::ERROR;  // 返回错误码
        }

        // 关闭 SSH 通道并清理资源
        contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别
        ssh_channel_close(sshChannel);  // 关闭 SSH 通道
        ssh_channel_free(sshChannel);   // 释放通道资源
        contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别
        sshChannel = nullptr;  // 重置通道指针

        // 记录传输完成的跟踪日志
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "transfer completed, total bytes: " + std::to_string(dataLength));

        return REDO_CODE::OK;  // 返回成功状态
    }


    int ReaderAsmFilesystem::redoRead(uint8_t* buf, uint64_t offset, uint size) {
        // ASM 模式：从内存缓冲区读取数据
        if (fileBuffer == nullptr || offset >= dataLength) {  // 检查缓冲区是否存在且偏移量有效
            return 0; // 返回 0 表示 EOF 或无效偏移量
        }

        // 计算实际可读取的字节数
        uint64_t availableBytes = dataLength - offset;  // 计算从偏移量开始的可用字节数
        uint64_t bytesToRead = std::min(static_cast<uint64_t>(size), availableBytes);  // 取请求大小和可用字节数的较小值

        // 从内存缓冲区复制数据到调用者提供的缓冲区
        memcpy(buf, fileBuffer.get() + offset, bytesToRead);  // 复制指定偏移量和大小的数据

        // 记录读取操作的跟踪日志
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "read from memory buffer: offset=" + std::to_string(offset) +
                         " size=" + std::to_string(size) + " returned=" + std::to_string(bytesToRead));

        return static_cast<int>(bytesToRead);  // 返回实际读取的字节数
    }

    // 关闭 SSH 连接的方法
    void ReaderAsmFilesystem::closeSshConnection() {
        if (sshChannel) {  // 检查 SSH 通道是否存在
            contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别
            ssh_channel_close(sshChannel);  // 关闭 SSH 通道
            ssh_channel_free(sshChannel);   // 释放通道资源
            contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别
            sshChannel = nullptr;  // 重置通道指针为空
        }

        if (sshSession) {  // 检查 SSH 会话是否存在
            contextSet(CONTEXT::OS, REASON::OS);  // 设置上下文为操作系统级别
            ssh_disconnect(sshSession);  // 断开 SSH 连接
            ssh_free(sshSession);        // 释放会话资源
            contextSet(CONTEXT::CPU);  // 恢复上下文为 CPU 级别
            sshSession = nullptr;  // 重置会话指针为空
        }

        // 记录 SSH 连接关闭的跟踪日志
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "SSH connection closed");
    }
}