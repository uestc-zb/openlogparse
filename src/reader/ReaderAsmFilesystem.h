//
// Created by zhangb on 2025/7/30.
//

#ifndef READER_ASM_FILESYSTEM_H_
#define READER_ASM_FILESYSTEM_H_
#include "ReaderFilesystem.h"
#include <memory>
#include <string>
#include <libssh/libssh.h>

namespace OpenLogReplicator {
    class ReaderAsmFilesystem final : public ReaderFilesystem {
    protected:
        void redoClose() override;
        REDO_CODE redoOpen() override;
        int redoRead(uint8_t* buf, uint64_t offset, uint size) override;

    public:
        ReaderAsmFilesystem(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum);
        ~ReaderAsmFilesystem() override;

        void setAsmConfig(const std::string& host, const std::string& user,
                         const std::string& password, int port,
                         const std::string& container, const std::string& oraHome,
                         const std::string& oraSid);
    private:
        REDO_CODE setupSshConnection();
        REDO_CODE transferAsmFileToMemory();
        void closeSshConnection();

        // SSH 连接相关
        ssh_session sshSession{nullptr};
        ssh_channel sshChannel{nullptr};
        std::string sshHost;
        std::string sshUser;
        std::string sshPassword;
        int sshPort{22};

        // Docker 配置
        std::string dockerContainer;
        std::string oracleHome;
        std::string oracleSid;

        // 内存缓冲区
        std::unique_ptr<uint8_t[]> fileBuffer;
        uint64_t bufferSize{0};
        uint64_t dataLength{0};
    };
}

#endif
