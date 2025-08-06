#define _LARGEFILE_SOURCE
#define _FILE_OFFSET_BITS 64

#include <algorithm>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <thread>
#include <unistd.h>

#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../common/exception/RuntimeException.h"
#include "../common/metrics/Metrics.h"
#include "../common/types/Seq.h"
#include "Reader.h"

namespace OpenLogReplicator {
    const char* Reader::REDO_MSG[]{"OK", "OVERWRITTEN", "FINISHED", "STOPPED", "SHUTDOWN", "EMPTY", "READ ERROR",
                                   "WRITE ERROR", "SEQUENCE ERROR", "CRC ERROR", "BLOCK ERROR", "BAD DATA ERROR",
                                   "OTHER ERROR"};

    Reader::Reader(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum) :
            Thread(newCtx, std::move(newAlias)),
            database(std::move(newDatabase)),
            configuredBlockSum(newConfiguredBlockSum),
            group(newGroup) {
    }

    /**
     * @brief 初始化读取器资源
     * 
     * 此函数用于初始化读取器所需的内存缓冲区和其他资源。
     * 包括分配重做日志缓冲区列表、头部缓冲区，并检查重做日志复制路径。
     */
    void Reader::initialize() {
        // 如果重做日志缓冲区列表未分配，则分配内存
        if (redoBufferList == nullptr) {
            redoBufferList = new uint8_t* [ctx->memoryChunksReadBufferMax];
            memset(reinterpret_cast<void*>(redoBufferList), 0, ctx->memoryChunksReadBufferMax * sizeof(uint8_t*));
        }

        // 如果头部缓冲区未分配，则分配内存
        if (headerBuffer == nullptr) {
            headerBuffer = reinterpret_cast<uint8_t*>(aligned_alloc(Ctx::MEMORY_ALIGNMENT, PAGE_SIZE_MAX * 2));
            if (unlikely(headerBuffer == nullptr))
                throw RuntimeException(10016, "couldn't allocate " + std::to_string(PAGE_SIZE_MAX * 2) +
                                              " bytes memory for: read header");
        }

        // 如果重做日志复制路径不为空，则检查路径是否可访问
        if (!ctx->redoCopyPath.empty()) {
            if (opendir(ctx->redoCopyPath.c_str()) == nullptr)
                throw RuntimeException(10012, "directory: " + ctx->redoCopyPath + " - can't read");
        }
    }

    /**
     * @brief 唤醒读取器线程
     * 
     * 当需要中断读取器的等待状态时调用此函数，例如在系统关闭或需要重新加载配置时。
     * 该函数会通知所有等待条件变量的线程，包括缓冲区满、读取器休眠和解析器休眠的线程。
     */
    void Reader::wakeUp() {
        // 设置上下文为互斥锁，记录唤醒原因
        contextSet(CONTEXT::MUTEX, REASON::READER_WAKE_UP);
        {
            // 获取互斥锁并通知所有等待的条件变量
            std::unique_lock<std::mutex> const lck(mtx);
            condBufferFull.notify_all();
            condReaderSleeping.notify_all();
            condParserSleeping.notify_all();
        }
        // 恢复上下文为CPU执行
        contextSet(CONTEXT::CPU);
    }

    /**
     * @brief Reader类的析构函数
     * 
     * 负责释放Reader对象占用的所有资源，包括：
     * - 释放所有读取缓冲区(chunk)
     * - 释放redo日志缓冲区列表
     * - 释放文件头缓冲区
     * - 关闭文件复制描述符
     */
    Reader::~Reader() {
        // 释放所有读取缓冲区(chunk)
        for (uint num = 0; num < ctx->memoryChunksReadBufferMax; ++num)
            bufferFree(this, num);

        // 释放redo日志缓冲区列表
        delete[] redoBufferList;
        redoBufferList = nullptr;

        // 释放文件头缓冲区
        if (headerBuffer != nullptr) {
            free(headerBuffer);
            headerBuffer = nullptr;
        }

        // 关闭文件复制描述符
        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }
    }

    /**
     * @brief 检查Redo Log块头的有效性
     * 
     * 该函数验证Redo Log块头的多个属性，包括块大小、块号、序列号和校验和。
     * 如果任何检查失败，函数将返回相应的错误代码。
     * 
     * @param buffer 指向块头数据的指针
     * @param blockNumber 预期的块号
     * @param showHint 是否显示错误提示
     * @return REDO_CODE 检查结果代码
     *   - REDO_CODE::OK: 所有检查通过
     *   - REDO_CODE::EMPTY: 空块
     *   - REDO_CODE::ERROR_BAD_DATA: 块大小无效
     *   - REDO_CODE::ERROR_SEQUENCE: 序列号不匹配
     *   - REDO_CODE::ERROR_BLOCK: 块号不匹配
     *   - REDO_CODE::ERROR_CRC: 校验和错误
     *   - REDO_CODE::OVERWRITTEN: 块被覆盖
     */
    Reader::REDO_CODE Reader::checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint) {
        // 检查是否为空块(前两个字节都为0)
        if (buffer[0] == 0 && buffer[1] == 0)
            return REDO_CODE::EMPTY;

        // 验证块大小标记是否正确
        // 对于512和1024字节的块，标记应为0x22
        // 对于4096字节的块，标记应为0x82
        if ((blockSize == 512 && buffer[1] != 0x22) ||
            (blockSize == 1024 && buffer[1] != 0x22) ||
            (blockSize == 4096 && buffer[1] != 0x82)) {
            ctx->error(40001, "file: " + fileName + " - block: " + std::to_string(blockNumber) + " - invalid block size: " +
                              std::to_string(blockSize) + ", header[1]: " + std::to_string(static_cast<uint>(buffer[1])));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        // 从块头中读取块号和序列号
        const typeBlk blockNumberHeader = ctx->read32(buffer + 4);
        const Seq sequenceHeader = Seq(ctx->read32(buffer + 8));

        // 检查序列号是否匹配
        // 如果是第一个块(sequence未初始化)或状态为UPDATE，则更新序列号
        // 否则根据group值进行不同的序列号验证
        if (sequence == Seq::zero() || status == STATUS::UPDATE) {
            sequence = sequenceHeader;
        } else {
            if (group == 0) {
                // 对于归档日志(group=0)，序列号必须完全匹配
                if (sequence != sequenceHeader) {
                    ctx->warning(60024, "file: " + fileName + " - invalid header sequence, found: " + sequenceHeader.toString() +
                                        ", expected: " + sequence.toString());
                    return REDO_CODE::ERROR_SEQUENCE;
                }
            } else {
                // 对于在线日志(group!=0)
                // 如果序列号大于块头中的序列号，表示块为空
                // 如果序列号小于块头中的序列号，表示块已被覆盖
                if (sequence > sequenceHeader)
                    return REDO_CODE::EMPTY;
                if (sequence < sequenceHeader)
                    return REDO_CODE::OVERWRITTEN;
            }
        }

        // 验证块号是否匹配
        if (blockNumberHeader != blockNumber) {
            ctx->error(40002, "file: " + fileName + " - invalid header block number: " + std::to_string(blockNumberHeader) +
                              ", expected: " + std::to_string(blockNumber));
            return REDO_CODE::ERROR_BLOCK;
        }

        // 如果未禁用块校验和检查，则验证校验和
        if (!ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::BLOCK_SUM)) {
            // 从块头读取存储的校验和
            const typeSum chSum = ctx->read16(buffer + 14);
            // 计算实际的校验和
            const typeSum chSumCalculated = calcChSum(buffer, blockSize);
            // 比较两者是否一致
            if (chSum != chSumCalculated) {
                if (showHint) {
                    ctx->warning(60025, "file: " + fileName + " - block: " + std::to_string(blockNumber) +
                                        " - invalid header checksum, expected: " + std::to_string(chSum) + ", calculated: " +
                                        std::to_string(chSumCalculated));
                    // 如果尚未显示提示信息，则显示配置建议
                    if (!hintDisplayed) {
                        if (!configuredBlockSum) {
                            ctx->hint("set DB_BLOCK_CHECKSUM = TYPICAL on the database or turn off consistency checking in OpenLogReplicator"
                                      " setting parameter disable-checks: " + std::to_string(static_cast<int>(Ctx::DISABLE_CHECKS::BLOCK_SUM)) +
                                      " for the reader");
                        }
                        hintDisplayed = true;
                    }
                }
                return REDO_CODE::ERROR_CRC;
            }
        }

        return REDO_CODE::OK;
    }

    uint Reader::readSize(uint prevRead) {
        if (prevRead < blockSize)
            return blockSize;

        prevRead *= 2;
        prevRead = std::min<uint64_t>(prevRead, Ctx::MEMORY_CHUNK_SIZE);

        return prevRead;
    }

    /**
     * @brief 读取并验证重做日志文件头部
     * 
     * 该函数负责从重做日志文件中读取头部信息并进行基本验证，
     * 包括检查文件头标识、字节序、块大小等关键信息。
     * 如果启用了日志复制功能，还会将读取的头部信息写入复制文件。
     * 
     * Oracle重做日志文件头部结构说明:
     * - Block 0 (文件头块):
     *   - headerBuffer[0]: 文件标识 (正常为0)
     *   - headerBuffer[1]: 文件类型标识 (0x22表示重做日志)
     *   - 偏移20处4字节: 块大小 (512/1024/4096字节)
     *   - 偏移28-31处4字节: 字节序标识 (0x7A7B7C7D/0x7D7C7B7A)
     * - Block 1 (重做头块):
     *   - 包含数据库版本、SCN、序列号等元数据
     * 
     * @return REDO_CODE::OK 如果头部读取和验证成功
     * @return REDO_CODE::ERROR 如果发生错误
     * @return REDO_CODE::ERROR_READ 如果读取失败
     * @return REDO_CODE::ERROR_BAD_DATA 如果数据验证失败
     * @return REDO_CODE::ERROR_WRITE 如果写入复制文件失败
     */
    Reader::REDO_CODE Reader::reloadHeaderRead() {
        // 检查是否触发软关闭，如果是则返回错误
        if (ctx->softShutdown)
            return REDO_CODE::ERROR;

        // 读取文件头部数据，如果已知块大小则读取两个块的数据，否则读取默认最大页面大小的两倍
        int actualRead = redoRead(headerBuffer, 0, blockSize > 0 ? blockSize * 2 : PAGE_SIZE_MAX * 2);
        if (actualRead < Ctx::MIN_BLOCK_SIZE) {
            ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
            return REDO_CODE::ERROR_READ;
        }
        // 如果配置了指标收集，则记录读取的字节数
        if (ctx->metrics != nullptr)
            ctx->metrics->emitBytesRead(actualRead);

        // 检查文件头部标识，正常情况下header[0]应该为0
        if (headerBuffer[0] != 0) {
            ctx->error(40003, "file: " + fileName + " - invalid header[0]: " + std::to_string(static_cast<uint>(headerBuffer[0])));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        // 检查字节序标识，判断是否需要设置大端序
        if (headerBuffer[28] == 0x7A && headerBuffer[29] == 0x7B && headerBuffer[30] == 0x7C && headerBuffer[31] == 0x7D) {
            // 如果当前不是大端序，则设置为大端序
            if (!ctx->isBigEndian())
                ctx->setBigEndian();
        } else if (headerBuffer[28] != 0x7D || headerBuffer[29] != 0x7C || headerBuffer[30] != 0x7B || headerBuffer[31] != 0x7A || ctx->isBigEndian()) {
            // 如果字节序标识不匹配，报告错误
            ctx->error(40004, "file: " + fileName + " - invalid header[28-31]: " + std::to_string(static_cast<uint>(headerBuffer[28])) +
                              ", " + std::to_string(static_cast<uint>(headerBuffer[29])) + ", " + std::to_string(static_cast<uint>(headerBuffer[30])) +
                              ", " + std::to_string(static_cast<uint>(headerBuffer[31])));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        // 验证块大小是否有效
        // headerBuffer[1]是文件类型标识，0x22表示重做日志文件，0x82也表示重做日志文件
        // 当headerBuffer[1]为0x22时，块大小应为512或1024字节
        // 当headerBuffer[1]为0x82时，块大小应为4096字节
        bool blockSizeOK = false;
        blockSize = ctx->read32(headerBuffer + 20);
        if ((blockSize == 512 && headerBuffer[1] == 0x22) || (blockSize == 1024 && headerBuffer[1] == 0x22) || (blockSize == 4096 && headerBuffer[1] == 0x82))
            blockSizeOK = true;

        if (!blockSizeOK) {
            ctx->error(40005, "file: " + fileName + " - invalid block size: " + std::to_string(blockSize) + ", header[1]: " +
                              std::to_string(static_cast<uint>(headerBuffer[1])));
            blockSize = 0;
            return REDO_CODE::ERROR_BAD_DATA;
        }

        // 确保实际读取的字节数不少于两个块的大小
        if (actualRead < static_cast<int>(blockSize * 2)) {
            ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
            return REDO_CODE::ERROR_READ;
        }

        // 如果配置了重做日志复制路径，则将头部信息写入复制文件
        // ctx->redoCopyPath是一个字符串，存储重做日志复制的目标路径
        // empty()方法用于检查该路径字符串是否为空
        // ctx->redoCopyPath 字段总结
        // 作用： 调试参数，用于将处理过的 redo log 文件内容复制到指定目录，便于诊断磁盘读取问题和事后分析 reference-manual.adoc:555-562
        //
        // 设置方法： 在 JSON 配置文件的 reader 部分添加 "redo-copy-path" 字段 OpenLogReplicator.cpp:760-761
        //
        // 变量定义： 在 Ctx 类中定义为字符串类型，最大长度 2048 字符 Ctx.h:178
        if (!ctx->redoCopyPath.empty()) {
            // 确保写入的字节数不超过两个块的大小
            if (static_cast<uint>(actualRead) > blockSize * 2)
                actualRead = static_cast<int>(blockSize * 2);

            // 获取头部中的序列号
            const Seq sequenceHeader = Seq(ctx->read32(headerBuffer + blockSize + 8));
            // 如果序列号发生变化，关闭当前的复制文件描述符
            if (fileCopySequence != sequenceHeader) {
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }
            }

            // 如果没有打开复制文件，则创建新的复制文件
            if (fileCopyDes == -1) {
                fileNameWrite = ctx->redoCopyPath + "/" + database + "_" + sequenceHeader.toString() + ".arc";
                fileCopyDes = open(fileNameWrite.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);
                if (unlikely(fileCopyDes == -1))
                    throw RuntimeException(10006, "file: " + fileNameWrite + " - open for writing returned: " + strerror(errno));
                ctx->info(0, "writing redo log copy to: " + fileNameWrite);
                fileCopySequence = sequenceHeader;
            }

            // 将头部数据写入复制文件
            const int bytesWritten = pwrite(fileCopyDes, headerBuffer, actualRead, 0);
            if (bytesWritten != actualRead) {
                ctx->error(10007, "file: " + fileNameWrite + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                                  std::to_string(actualRead) + ", code returned: " + strerror(errno));
                return REDO_CODE::ERROR_WRITE;
            }
        }

        return REDO_CODE::OK;
    }

    /**
     * @brief 解析并验证重做日志文件头部信息
     * 
     * 该函数首先调用reloadHeaderRead()读取文件头部，
     * 然后解析头部中的各种元数据信息，如数据库版本、SCN等，
     * 并进行一致性验证。
     * 
     * @return REDO_CODE::OK 如果头部解析和验证成功
     * @return REDO_CODE::EMPTY 如果文件为空
     * @return REDO_CODE::ERROR 如果发生错误
     * @return REDO_CODE::ERROR_BAD_DATA 如果数据验证失败
     *
     *
     * Block 1 Format:
     * - Bytes 0-3: Block number (should be 1)
     * - Bytes 4-7: Block size (should match headerBuffer[1] value)
     * - Bytes 8-11: First change number (SCN) of the redo log
     * - Bytes 12-15: Next change number (SCN) of the redo log
     * - Bytes 16-19: Sequence number of the redo log
     * - Bytes 20-23: Database compatibility version
     * - Bytes 24-27: Activation number
     * - Bytes 28-35: Database SID (8 characters)
     * - Bytes 36-39: Resetlogs SCN
     * - Bytes 40-43: Resetlogs time
     * - Bytes 44-47: Previous resetlogs SCN
     * - Bytes 48-51: Previous resetlogs time
     * - Bytes 52-55: Activation number (again)
     * - Bytes 56-59: First time (timestamp)
     * - Bytes 60-63: Next time (timestamp)
     * - Bytes 64-67: Number of blocks in header
     * - Bytes 68-71: Archive log sequence number
     * - Bytes 72-75: Archive log sequence number (again)
     * - Bytes 76-79: Archive log sequence number (again)
     * - Bytes 80-83: Archive log sequence number (again)
     */
    Reader::REDO_CODE Reader::reloadHeader() {
        // 首先读取并验证文件头部
        REDO_CODE retReload = reloadHeaderRead();
        if (retReload != REDO_CODE::OK)
            return retReload;

        // 解析数据库兼容版本
        uint32_t version;
        compatVsn = ctx->read32(headerBuffer + blockSize + 20);
        if (compatVsn == 0)
            return REDO_CODE::EMPTY;

        // 验证数据库版本是否支持
        if ((compatVsn >= 0x0B200000 && compatVsn <= 0x0B200400)        // 11.2.0.0 - 11.2.0.4
            || (compatVsn >= 0x0C100000 && compatVsn <= 0x0C100200)     // 12.1.0.0 - 12.1.0.2
            || (compatVsn >= 0x0C200000 && compatVsn <= 0x0C200100)     // 12.2.0.0 - 12.2.0.1
            || (compatVsn >= 0x12000000 && compatVsn <= 0x120E0000)     // 18.0.0.0 - 18.14.0.0
            || (compatVsn >= 0x13000000 && compatVsn <= 0x13120000)     // 19.0.0.0 - 19.18.0.0
            || (compatVsn >= 0x15000000 && compatVsn <= 0x15080000)     // 21.0.0.0 - 21.8.0.0
            || (compatVsn >= 0x17000000 && compatVsn <= 0x17030000))    // 21.0.0.0 - 21.3.0.0
            version = compatVsn;
        else {
            ctx->error(40006, "file: " + fileName + " - invalid database version: " + std::to_string(compatVsn));
            return REDO_CODE::ERROR_BAD_DATA;
        }
        // 解析头部中的各种元数据
        activation = ctx->read32(headerBuffer + blockSize + 52);
        numBlocksHeader = ctx->read32(headerBuffer + blockSize + 156);
        resetlogs = ctx->read32(headerBuffer + blockSize + 160);
        firstScnHeader = ctx->readScn(headerBuffer + blockSize + 180);
        firstTimeHeader = ctx->read32(headerBuffer + blockSize + 188);
        nextScnHeader = ctx->readScn(headerBuffer + blockSize + 192);
        nextTime = ctx->read32(headerBuffer + blockSize + 200);

        // 如果是归档日志且文件大小超过头部指定的块数，则更新文件大小
        if (numBlocksHeader != Ctx::ZERO_BLK && fileSize > static_cast<uint64_t>(numBlocksHeader) * blockSize && group == 0) {
            fileSize = static_cast<uint64_t>(numBlocksHeader) * blockSize;
            ctx->info(0, "updating redo log size to: " + std::to_string(fileSize) + " for: " + fileName);
        }

        // 如果是第一次读取文件头部，初始化上下文中的版本信息
        if (ctx->version == 0) {
            char SID[9];
            memcpy(reinterpret_cast<void*>(SID),
                   reinterpret_cast<const void*>(headerBuffer + blockSize + 28), 8);
            SID[8] = 0;
            ctx->version = version;
            // 如果是23c及以上版本，设置列数限制
            if (compatVsn >= RedoLogRecord::REDO_VERSION_23_0)
                ctx->columnLimit = Ctx::COLUMN_LIMIT_23_0;
            const Seq sequenceHeader = Seq(ctx->read32(headerBuffer + blockSize + 8));

            // 根据版本号生成版本字符串
            if (compatVsn < RedoLogRecord::REDO_VERSION_18_0) {
                ctx->versionStr = std::to_string(compatVsn >> 24) + "." + std::to_string((compatVsn >> 20) & 0xF) + "." +
                                  std::to_string((compatVsn >> 16) & 0xF) + "." + std::to_string((compatVsn >> 8) & 0xFF);
            } else {
                ctx->versionStr = std::to_string(compatVsn >> 24) + "." + std::to_string((compatVsn >> 16) & 0xFF) + "." +
                                  std::to_string((compatVsn >> 8) & 0xFF);
            }
            ctx->info(0, "found redo log version: " + ctx->versionStr + ", activation: " + std::to_string(activation) + ", resetlogs: " +
                         std::to_string(resetlogs) + ", page: " + std::to_string(blockSize) + ", sequence: " + sequenceHeader.toString() +
                         ", SID: " + SID + ", endian: " + (ctx->isBigEndian() ? "BIG" : "LITTLE"));
        }

        // 验证数据库版本是否与上下文中的版本一致
        if (version != ctx->version) {
            ctx->error(40007, "file: " + fileName + " - invalid database version: " + std::to_string(compatVsn) + ", expected: " +
                              std::to_string(ctx->version));
            return REDO_CODE::ERROR_BAD_DATA;
        }

        // 检查第一个块的CRC校验，如果失败则重试
        uint badBlockCrcCount = 0;
        retReload = checkBlockHeader(headerBuffer + blockSize, 1, false);
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
            ctx->logTrace(Ctx::TRACE::DISK, "block: 1 check: " + std::to_string(static_cast<uint>(retReload)));

        while (retReload == REDO_CODE::ERROR_CRC) {
            ++badBlockCrcCount;
            // 如果CRC错误次数超过最大限制，则返回错误
            if (badBlockCrcCount == BAD_CDC_MAX_CNT)
                return REDO_CODE::ERROR_BAD_DATA;

            // 睡眠一段时间后重试
            contextSet(CONTEXT::SLEEP);
            usleep(ctx->redoReadSleepUs);
            contextSet(CONTEXT::CPU);
            retReload = checkBlockHeader(headerBuffer + blockSize, 1, false);
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "block: 1 check: " + std::to_string(static_cast<uint>(retReload)));
        }

        if (retReload != REDO_CODE::OK)
            return retReload;

        // 更新SCN信息
        if (firstScn == Scn::none() || status == STATUS::UPDATE) {
            firstScn = firstScnHeader;
            nextScn = nextScnHeader;
        } else {
            // 验证first SCN是否一致
            if (firstScnHeader != firstScn) {
                ctx->error(40008, "file: " + fileName + " - invalid first scn value: " + firstScnHeader.toString() + ", expected: " + firstScn.toString());
                return REDO_CODE::ERROR_BAD_DATA;
            }
        }

        // 更新next SCN信息
        if (nextScn == Scn::none() && nextScnHeader != Scn::none()) {
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "updating next scn to: " + nextScnHeader.toString());
            nextScn = nextScnHeader;
        } else if (nextScn != Scn::none() && nextScnHeader != Scn::none() && nextScn != nextScnHeader) {
            ctx->error(40009, "file: " + fileName + " - invalid next scn value: " + nextScnHeader.toString() + ", expected: " +
                              nextScn.toString());
            return REDO_CODE::ERROR_BAD_DATA;
        }

        return retReload;
    }

    /**
     * @brief 从Redo日志文件中读取数据到缓冲区(read1)
     * 
     * 此函数负责从Redo日志文件中读取数据到内存缓冲区。它计算需要读取的数据量，
     * 处理边界情况（如文件末尾），分配必要的缓冲区内存，并执行实际的读取操作。
     * 
     * @return bool 读取成功返回true，失败返回false
     */
    bool Reader::read1() {
        // 计算本次需要读取的数据量
        uint toRead = readSize(lastRead);

        // 如果读取会超出文件大小，则调整读取量为文件剩余大小
        if (bufferScan + toRead > fileSize)
            toRead = fileSize - bufferScan;

        // 计算缓冲区中的位置和缓冲区编号
        const uint64_t redoBufferPos = bufferScan % Ctx::MEMORY_CHUNK_SIZE;
        const uint64_t redoBufferNum = (bufferScan / Ctx::MEMORY_CHUNK_SIZE) % ctx->memoryChunksReadBufferMax;
        
        // 如果读取会超出当前内存块边界，则调整读取量
        if (redoBufferPos + toRead > Ctx::MEMORY_CHUNK_SIZE)
            toRead = Ctx::MEMORY_CHUNK_SIZE - redoBufferPos;

        // 检查读取量是否为0，如果为0则报错返回
        if (toRead == 0) {
            ctx->error(40010, "file: " + fileName + " - zero to read, start: " + std::to_string(bufferStart) + ", end: " +
                              std::to_string(bufferEnd) + ", scan: " + std::to_string(bufferScan));
            ret = REDO_CODE::ERROR;
            return false;
        }

        // 分配读取缓冲区
        bufferAllocate(redoBufferNum);
        // 如果启用了磁盘跟踪，则记录读取信息
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
            ctx->logTrace(Ctx::TRACE::DISK, "reading#1 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                                            std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") bytes: " + std::to_string(toRead));
        // 执行实际的读取操作
        const int actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferScan, toRead);

        // 如果启用了磁盘跟踪，则记录读取结果
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
            ctx->logTrace(Ctx::TRACE::DISK, "reading#1 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                                            std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") got: " + std::to_string(actualRead));
        // 检查读取是否出错
        if (actualRead < 0) {
            ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
            ret = REDO_CODE::ERROR_READ;
            return false;
        }
        // 更新读取字节数的指标
        if (ctx->metrics != nullptr)
            ctx->metrics->emitBytesRead(actualRead);

        // 如果实际读取了数据且文件描述符有效，并且不需要延迟验证或为第一组日志
        if (actualRead > 0 && fileCopyDes != -1 && (ctx->redoVerifyDelayUs == 0 || group == 0)) {
            // 将读取的数据写入到复制文件中，从bufferEnd位置开始写入
            const int bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead,
                                            static_cast<int64_t>(bufferEnd));
            // 检查写入的字节数是否与实际读取的字节数一致
            if (bytesWritten != actualRead) {
                // 如果不一致，则记录错误信息并返回写入错误
                ctx->error(10007, "file: " + fileNameWrite + " - " + std::to_string(bytesWritten) + " bytes written instead of " +
                                  std::to_string(actualRead) + ", code returned: " + strerror(errno));
                ret = REDO_CODE::ERROR_WRITE;
                return false;
            }
        }

        // 计算实际读取的数据块数量
        const typeBlk maxNumBlock = actualRead / blockSize;
        // 计算当前扫描位置对应的块号
        const typeBlk bufferScanBlock = bufferScan / blockSize;
        // 初始化有效数据块计数器
        uint goodBlocks = 0;
        // 初始化返回码为OK
        REDO_CODE currentRet = REDO_CODE::OK;

        // 检查每个数据块的有效性
        for (typeBlk numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
            // 调用checkBlockHeader检查数据块头部信息，验证数据块是否有效
            currentRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + (numBlock * blockSize), bufferScanBlock + numBlock,
                                          ctx->redoVerifyDelayUs == 0 || group == 0);
            // 如果启用了磁盘跟踪日志，则记录当前块的检查结果
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "block: " + std::to_string(bufferScanBlock + numBlock) + " check: " +
                                                std::to_string(static_cast<uint>(currentRet)));

            // 如果当前数据块检查失败，则跳出循环
            if (currentRet != REDO_CODE::OK)
                break;
            // 增加有效数据块计数
            ++goodBlocks;
        }

        // 处理部分在线重做日志文件的情况
        // 当没有有效数据块且为第一组日志时
        if (goodBlocks == 0 && group == 0) {
            // 如果下一个SCN头不为空，表示已到达文件尾部，标记为完成
            if (nextScnHeader != Scn::none()) {
                ret = REDO_CODE::FINISHED;
                nextScn = nextScnHeader;
            } else {
                // 否则记录警告信息并停止读取
                ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) + " - unexpected end of file");
                ret = REDO_CODE::STOPPED;
            }
            return false;
        }

        // 将CRC错误的坏块视为空块处理
        // 当存在CRC错误、需要延迟验证且不为第一组日志时
        if (currentRet == REDO_CODE::ERROR_CRC && ctx->redoVerifyDelayUs > 0 && group != 0)
            currentRet = REDO_CODE::EMPTY;

        // 如果没有有效数据块且返回码不是OK或EMPTY，则返回当前错误码
        if (goodBlocks == 0 && currentRet != REDO_CODE::OK && currentRet != REDO_CODE::EMPTY) {
            ret = currentRet;
            return false;
        }

        // 检查是否发生日志切换
        // 当没有有效数据块且当前返回码为空时，重新加载头部信息
        if (goodBlocks == 0 && currentRet == REDO_CODE::EMPTY) {
            currentRet = reloadHeader();
            if (currentRet != REDO_CODE::OK) {
                ret = currentRet;
                return false;
            }
            // 标记已到达零点
            reachedZero = true;
        } else {
            // 标记已读取数据块
            readBlocks = true;
            // 未到达零点
            reachedZero = false;
        }

        // 记录本次读取的字节数和时间戳
        lastRead = goodBlocks * blockSize;
        lastReadTime = ctx->clock->getTimeUt();
        // 如果有有效数据块
        if (goodBlocks > 0) {
            // 如果需要延迟验证且不是第一组日志
            if (ctx->redoVerifyDelayUs > 0 && group != 0) {
                // 更新扫描位置
                bufferScan += goodBlocks * blockSize;

                // 为每个数据块记录读取时间
                for (uint numBlock = 0; numBlock < goodBlocks; ++numBlock) {
                    auto* readTimeP = reinterpret_cast<time_t*>(redoBufferList[redoBufferNum] + redoBufferPos + (numBlock * blockSize));
                    *readTimeP = lastReadTime;
                }
            } else {
                // 不需要延迟验证的情况
                {
                    // 设置上下文为互斥锁状态
                    contextSet(CONTEXT::MUTEX, REASON::READER_READ1);
                    // 获取互斥锁
                    std::unique_lock<std::mutex> const lck(mtx);
                    // 更新缓冲区结束位置和扫描位置
                    bufferEnd += goodBlocks * blockSize;
                    bufferScan = bufferEnd;
                    // 通知解析器线程有新数据
                    condParserSleeping.notify_all();
                }
                // 恢复CPU上下文
                contextSet(CONTEXT::CPU);
            }
        }

        // 批量模式下处理部分在线重做日志文件的序列错误
        // 当返回错误代码为序列错误且为第一组日志时
        if (currentRet == REDO_CODE::ERROR_SEQUENCE && group == 0) {
            // 如果下一个SCN头不为空，表示已到达文件尾部，标记为完成
            if (nextScnHeader != Scn::none()) {
                ret = REDO_CODE::FINISHED;
                nextScn = nextScnHeader;
            } else {
                // 否则记录警告信息并停止读取
                ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) + " - unexpected end of file");
                ret = REDO_CODE::STOPPED;
            }
            return false;
        }

        return true;
    }

    bool Reader::read2() {
        uint maxNumBlock = (bufferScan - bufferEnd) / blockSize;
        uint goodBlocks = 0;
        maxNumBlock = std::min<uint64_t>(maxNumBlock, Ctx::MEMORY_CHUNK_SIZE / blockSize);

        for (uint numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
            const uint64_t redoBufferPos = (bufferEnd + numBlock * blockSize) % Ctx::MEMORY_CHUNK_SIZE;
            const uint64_t redoBufferNum = ((bufferEnd + numBlock * blockSize) / Ctx::MEMORY_CHUNK_SIZE) % ctx->memoryChunksReadBufferMax;

            const auto* const readTimeP = reinterpret_cast<const time_ut*>(redoBufferList[redoBufferNum] + redoBufferPos);
            if (*readTimeP + static_cast<time_ut>(ctx->redoVerifyDelayUs) < loopTime) {
                ++goodBlocks;
            } else {
                readTime = *readTimeP + static_cast<time_t>(ctx->redoVerifyDelayUs);
                break;
            }
        }

        if (goodBlocks > 0) {
            uint toRead = readSize(goodBlocks * blockSize);
            toRead = std::min(toRead, goodBlocks * blockSize);

            const uint64_t redoBufferPos = bufferEnd % Ctx::MEMORY_CHUNK_SIZE;
            const uint64_t redoBufferNum = (bufferEnd / Ctx::MEMORY_CHUNK_SIZE) % ctx->memoryChunksReadBufferMax;

            if (redoBufferPos + toRead > Ctx::MEMORY_CHUNK_SIZE)
                toRead = Ctx::MEMORY_CHUNK_SIZE - redoBufferPos;

            if (toRead == 0) {
                ctx->error(40011, "zero to read (start: " + std::to_string(bufferStart) + ", end: " + std::to_string(bufferEnd) +
                                  ", scan: " + std::to_string(bufferScan) + "): " + fileName);
                ret = REDO_CODE::ERROR;
                return false;
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "reading#2 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                                                std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") bytes: " + std::to_string(toRead));
            const int actualRead = redoRead(redoBufferList[redoBufferNum] + redoBufferPos, bufferEnd, toRead);

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                ctx->logTrace(Ctx::TRACE::DISK, "reading#2 " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                                                std::to_string(bufferEnd) + "/" + std::to_string(bufferScan) + ") got: " + std::to_string(actualRead));

            if (actualRead < 0) {
                ctx->error(40003, "file: " + fileName + " - " + strerror(errno));
                ret = REDO_CODE::ERROR_READ;
                return false;
            }
            if (ctx->metrics != nullptr)
                ctx->metrics->emitBytesRead(actualRead);

            if (actualRead > 0 && fileCopyDes != -1) {
                const int bytesWritten = pwrite(fileCopyDes, redoBufferList[redoBufferNum] + redoBufferPos, actualRead,
                                                static_cast<int64_t>(bufferEnd));
                if (bytesWritten != actualRead) {
                    ctx->error(10007, "file: " + fileNameWrite + " - " + std::to_string(bytesWritten) +
                                      " bytes written instead of " + std::to_string(actualRead) + ", code returned: " + strerror(errno));
                    ret = REDO_CODE::ERROR_WRITE;
                    return false;
                }
            }

            readBlocks = true;
            REDO_CODE currentRet = REDO_CODE::OK;
            maxNumBlock = actualRead / blockSize;
            const typeBlk bufferEndBlock = bufferEnd / blockSize;

            // Check which blocks are good
            for (uint numBlock = 0; numBlock < maxNumBlock; ++numBlock) {
                currentRet = checkBlockHeader(redoBufferList[redoBufferNum] + redoBufferPos + (numBlock * blockSize),
                                              bufferEndBlock + numBlock, true);
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                    ctx->logTrace(Ctx::TRACE::DISK, "block: " + std::to_string(bufferEndBlock + numBlock) + " check: " +
                                                    std::to_string(static_cast<uint>(currentRet)));

                if (currentRet != REDO_CODE::OK)
                    break;
                ++goodBlocks;
            }

            // Verify header for online redo logs after every successful read
            if (currentRet == REDO_CODE::OK && group > 0)
                currentRet = reloadHeader();

            if (currentRet != REDO_CODE::OK) {
                ret = currentRet;
                return false;
            }

            {
                contextSet(CONTEXT::MUTEX, REASON::READER_READ2);
                std::unique_lock<std::mutex> const lck(mtx);
                bufferEnd += actualRead;
                condParserSleeping.notify_all();
            }
            contextSet(CONTEXT::CPU);
        }

        return true;
    }

    void Reader::mainLoop() {
        while (!ctx->softShutdown) {
            {
                // 设置上下文为互斥锁状态，表示进入读取器主循环的关键部分
                contextSet(CONTEXT::MUTEX, REASON::READER_MAIN1);
                // 获取互斥锁，用于保护共享资源
                std::unique_lock<std::mutex> lck(mtx);
                // 通知解析器线程有新的数据可以处理
                condParserSleeping.notify_all();

                // 如果读取器状态为休眠且没有软关闭请求
                if (status == STATUS::SLEEPING && !ctx->softShutdown) {
                    // 如果启用了睡眠跟踪，则记录日志
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                        ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:mainLoop:sleep");
                    // 设置上下文为等待状态，表示读取器没有工作要做
                    contextSet(CONTEXT::WAIT, REASON::READER_NO_WORK);
                    // 等待条件变量，直到被唤醒
                    condReaderSleeping.wait(lck);
                    // 重新设置上下文为互斥锁状态
                    contextSet(CONTEXT::MUTEX, REASON::READER_MAIN2);
                } else if (status == STATUS::READ && !ctx->softShutdown && (bufferEnd % Ctx::MEMORY_CHUNK_SIZE) == 0) {
                    // 如果状态为读取且缓冲区末尾位置是内存块大小的倍数，则发出缓冲区满的警告
                    ctx->warning(0, "buffer full?");
                }
            }
            contextSet(CONTEXT::CPU);

            // 检查是否需要软关闭，如果需要则跳出循环
            if (ctx->softShutdown)
                break;

            // 如果当前状态为检查状态(CHECK)
            if (status == STATUS::CHECK) {
                // 如果启用了文件跟踪，则记录尝试打开文件的日志
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
                    ctx->logTrace(Ctx::TRACE::FILE, "trying to open: " + fileName);
                // 关闭当前文件
                redoClose();
                // 尝试重新打开文件
                const REDO_CODE currentRet = redoOpen();
                // 更新状态并通知等待的解析器线程
                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_STATUS);
                    std::unique_lock<std::mutex> const lck(mtx);
                    ret = currentRet;
                    status = STATUS::SLEEPING;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
                // 继续下一次循环
                continue;
            }

            // 如果当前状态为更新状态(UPDATE)
            if (status == STATUS::UPDATE) {
                // 如果文件复制描述符有效，则关闭文件并重置描述符
                if (fileCopyDes != -1) {
                    close(fileCopyDes);
                    fileCopyDes = -1;
                }

                // 重置读取统计信息
                sumRead = 0;
                sumTime = 0;
                // 重新加载文件头信息
                const REDO_CODE currentRet = reloadHeader();
                // 如果重新加载成功，则重置缓冲区起始和结束位置
                if (currentRet == REDO_CODE::OK) {
                    bufferStart = blockSize * 2;
                    bufferEnd = blockSize * 2;
                }

                // 释放所有读取缓冲区
                // 当文件状态更新时，需要释放旧的缓冲区以便重新读取文件内容
                for (uint num = 0; num < ctx->memoryChunksReadBufferMax; ++num)
                    bufferFree(this, num);

                // 更新状态并通知等待的解析器线程
                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_SLEEP1);
                    std::unique_lock<std::mutex> const lck(mtx);
                    ret = currentRet;
                    status = STATUS::SLEEPING;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
            } else if (status == STATUS::READ) {
                // 如果启用了磁盘跟踪，则记录当前读取位置和文件大小信息
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::DISK)))
                    ctx->logTrace(Ctx::TRACE::DISK, "reading " + fileName + " at (" + std::to_string(bufferStart) + "/" +
                                                    std::to_string(bufferEnd) + ") at size: " + std::to_string(fileSize));
                // 初始化上次读取大小和时间相关变量
                lastRead = blockSize;
                lastReadTime = 0;
                readTime = 0;
                // 设置扫描位置为当前缓冲区末尾
                bufferScan = bufferEnd;
                // 重置零块标记
                reachedZero = false;

                // 在READ状态下持续读取文件，直到满足退出条件
                while (!ctx->softShutdown && status == STATUS::READ) {
                    // 获取当前时间，用于计算读取间隔
                    loopTime = ctx->clock->getTimeUt();
                    // 重置读取块标记和读取时间
                    readBlocks = false;
                    readTime = 0;

                    // 检查是否已读取到文件末尾
                    if (bufferEnd == fileSize) {
                        // 如果已读取到文件末尾，根据nextScnHeader的值决定返回状态
                        if (nextScnHeader != Scn::none()) {
                            // 如果nextScnHeader有效，则标记为已完成
                            ret = REDO_CODE::FINISHED;
                            nextScn = nextScnHeader;
                        } else {
                            // 如果nextScnHeader无效，则发出警告并标记为已停止
                            ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) +
                                                " - unexpected end of file");
                            ret = REDO_CODE::STOPPED;
                        }
                        // 退出读取循环
                        break;
                    }

                    // 检查缓冲区是否已满
                    if (bufferStart + ctx->bufferSizeMax == bufferEnd) {
                        // 设置上下文为互斥锁状态，表示需要等待缓冲区空间
                        contextSet(CONTEXT::MUTEX, REASON::READER_FULL);
                        // 获取互斥锁
                        std::unique_lock<std::mutex> lck(mtx);
                        // 再次检查缓冲区是否已满（防止在获取锁期间状态发生变化）
                        if (!ctx->softShutdown && bufferStart + ctx->bufferSizeMax == bufferEnd) {
                            // 如果启用了睡眠跟踪，则记录日志
                            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                                ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:mainLoop:bufferFull");
                            // 设置上下文为等待状态
                            contextSet(CONTEXT::WAIT, REASON::READER_BUFFER_FULL);
                            // 等待缓冲区有空间的条件变量
                            condBufferFull.wait(lck);
                            // 恢复上下文为CPU状态
                            contextSet(CONTEXT::CPU);
                            // 继续下一次循环
                            continue;
                        }
                    }

                    // 检查缓冲区是否需要更多数据
                    // 当已扫描位置超过有效数据结束位置时，调用read2()读取更多数据
                    // 如果read2()失败则跳出循环
                    if (bufferEnd < bufferScan)
                        if (!read2())
                            break;

                    // #1 read
                    // 主要读取逻辑：检查是否需要读取更多数据
                    // 条件包括：
                    // 1. 扫描位置小于文件大小
                    // 2. 缓冲区有空闲空间或扫描位置未对齐到内存块边界
                    // 3. 未达到零块或距离上次读取时间已超过设定的休眠时间
                    // 如果read1()失败则跳出循环
                    if (bufferScan < fileSize && (bufferIsFree() || (bufferScan % Ctx::MEMORY_CHUNK_SIZE) > 0)
                        && (!reachedZero || lastReadTime + static_cast<time_t>(ctx->redoReadSleepUs) < loopTime))
                        if (!read1())
                            break;

                    // 检查是否已到达文件头中指定的块数位置
                    // 如果已到达且有下一个SCN，则标记为完成；否则记录警告并停止读取
                    if (numBlocksHeader != Ctx::ZERO_BLK && bufferEnd == static_cast<uint64_t>(numBlocksHeader) * blockSize) {
                        if (nextScnHeader != Scn::none()) {
                            ret = REDO_CODE::FINISHED;
                            nextScn = nextScnHeader;
                        } else {
                            ctx->warning(60023, "file: " + fileName + " - position: " + std::to_string(bufferScan) +
                                                " - unexpected end of file");
                            ret = REDO_CODE::STOPPED;
                        }
                        break;
                    }

                    // 如果没有读取到数据块，则根据设定的休眠时间进行休眠
                    // 如果readTime为0，直接休眠redoReadSleepUs微秒
                    // 否则，根据当前时间和readTime计算休眠时间
                    if (!readBlocks) {
                        if (readTime == 0) {
                            contextSet(CONTEXT::SLEEP);
                            usleep(ctx->redoReadSleepUs);
                            contextSet(CONTEXT::CPU);
                        } else {
                            const time_ut nowTime = ctx->clock->getTimeUt();
                            if (readTime > nowTime) {
                                // 如果redoReadSleepUs小于等待时间，则休眠redoReadSleepUs微秒
                                // 否则休眠到readTime时间点
                                if (static_cast<time_ut>(ctx->redoReadSleepUs) < readTime - nowTime) {
                                    contextSet(CONTEXT::SLEEP);
                                    usleep(ctx->redoReadSleepUs);
                                    contextSet(CONTEXT::CPU);
                                } else {
                                    contextSet(CONTEXT::SLEEP);
                                    usleep(readTime - nowTime);
                                    contextSet(CONTEXT::CPU);
                                }
                            }
                        }
                    }
                }

                {
                    contextSet(CONTEXT::MUTEX, REASON::READER_SLEEP2);
                    std::unique_lock<std::mutex> const lck(mtx);
                    status = STATUS::SLEEPING;
                    condParserSleeping.notify_all();
                }
                contextSet(CONTEXT::CPU);
            }
        }
    }

    typeSum Reader::calcChSum(uint8_t* buffer, uint size) const {
        const typeSum oldChSum = ctx->read16(buffer + 14);
        uint64_t sum = 0;

        for (uint i = 0; i < size / 8; ++i, buffer += sizeof(uint64_t))
            sum ^= *reinterpret_cast<const uint64_t*>(buffer);
        sum ^= (sum >> 32);
        sum ^= (sum >> 16);
        sum ^= oldChSum;

        return sum & 0xFFFF;
    }

    void Reader::run() {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "reader (" + ss.str() + ") start");
        }

        try {
            mainLoop();
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }

        redoClose();
        if (fileCopyDes != -1) {
            close(fileCopyDes);
            fileCopyDes = -1;
        }

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "reader (" + ss.str() + ") stop");
        }
    }

    /**
     * @brief 分配指定编号的读取缓冲区
     * 
     * 此函数用于为读取操作分配指定编号的缓冲区。它首先检查该缓冲区是否已经分配，
     * 如果已分配则直接返回。否则，从内存管理器获取新的内存块，并更新缓冲区列表
     * 和空闲缓冲区计数。
     * 
     * @param num 要分配的缓冲区编号
     */
    void Reader::bufferAllocate(uint num) {
        // 第一次尝试获取互斥锁并检查缓冲区是否已分配
        {
            // 设置上下文为互斥锁状态，表示正在进行缓冲区分配操作
            contextSet(CONTEXT::MUTEX, REASON::READER_ALLOCATE1);
            // 获取互斥锁以安全访问共享资源
            std::unique_lock<std::mutex> const lck(mtx);
            // 检查指定编号的缓冲区是否已经分配，如果已分配则直接返回
            if (redoBufferList[num] != nullptr) {
                contextSet(CONTEXT::CPU);
                return;
            }
        }
        // 释放锁后设置上下文为CPU状态
        contextSet(CONTEXT::CPU);

        // 从上下文的内存管理器获取新的内存块
        uint8_t* buffer = ctx->getMemoryChunk(this, Ctx::MEMORY::READER);

        // 第二次获取互斥锁以更新缓冲区列表
        {
            // 设置上下文为互斥锁状态，表示正在进行缓冲区列表更新
            contextSet(CONTEXT::MUTEX, REASON::READER_ALLOCATE2);
            // 获取互斥锁以安全更新共享资源
            std::unique_lock<std::mutex> const lck(mtx);
            // 将新分配的内存块添加到缓冲区列表中
            redoBufferList[num] = buffer;
            // 减少空闲缓冲区计数
            --ctx->bufferSizeFree;
        }
        // 释放锁后设置上下文为CPU状态
        contextSet(CONTEXT::CPU);
    }

    /**
     * @brief 释放指定编号的读取缓冲区
     * 
     * 此函数用于释放 `redoBufferList` 中指定编号的缓冲区。它首先检查该缓冲区是否为空，
     * 如果为空则直接返回。否则，将缓冲区指针置空并增加空闲缓冲区计数，最后调用
     * `freeMemoryChunk` 释放内存。
     * 
     * @param t 指向当前线程的指针
     * @param num 要释放的缓冲区编号
     */
    void Reader::bufferFree(Thread* t, uint num) {
        uint8_t* buffer;
        {
            // 设置上下文为互斥锁状态，表示正在进行缓冲区释放操作
            t->contextSet(CONTEXT::MUTEX, REASON::READER_FREE);
            std::unique_lock<std::mutex> const lck(mtx);
            // 检查指定编号的缓冲区是否为空，如果为空则直接返回
            if (redoBufferList[num] == nullptr) {
                t->contextSet(CONTEXT::CPU);
                return;
            }
            // 获取缓冲区指针，并将列表中的指针置空
            buffer = redoBufferList[num];
            redoBufferList[num] = nullptr;
            // 增加空闲缓冲区计数
            ++ctx->bufferSizeFree;
        }
        // 设置上下文为CPU状态，表示释放操作完成
        t->contextSet(CONTEXT::CPU);

        // 调用上下文的内存管理函数释放缓冲区内存
        ctx->freeMemoryChunk(this, Ctx::MEMORY::READER, buffer);
    }

    /**
     * @brief 检查读取缓冲区是否有空闲空间
     * 
     * 此函数用于检查当前读取缓冲区是否有空闲空间可用于分配。
     * 它通过检查上下文中的空闲缓冲区计数来判断。
     * 
     * @return bool 如果有空闲缓冲区返回true，否则返回false
     */
    bool Reader::bufferIsFree() {
        bool isFree;
        {
            // 设置上下文为互斥锁状态，表示正在进行缓冲区空闲检查
            contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_FREE);
            // 获取互斥锁以安全访问共享资源
            std::unique_lock<std::mutex> const lck(mtx);
            // 检查空闲缓冲区数量是否大于0
            isFree = (ctx->bufferSizeFree > 0);
        }
        // 恢复上下文为CPU状态
        contextSet(CONTEXT::CPU);
        return isFree;
    }

    void Reader::printHeaderInfo(std::ostringstream& ss, const std::string& path) const {
        char SID[9];
        memcpy(reinterpret_cast<void*>(SID),
               reinterpret_cast<const void*>(headerBuffer + blockSize + 28), 8);
        SID[8] = 0;

        ss << "DUMP OF REDO FROM FILE '" << path << "'\n";
        if (ctx->version >= RedoLogRecord::REDO_VERSION_12_2)
            ss << " Container ID: 0\n Container UID: 0\n";
        ss << " Opcodes *.*\n";
        if (ctx->version >= RedoLogRecord::REDO_VERSION_12_2)
            ss << " Container ID: 0\n Container UID: 0\n";
        ss << " RBAs: 0x000000.00000000.0000 thru 0xffffffff.ffffffff.ffff\n";
        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
            ss << " SCNs: scn: 0x0000.00000000 thru scn: 0xffff.ffffffff\n";
        else
            ss << " SCNs: scn: 0x0000000000000000 thru scn: 0xffffffffffffffff\n";
        ss << " Times: creation thru eternity\n";

        const uint32_t dbid = ctx->read32(headerBuffer + blockSize + 24);
        const uint32_t controlSeq = ctx->read32(headerBuffer + blockSize + 36);
        const uint32_t fileSizeHeader = ctx->read32(headerBuffer + blockSize + 40);
        const uint16_t fileNumber = ctx->read16(headerBuffer + blockSize + 48);

        ss << " FILE HEADER:\n" <<
           "\tCompatibility Vsn = " << std::dec << compatVsn << "=0x" << std::hex << compatVsn << '\n' <<
           "\tDb ID=" << std::dec << dbid << "=0x" << std::hex << dbid << ", Db Name='" << SID << "'\n" <<
           "\tActivation ID=" << std::dec << activation << "=0x" << std::hex << activation << '\n' <<
           "\tControl Seq=" << std::dec << controlSeq << "=0x" << std::hex << controlSeq << ", File size=" << std::dec << fileSizeHeader << "=0x" <<
           std::hex << fileSizeHeader << '\n' <<
           "\tFile Number=" << std::dec << fileNumber << ", Blksiz=" << std::dec << blockSize << ", File Type=2 LOG\n";

        const Seq seq = Seq(ctx->read32(headerBuffer + blockSize + 8));
        uint8_t descrip[65];
        memcpy(reinterpret_cast<void*>(descrip),
               reinterpret_cast<const void*>(headerBuffer + blockSize + 92), 64);
        descrip[64] = 0;
        const uint16_t thread = ctx->read16(headerBuffer + blockSize + 176);
        const uint32_t hws = ctx->read32(headerBuffer + blockSize + 172);
        const uint8_t eot = headerBuffer[blockSize + 204];
        const uint8_t dis = headerBuffer[blockSize + 205];

        ss << R"( descrip:")" << descrip << R"(")" << '\n' <<
           " thread: " << std::dec << thread <<
           " nab: 0x" << std::hex << numBlocksHeader <<
           " seq: " << seq.toStringHex(8) <<
           " hws: 0x" << std::hex << hws <<
           " eot: " << std::dec << static_cast<uint>(eot) <<
           " dis: " << std::dec << static_cast<uint>(dis) << '\n';

        const Scn resetlogsScn = ctx->readScn(headerBuffer + blockSize + 164);
        const typeResetlogs prevResetlogsCnt = ctx->read32(headerBuffer + blockSize + 292);
        const Scn prevResetlogsScn = ctx->readScn(headerBuffer + blockSize + 284);
        const Scn enabledScn = ctx->readScn(headerBuffer + blockSize + 208);
        const Time enabledTime(ctx->read32(headerBuffer + blockSize + 216));
        const Scn threadClosedScn = ctx->readScn(headerBuffer + blockSize + 220);
        const Time threadClosedTime(ctx->read32(headerBuffer + blockSize + 228));
        const Scn termialRecScn = ctx->readScn(headerBuffer + blockSize + 240);
        const Time termialRecTime(ctx->read32(headerBuffer + blockSize + 248));
        const Scn mostRecentScn = ctx->readScn(headerBuffer + blockSize + 260);
        const typeSum chSum = ctx->read16(headerBuffer + blockSize + 14);
        const typeSum chSum2 = calcChSum(headerBuffer + blockSize, blockSize);

        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2) {
            ss << " resetlogs count: 0x" << std::hex << resetlogs << " scn: " << resetlogsScn.to48() <<
               " (" << resetlogsScn.toString() << ")\n" <<
               " prev resetlogs count: 0x" << std::hex << prevResetlogsCnt << " scn: " << prevResetlogsScn.to48() <<
               " (" << prevResetlogsScn.toString() << ")\n" <<
               " Low  scn: " << firstScnHeader.to48() << " (" << firstScnHeader.toString() << ")" << " " << firstTimeHeader << '\n' <<
               " Next scn: " << nextScnHeader.to48() << " (" << nextScn.toString() << ")" << " " << nextTime << '\n' <<
               " Enabled scn: " << enabledScn.to48() << " (" << enabledScn.toString() << ")" << " " << enabledTime << '\n' <<
               " Thread closed scn: " << threadClosedScn.to48() << " (" << threadClosedScn.toString() << ")" <<
               " " << threadClosedTime << '\n' <<
               " Disk cksum: 0x" << std::hex << chSum << " Calc cksum: 0x" << std::hex << chSum2 << '\n' <<
               " Terminal recovery stop scn: " << termialRecScn.to48() << '\n' <<
               " Terminal recovery  " << termialRecTime << '\n' <<
               " Most recent redo scn: " << mostRecentScn.to48() << '\n';
        } else {
            const Scn realNextScn = ctx->readScn(headerBuffer + blockSize + 272);

            ss << " resetlogs count: 0x" << std::hex << resetlogs << " scn: " << resetlogsScn.to64() << '\n' <<
               " prev resetlogs count: 0x" << std::hex << prevResetlogsCnt << " scn: " << prevResetlogsScn.to64() << '\n' <<
               " Low  scn: " << firstScnHeader.to64() << " " << firstTimeHeader << '\n' <<
               " Next scn: " << nextScnHeader.to64() << " " << nextTime << '\n' <<
               " Enabled scn: " << enabledScn.to64() << " " << enabledTime << '\n' <<
               " Thread closed scn: " << threadClosedScn.to64() << " " << threadClosedTime << '\n' <<
               " Real next scn: " << realNextScn.to64() << '\n' <<
               " Disk cksum: 0x" << std::hex << chSum << " Calc cksum: 0x" << std::hex << chSum2 << '\n' <<
               " Terminal recovery stop scn: " << termialRecScn.to64() << '\n' <<
               " Terminal recovery  " << termialRecTime << '\n' <<
               " Most recent redo scn: " << mostRecentScn.to64() << '\n';
        }

        const uint32_t largestLwn = ctx->read32(headerBuffer + blockSize + 268);
        ss << " Largest LWN: " << std::dec << largestLwn << " blocks\n";

        const uint32_t miscFlags = ctx->read32(headerBuffer + blockSize + 236);
        const char* endOfRedo;
        if ((miscFlags & FLAGS_END) != 0)
            endOfRedo = "Yes";
        else
            endOfRedo = "No";
        if ((miscFlags & FLAGS_CLOSEDTHREAD) != 0)
            ss << " FailOver End-of-redo stream : " << endOfRedo << '\n';
        else
            ss << " End-of-redo stream : " << endOfRedo << '\n';

        if ((miscFlags & FLAGS_ASYNC) != 0)
            ss << " Archivelog created using asynchronous network transmittal" << '\n';

        if ((miscFlags & FLAGS_NODATALOSS) != 0)
            ss << " No ctx-loss mode\n";

        if ((miscFlags & FLAGS_RESYNC) != 0)
            ss << " Resynchronization mode\n";
        else
            ss << " Unprotected mode\n";

        if ((miscFlags & FLAGS_CLOSEDTHREAD) != 0)
            ss << " Closed thread archival\n";

        if ((miscFlags & FLAGS_MAXPERFORMANCE) != 0)
            ss << " Maximize performance mode\n";

        ss << " Miscellaneous flags: 0x" << std::hex << miscFlags << '\n';

        if (ctx->version >= RedoLogRecord::REDO_VERSION_12_2) {
            const uint32_t miscFlags2 = ctx->read32(headerBuffer + blockSize + 296);
            ss << " Miscellaneous second flags: 0x" << std::hex << miscFlags2 << '\n';
        }

        auto thr = static_cast<int32_t>(ctx->read32(headerBuffer + blockSize + 432));
        const auto seq2 = static_cast<int32_t>(ctx->read32(headerBuffer + blockSize + 436));
        const Scn scn2 = ctx->readScn(headerBuffer + blockSize + 440);
        const uint8_t zeroBlocks = headerBuffer[blockSize + 206];
        const uint8_t formatId = headerBuffer[blockSize + 207];
        if (ctx->version < RedoLogRecord::REDO_VERSION_12_2)
            ss << " Thread internal enable indicator: thr: " << std::dec << thr << "," <<
               " seq: " << std::dec << seq2 <<
               " scn: " << scn2.to48() << '\n' <<
               " Zero blocks: " << std::dec << static_cast<uint>(zeroBlocks) << '\n' <<
               " Format ID is " << std::dec << static_cast<uint>(formatId) << '\n';
        else
            ss << " Thread internal enable indicator: thr: " << std::dec << thr << "," <<
               " seq: " << std::dec << seq2 <<
               " scn: " << scn2.to64() << '\n' <<
               " Zero blocks: " << std::dec << static_cast<uint>(zeroBlocks) << '\n' <<
               " Format ID is " << std::dec << static_cast<uint>(formatId) << '\n';

        const uint32_t standbyApplyDelay = ctx->read32(headerBuffer + blockSize + 280);
        if (standbyApplyDelay > 0)
            ss << " Standby Apply Delay: " << std::dec << standbyApplyDelay << " minute(s) \n";

        const Time standbyLogCloseTime(ctx->read32(headerBuffer + blockSize + 304));
        if (standbyLogCloseTime.getVal() > 0)
            ss << " Standby Log Close Time:  " << standbyLogCloseTime << '\n';

        ss << " redo log key is ";
        for (uint i = 448; i < 448 + 16; ++i)
            ss << std::setfill('0') << std::setw(2) << std::hex << static_cast<uint>(headerBuffer[blockSize + i]);
        ss << '\n';

        const uint16_t redoKeyFlag = ctx->read16(headerBuffer + blockSize + 480);
        ss << " redo log key flag is " << std::dec << redoKeyFlag << '\n';
        const uint16_t enabledRedoThreads = 1; // TODO: find field position/size
        ss << " Enabled redo threads: " << std::dec << enabledRedoThreads << " \n";
    }

    uint Reader::getBlockSize() const {
        return blockSize;
    }

    FileOffset Reader::getBufferStart() const {
        return FileOffset(bufferStart);
    }

    FileOffset Reader::getBufferEnd() const {
        return FileOffset(bufferEnd);
    }

    Reader::REDO_CODE Reader::getRet() const {
        return ret;
    }

    Scn Reader::getFirstScn() const {
        return firstScn;
    }

    Scn Reader::getFirstScnHeader() const {
        return firstScnHeader;
    }

    Scn Reader::getNextScn() const {
        return nextScn;
    }

    Time Reader::getNextTime() const {
        return nextTime;
    }

    typeBlk Reader::getNumBlocks() const {
        return numBlocksHeader;
    }

    int Reader::getGroup() const {
        return group;
    }

    Seq Reader::getSequence() const {
        return sequence;
    }

    typeResetlogs Reader::getResetlogs() const {
        return resetlogs;
    }

    typeActivation Reader::getActivation() const {
        return activation;
    }

    uint64_t Reader::getSumRead() const {
        return sumRead;
    }

    uint64_t Reader::getSumTime() const {
        return sumTime;
    }

    void Reader::setRet(REDO_CODE newRet) {
        ret = newRet;
    }

    void Reader::setBufferStartEnd(FileOffset newBufferStart, FileOffset newBufferEnd) {
        bufferStart = newBufferStart.getData();
        bufferEnd = newBufferEnd.getData();
    }

    /**
     * @brief 检查重做日志文件的有效性
     * 
     * 此函数用于验证重做日志文件的格式和内容是否正确，并初始化相关状态。
     * 它会设置读取器状态为检查模式，并等待解析器线程完成检查操作。
     * 
     * @return bool 返回检查结果，true表示检查成功，false表示检查失败
     */
    bool Reader::checkRedoLog() {
        // 设置上下文为互斥锁状态，表示进入关键代码段
        contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_REDO);
        // 获取互斥锁，保护共享资源访问
        std::unique_lock<std::mutex> lck(mtx);
        // 设置读取器状态为检查模式
        status = STATUS::CHECK;
        // 初始化序列号、起始SCN和下一个SCN为默认值
        sequence = Seq::zero();
        firstScn = Scn::none();
        nextScn = Scn::none();
        // 通知所有等待缓冲区满条件的线程
        condBufferFull.notify_all();
        // 通知所有等待读取器休眠条件的线程
        condReaderSleeping.notify_all();
    
        // 循环等待直到状态不再是检查模式
        while (status == STATUS::CHECK) {
            // 如果系统正在关闭，则跳出循环
            if (ctx->softShutdown)
                break;
            // 如果启用了睡眠跟踪，则记录日志
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:checkRedoLog");
            // 设置上下文为等待状态
            contextSet(CONTEXT::WAIT, REASON::READER_CHECK);
            // 等待解析器线程完成检查操作
            condParserSleeping.wait(lck);
        }
        // 恢复上下文为CPU执行状态
        contextSet(CONTEXT::CPU);
        // 返回检查结果，OK表示成功
        return (ret == REDO_CODE::OK);
    }

    /**
     * @brief 更新Redo日志状态
     * 
     * 此函数用于更新Redo日志的状态。它会设置状态为UPDATE，并通知所有等待的条件变量。
     * 然后进入一个循环，等待状态不再是UPDATE。如果检测到软关闭请求，则跳出循环。
     * 如果返回码为EMPTY，则等待一段时间后继续循环。最后根据返回码决定是否返回成功。
     * 
     * @return bool 表示更新是否成功
     */
    bool Reader::updateRedoLog() {
        for (;;) {
            // 设置上下文为互斥锁状态，表示正在进行Redo日志更新操作
            contextSet(CONTEXT::MUTEX, REASON::READER_UPDATE_REDO1);
            std::unique_lock<std::mutex> lck(mtx);
            // 设置状态为更新状态
            status = STATUS::UPDATE;
            // 通知所有等待缓冲区满的线程
            condBufferFull.notify_all();
            // 通知所有等待读取器休眠的线程
            condReaderSleeping.notify_all();

            // 当状态为更新状态时，进入循环
            while (status == STATUS::UPDATE) {
                // 如果检测到软关闭请求，则跳出循环
                if (ctx->softShutdown)
                    break;
                // 如果设置了跟踪休眠标志，则记录跟踪日志
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                    ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:updateRedoLog");
                // 设置上下文为等待状态
                contextSet(CONTEXT::WAIT);
                // 等待解析器休眠条件变量
                condParserSleeping.wait(lck);
                // 重新设置上下文为互斥锁状态
                contextSet(CONTEXT::MUTEX, REASON::READER_UPDATE_REDO2);
            }

            // 如果返回码为EMPTY，则等待一段时间后继续循环
            if (ret == REDO_CODE::EMPTY) {
                // 设置上下文为等待状态，并指定原因为读取器空
                contextSet(CONTEXT::WAIT, REASON::READER_EMPTY);
                // 等待指定的微秒数
                condParserSleeping.wait_for(lck, std::chrono::microseconds(ctx->redoReadSleepUs));
                // 重新设置上下文为互斥锁状态
                contextSet(CONTEXT::MUTEX, REASON::READER_UPDATE_REDO3);
                continue;
            }

            // 设置上下文为CPU状态
            contextSet(CONTEXT::CPU);
            // 根据返回码决定是否返回成功
            return (ret == REDO_CODE::OK);
        }
    }

    void Reader::setStatusRead() {
        {
            contextSet(CONTEXT::MUTEX, REASON::READER_SET_READ);
            std::unique_lock<std::mutex> const lck(mtx);
            status = STATUS::READ;
            condBufferFull.notify_all();
            condReaderSleeping.notify_all();
        }
        contextSet(CONTEXT::CPU);
    }

    void Reader::confirmReadData(FileOffset confirmedBufferStart) {
        contextSet(CONTEXT::MUTEX, REASON::READER_CONFIRM);
        {
            std::unique_lock<std::mutex> const lck(mtx);
            bufferStart = confirmedBufferStart.getData();
            if (status == STATUS::READ) {
                condBufferFull.notify_all();
            }
        }
        contextSet(CONTEXT::CPU);
    }

    bool Reader::checkFinished(Thread* t, FileOffset confirmedBufferStart) {
        t->contextSet(CONTEXT::MUTEX, REASON::READER_CHECK_FINISHED);
        {
            std::unique_lock<std::mutex> lck(mtx);
            if (bufferStart < confirmedBufferStart.getData())
                bufferStart = confirmedBufferStart.getData();

            // All work done
            if (confirmedBufferStart.getData() == bufferEnd) {
                if (ret == REDO_CODE::STOPPED || ret == REDO_CODE::OVERWRITTEN || ret == REDO_CODE::FINISHED || status == STATUS::SLEEPING) {
                    t->contextSet(CONTEXT::CPU);
                    return true;
                }
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::SLEEP)))
                    ctx->logTrace(Ctx::TRACE::SLEEP, "Reader:checkFinished");
                t->contextSet(CONTEXT::WAIT, REASON::READER_FINISHED);
                condParserSleeping.wait(lck);
            }
        }
        t->contextSet(CONTEXT::CPU);
        return false;
    }
}
