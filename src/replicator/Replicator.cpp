#include <cerrno>
#include <cstddef>
#include <dirent.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

#include "../builder/Builder.h"
#include "../common/Clock.h"
#include "../common/Ctx.h"
#include "../common/DbIncarnation.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RedoLogException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/types/Seq.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../parser/Transaction.h"
#include "../reader/ReaderFilesystem.h"
#include "../reader/ReaderAsmFilesystem.h"
#include "Replicator.h"
#include "ReplicatorRacOnline.h"
namespace OpenLogReplicator {
    /**
     * @brief 复制器构造函数
     * 
     * 初始化复制器对象，设置上下文、日志获取函数、构建器、元数据、事务缓冲区、别名和数据库名称等成员变量。
     * 
     * @param newCtx 上下文对象指针
     * @param newArchGetLog 归档日志获取函数指针
     * @param newBuilder 构建器对象指针
     * @param newMetadata 元数据对象指针
     * @param newTransactionBuffer 事务缓冲区对象指针
     * @param newAlias 复制器别名
     * @param newDatabase 数据库名称
     */
    Replicator::Replicator(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                           TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase) :
            Thread(newCtx, std::move(newAlias)),
            archGetLog(newArchGetLog),
            builder(newBuilder),
            metadata(newMetadata),
            transactionBuffer(newTransactionBuffer),
            database(std::move(newDatabase)) {
        // 设置解析器线程
        ctx->parserThread = this;
    }

    Replicator::~Replicator() {
        readerDropAll();

        while (!archiveRedoQueue.empty()) {
            Parser* parser = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete parser;
        }

        for (Parser* parser: onlineRedoSet)
            delete parser;
        onlineRedoSet.clear();

        pathMapping.clear();
        redoLogsBatch.clear();
    }

    void Replicator::initialize() {
    }

    void Replicator::cleanArchList() {
        while (!archiveRedoQueue.empty()) {
            Parser* parser = archiveRedoQueue.top();
            archiveRedoQueue.pop();
            delete parser;
        }
    }
    void Replicator::updateOnlineLogs() {
        for (Parser* onlineRedo: onlineRedoSet) {
            if (!onlineRedo->reader->updateRedoLog())
                throw RuntimeException(10039, "updating of online redo logs failed for " + onlineRedo->path);
            onlineRedo->sequence = onlineRedo->reader->getSequence();
            onlineRedo->firstScn = onlineRedo->reader->getFirstScn();
            onlineRedo->nextScn = onlineRedo->reader->getNextScn();
        }
    }

    void Replicator::readerDropAll() {
        for (;;) {
            bool wakingUp = false;
            for (Reader* reader: readers) {
                if (!reader->finished) {
                    reader->wakeUp();
                    wakingUp = true;
                }
            }
            if (!wakingUp)
                break;
            contextSet(CONTEXT::SLEEP);
            usleep(1000);
            contextSet(CONTEXT::CPU);
        }

        while (!readers.empty()) {
            Reader* reader = *(readers.begin());
            ctx->finishThread(reader);
            readers.erase(reader);
            delete reader;
        }

        archReader = nullptr;
        readers.clear();
    }


    /**
     * @brief 加载数据库元数据
     * 
     * 此函数用于初始化归档读取器，创建一个新的读取器实例用于处理归档日志。
     * 通过调用 readerCreate(0) 创建一个组标识为0的读取器，通常用于处理归档日志。
     */
    void Replicator::loadDatabaseMetadata() {
        archReader = readerCreate(0);
    }

    /**
     * @brief 定位读取器位置
     * 
     * 此函数用于设置读取器的起始序列号和文件偏移量。
     * 如果元数据中指定了起始序列号，则使用该序列号和零偏移量；
     * 否则使用零序列号和零偏移量作为起始位置。
     */
    void Replicator::positionReader() {
        if (metadata->startSequence != Seq::none())
            metadata->setSeqFileOffset(metadata->startSequence, FileOffset::zero());
        else
            metadata->setSeqFileOffset(Seq(Seq::zero()), FileOffset::zero());
    }

    void Replicator::verifySchema(Scn currentScn __attribute__((unused))) {
        // Nothing for offline mode
    }

    void Replicator::createSchema() {
        if (ctx->isFlagSet(Ctx::REDO_FLAGS::SCHEMALESS)) {
            metadata->allowCheckpoints();
            return;
        }

        throw RuntimeException(10040, "schema file missing");
    }


    void Replicator::updateOnlineRedoLogData() {
        int64_t lastGroup = -1;
        Reader* onlineReader = nullptr;

        for (auto* redoLog: metadata->redoLogs) {
            if (redoLog->group != lastGroup || onlineReader == nullptr) {
                onlineReader = readerCreate(redoLog->group);
                onlineReader->paths.clear();
                lastGroup = redoLog->group;
            }
            onlineReader->paths.push_back(redoLog->path);
        }

        checkOnlineRedoLogs();
    }

    void Replicator::run() {
        // 检查是否启用了线程跟踪，如果启用则记录复制器启动的日志信息
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "replicator (" + ss.str() + ") start");
        }

        try {
            // 等待写入器完成操作
            metadata->waitForWriter(ctx->parserThread);

            // 加载数据库元数据
            loadDatabaseMetadata();
            // 读取检查点信息
            metadata->readCheckpoints();
            // 如果不是仅归档模式，则更新在线重做日志数据
            if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                updateOnlineRedoLogData();
            // 记录时区信息
            ctx->info(0, "timezone: " + Data::timezoneToString(-timezone) + ", db-timezone: " + Data::timezoneToString(metadata->dbTimezone) +
                         ", log-timezone: " + Data::timezoneToString(ctx->logTimezone) + ", host-timezone: " + Data::timezoneToString(ctx->hostTimezone));

            // 进入主循环，等待元数据状态变为REPLICATE
            do {
                // 检查是否需要软关闭
                if (ctx->softShutdown)
                    break;
                // 等待写入器完成操作
                metadata->waitForWriter(ctx->parserThread);

                // 如果元数据状态为READY，则继续下一次循环
                if (metadata->status == Metadata::STATUS::READY)
                    continue;

                // 再次检查是否需要软关闭
                if (ctx->softShutdown)
                    break;
                try {
                    // 打印启动信息
                    printStartMsg();
                    // 如果resetlogs不为0，则记录当前resetlogs值
                    if (metadata->resetlogs != 0)
                        ctx->info(0, "current resetlogs is: " + std::to_string(metadata->resetlogs));
                    // 如果firstDataScn不为空，则记录first data SCN
                    if (metadata->firstDataScn != Scn::none())
                        ctx->info(0, "first data SCN: " + metadata->firstDataScn.toString());
                    // 如果firstSchemaScn不为空，则记录first schema SCN
                    if (metadata->firstSchemaScn != Scn::none())
                        ctx->info(0, "first schema SCN: " + metadata->firstSchemaScn.toString());

                    // 如果firstDataScn或sequence为空，则定位读取器
                    if (metadata->firstDataScn == Scn::none() || metadata->sequence == Seq::none())
                        positionReader();

                    // 没有可用的schema?
                    if (metadata->schema->scn == Scn::none())
                        // 创建schema
                        createSchema();
                    else
                        // 允许检查点
                        metadata->allowCheckpoints();
                    // 更新XML上下文
                    metadata->schema->updateXmlCtx();

                    // 如果sequence为空，则抛出启动异常
                    if (metadata->sequence == Seq::none())
                        throw BootException(10028, "starting sequence is unknown");

                    // 根据firstDataScn是否为空，记录最后确认的SCN、起始序列号和偏移量
                    if (metadata->firstDataScn == Scn::none())
                        ctx->info(0, "last confirmed scn: <none>, starting sequence: " + metadata->sequence.toString() + ", offset: " +
                                     metadata->fileOffset.toString());
                    else
                        ctx->info(0, "last confirmed scn: " + metadata->firstDataScn.toString() + ", starting sequence: " +
                                     metadata->sequence.toString() + ", offset: " + metadata->fileOffset.toString());

                    // 如果数据库块校验和关闭且未禁用块校验和检查，则给出提示
                    if ((metadata->dbBlockChecksum == "OFF" || metadata->dbBlockChecksum == "FALSE") &&
                            !ctx->isDisableChecksSet(Ctx::DISABLE_CHECKS::BLOCK_SUM)) {
                        ctx->hint("set DB_BLOCK_CHECKSUM = TYPICAL on the database or turn off consistency checking in OpenLogReplicator "
                                  "setting parameter disable-checks: " + std::to_string(static_cast<uint>(Ctx::DISABLE_CHECKS::BLOCK_SUM)) +
                                  " for the reader");
                    }

                } catch (BootException& ex) {
                    // 如果未启用启动故障安全机制，则抛出运行时异常
                    if (!metadata->bootFailsafe)
                        throw RuntimeException(ex.code, ex.msg);

                    // 记录错误信息
                    ctx->error(ex.code, ex.msg);
                    // 记录复制启动失败信息，并等待进一步命令
                    ctx->info(0, "replication startup failed, waiting for further commands");
                    // 设置元数据状态为就绪
                    metadata->setStatusReady(this);
                    // 继续循环
                    continue;
                }

                // 启动成功
                ctx->info(0, "resume writer");
                // 设置元数据状态为复制
                metadata->setStatusReplicate(this);
            } while (metadata->status != Metadata::STATUS::REPLICATE);

            while (!ctx->softShutdown) {
                bool logsProcessed = false;

                logsProcessed |= processArchivedRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!continueWithOnline())
                    break;
                if (ctx->softShutdown)
                    break;

                if (!ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY))
                    logsProcessed |= processOnlineRedoLogs();
                if (ctx->softShutdown)
                    break;

                if (!logsProcessed) {
                    ctx->info(0, "no redo logs to process, waiting for new redo logs");
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->refreshIntervalUs);
                    contextSet(CONTEXT::CPU);
                }
            }
        } catch (DataException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RedoLogException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (RuntimeException& ex) {
            ctx->error(ex.code, ex.msg);
            ctx->stopHard();
        } catch (std::bad_alloc& ex) {
            ctx->error(10018, "memory allocation failed: " + std::string(ex.what()));
            ctx->stopHard();
        }

        ctx->info(0, "Replicator for: " + database + " is shutting down");
        transactionBuffer->purge();

        ctx->replicatorFinished = true;
        ctx->printMemoryUsageHWM();

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
            std::ostringstream ss;
            ss << std::this_thread::get_id();
            ctx->logTrace(Ctx::TRACE::THREADS, "replicator (" + ss.str() + ") stop");
        }
    }

    /**
     * @brief 创建或获取指定组的读取器实例
     * 
     * 此函数用于创建一个新的读取器实例或返回已存在的相同组的读取器。
     * 
     * @param group 读取器所属的组标识
     * @return Reader* 返回创建或找到的读取器指针
     */
    Reader* Replicator::readerCreate(int group) {
        // 查找是否已存在相同组的读取器
        for (Reader* reader: readers)
            if (reader->getGroup() == group)
                return reader;

        auto* replicator_rac = dynamic_cast<ReplicatorRacOnline*>(this);
        Reader* readerFS;
        
        // 根据是否使用ASM创建不同的读取器实例
        if (replicator_rac->getAsm()) {
            readerFS = new ReaderAsmFilesystem(ctx, alias + "-reader-" + std::to_string(group), database, group,
                                              metadata->dbBlockChecksum != "OFF" && metadata->dbBlockChecksum != "FALSE");
        } else  {
            readerFS = new ReaderFilesystem(ctx, alias + "-reader-" + std::to_string(group), database, group,
                                              metadata->dbBlockChecksum != "OFF" && metadata->dbBlockChecksum != "FALSE");
        }
        
        // 将新创建的读取器添加到读取器集合中并初始化
        readers.insert(readerFS);
        readerFS->initialize();

        // 启动读取器线程
        ctx->spawnThread(readerFS);
        return readerFS;
    }

    void Replicator::checkOnlineRedoLogs() {
        for (Parser* onlineRedo: onlineRedoSet)
            delete onlineRedo;
        onlineRedoSet.clear();

        for (Reader* reader: readers) {
            if (reader->getGroup() == 0)
                continue;

            bool foundPath = false;
            for (const std::string& path: reader->paths) {
                reader->fileName = path;
                applyMapping(reader->fileName);
                if (reader->checkRedoLog()) {
                    foundPath = true;
                    auto* parser = new Parser(ctx, builder, metadata, transactionBuffer,
                                             reader->getGroup(), reader->fileName);

                    parser->reader = reader;
                    ctx->info(0, "online redo log: " + reader->fileName);
                    onlineRedoSet.insert(parser);
                    break;
                }
            }

            if (!foundPath) {
                const int64_t badGroup = reader->getGroup();
                for (const std::string& path: reader->paths) {
                    std::string pathMapped(path);
                    applyMapping(pathMapped);
                    reader->showHint(this, path, pathMapped);
                }
                throw RuntimeException(10027, "can't read any member of group " + std::to_string(badGroup));
            }
        }
    }

    // Format uses wildcards:
    // %s - sequence number
    // %S - sequence number zero filled
    // %t - thread id
    // %T - thread id zero filled
    // %r - resetlogs id
    // %a - activation id
    // %d - database id
    // %h - some hash
    Seq Replicator::getSequenceFromFileName(Replicator* replicator, const std::string& file) {
        Seq sequence{0};
        size_t i{};
        size_t j{};

        while (i < replicator->metadata->logArchiveFormat.length() && j < file.length()) {
            if (replicator->metadata->logArchiveFormat[i] == '%') {
                if (i + 1 >= replicator->metadata->logArchiveFormat.length()) {
                    replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                                    replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                                    std::to_string(i) + ", found end after %");
                    return Seq::zero();
                }
                uint digits = 0;
                if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 't' || replicator->metadata->logArchiveFormat[i + 1] == 'T' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 'r' || replicator->metadata->logArchiveFormat[i + 1] == 'a' ||
                    replicator->metadata->logArchiveFormat[i + 1] == 'd') {
                    // Some [0-9]*
                    uint32_t number{};
                    while (j < file.length() && file[j] >= '0' && file[j] <= '9') {
                        number = number * 10 + (file[j] - '0');
                        ++j;
                        ++digits;
                    }

                    if (replicator->metadata->logArchiveFormat[i + 1] == 's' || replicator->metadata->logArchiveFormat[i + 1] == 'S')
                        sequence = Seq(number);
                    i += 2;
                } else if (replicator->metadata->logArchiveFormat[i + 1] == 'h') {
                    // Some [0-9a-z]*
                    while (j < file.length() && ((file[j] >= '0' && file[j] <= '9') || (file[j] >= 'a' && file[j] <= 'z'))) {
                        ++j;
                        ++digits;
                    }
                    i += 2;
                }

                if (digits == 0) {
                    replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                                    replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                                    std::to_string(i) + ", found no number/hash");
                    return Seq::zero();
                }
            } else if (file[j] == replicator->metadata->logArchiveFormat[i]) {
                ++i;
                ++j;
            } else {
                replicator->ctx->warning(60028, "can't get sequence from file: " + file + " log_archive_format: " +
                                                replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                                std::to_string(i) + ", found different values");
                return Seq::zero();
            }
        }

        if (i == replicator->metadata->logArchiveFormat.length() && j == file.length())
            return sequence;

        replicator->ctx->warning(60028, "error getting sequence from file: " + file + " log_archive_format: " +
                                        replicator->metadata->logArchiveFormat + " at position " + std::to_string(j) + " format position " +
                                        std::to_string(i) + ", found no sequence");
        return Seq::zero();
    }

    void Replicator::addPathMapping(std::string source, std::string target) {
        if (unlikely(ctx->isTraceSet(Ctx::TRACE::FILE)))
            ctx->logTrace(Ctx::TRACE::FILE, "added mapping [" + source + "] -> [" + target + "]");
        pathMapping.push_back(std::move(source));
        pathMapping.push_back(std::move(target));
    }

    void Replicator::addRedoLogsBatch(std::string path) {
        redoLogsBatch.emplace_back(std::move(path));
    }

    void Replicator::applyMapping(std::string& path) {
        const size_t newPathLength = path.length();
        std::array<char, Ctx::MAX_PATH_LENGTH> pathBuffer {};

        for (size_t i = 0; i < pathMapping.size() / 2; ++i) {
            const uint64_t sourceLength = pathMapping[i * 2].length();
            const uint64_t targetLength = pathMapping[(i * 2) + 1].length();

            if (sourceLength <= newPathLength &&
                newPathLength - sourceLength + targetLength < Ctx::MAX_PATH_LENGTH - 1 &&
                memcmp(path.c_str(), pathMapping[i * 2].c_str(), sourceLength) == 0) {

                memcpy(reinterpret_cast<void*>(pathBuffer.data()),
                       reinterpret_cast<const void*>(pathMapping[(i * 2) + 1].c_str()), targetLength);
                memcpy(reinterpret_cast<void*>(pathBuffer.data() + targetLength),
                       reinterpret_cast<const void*>(path.c_str() + sourceLength), newPathLength - sourceLength);
                pathBuffer[newPathLength - sourceLength + targetLength] = 0;
                path.assign(pathBuffer.data());
                break;
            }
        }
    }

    bool Replicator::checkConnection() {
        return true;
    }

    void Replicator::goStandby() {
    }

    bool Replicator::continueWithOnline() {
        return true;
    }

    std::string Replicator::getModeName() const {
        return {"offline"};
    }

    void Replicator::archGetLogPath(Replicator* replicator) {
        if (replicator->metadata->logArchiveFormat.empty())
            throw RuntimeException(10044, "missing location of archived redo logs for offline mode");

        std::string mappedPath(replicator->metadata->dbRecoveryFileDest + "/" + replicator->metadata->context + "/archivelog");
        replicator->applyMapping(mappedPath);
        if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
            replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

        DIR* dir = opendir(mappedPath.c_str());
        if (dir == nullptr)
            throw RuntimeException(10012, "directory: " + mappedPath + " - can't read");

        std::string newLastCheckedDay;
        const struct dirent* ent;
        while ((ent = readdir(dir)) != nullptr) {
            const std::string dName(ent->d_name);
            if (dName == "." || dName == "..")
                continue;

            struct stat fileStat{};
            const std::string mappedSubPath(mappedPath + "/" + ent->d_name);
            if (stat(mappedSubPath.c_str(), &fileStat) != 0) {
                replicator->ctx->warning(10003, "file: " + mappedSubPath + " - get metadata returned: " + strerror(errno));
                continue;
            }

            if (!S_ISDIR(fileStat.st_mode))
                continue;

            // Skip earlier days
            if (replicator->lastCheckedDay.empty() && replicator->lastCheckedDay == ent->d_name)
                continue;

            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath + "/" + ent->d_name);

            const std::string mappedPathWithFile(mappedPath + "/" + ent->d_name);
            DIR* dir2 = opendir(mappedPathWithFile.c_str());
            if (dir2 == nullptr) {
                closedir(dir);
                throw RuntimeException(10012, "directory: " + mappedPathWithFile + " - can't read");
            }

            const struct dirent* ent2;
            while ((ent2 = readdir(dir2)) != nullptr) {
                const std::string dName2(ent->d_name);
                if (dName2 == "." || dName2 == "..")
                    continue;

                const std::string fileName(mappedPath + "/" + ent->d_name + "/" + ent2->d_name);
                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + fileName);

                const Seq sequence = getSequenceFromFileName(replicator, ent2->d_name);

                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + sequence.toString());

                if (sequence == Seq::zero() || sequence < replicator->metadata->sequence)
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, fileName);

                parser->firstScn = Scn::none();
                parser->nextScn = Scn::none();
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
            }
            closedir(dir2);

            if (newLastCheckedDay.empty() || (newLastCheckedDay != ent->d_name))
                newLastCheckedDay = ent->d_name;
        }
        closedir(dir);

        if (!newLastCheckedDay.empty() || (!replicator->lastCheckedDay.empty() && replicator->lastCheckedDay.compare(newLastCheckedDay) < 0)) {
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "updating last checked day to: " + newLastCheckedDay);
            replicator->lastCheckedDay = newLastCheckedDay;
        }
    }

    void Replicator::archGetLogList(Replicator* replicator) {
        Seq sequenceStart = Seq::none();
        for (const std::string& mappedPath: replicator->redoLogsBatch) {
            if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

            struct stat fileStat{};
            if (stat(mappedPath.c_str(), &fileStat) != 0) {
                replicator->ctx->warning(10003, "file: " + mappedPath + " - get metadata returned: " + strerror(errno));
                continue;
            }

            // Single file
            if (!S_ISDIR(fileStat.st_mode)) {
                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + mappedPath);

                // Getting file name from the path
                const char* fileName = mappedPath.c_str();
                size_t j = mappedPath.length();
                while (j > 0) {
                    if (fileName[j - 1] == '/')
                        break;
                    --j;
                }
                const Seq sequence = getSequenceFromFileName(replicator, fileName + j);

                if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                    replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + sequence.toString());

                if (sequence == Seq::zero() || sequence < replicator->metadata->sequence)
                    continue;

                auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                          replicator->transactionBuffer, 0, mappedPath);
                parser->firstScn = Scn::none();
                parser->nextScn = Scn::none();
                parser->sequence = sequence;
                replicator->archiveRedoQueue.push(parser);
                if (sequenceStart == Seq::none() || sequenceStart > sequence)
                    sequenceStart = sequence;

            } else {
                // Dir, check all files
                DIR* dir = opendir(mappedPath.c_str());
                if (dir == nullptr)
                    throw RuntimeException(10012, "directory: " + mappedPath + " - can't read");

                const struct dirent* ent;
                while ((ent = readdir(dir)) != nullptr) {
                    const std::string dName(ent->d_name);
                    if (dName == "." || dName == "..")
                        continue;

                    const std::string fileName(mappedPath + "/" + ent->d_name);
                    if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "checking path: " + fileName);

                    const Seq sequence = getSequenceFromFileName(replicator, ent->d_name);

                    if (unlikely(replicator->ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        replicator->ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "found seq: " + sequence.toString());

                    if (sequence == Seq::zero() || sequence < replicator->metadata->sequence)
                        continue;

                    auto* parser = new Parser(replicator->ctx, replicator->builder, replicator->metadata,
                                             replicator->transactionBuffer, 0, fileName);
                    parser->firstScn = Scn::none();
                    parser->nextScn = Scn::none();
                    parser->sequence = sequence;
                    replicator->archiveRedoQueue.push(parser);
                }
                closedir(dir);
            }
        }

        if (sequenceStart != Seq::none() && replicator->metadata->sequence == Seq::zero())
            replicator->metadata->setSeqFileOffset(sequenceStart, FileOffset::zero());
        replicator->redoLogsBatch.clear();
    }

    bool parserCompare::operator()(const Parser* p1, const Parser* p2) {
        return p1->sequence > p2->sequence;
    }

    /**
     * @brief 更新数据库的resetlogs信息
     * 
     * 此函数用于在数据库启动或日志处理过程中更新resetlogs信息。它会检查当前的resetlogs值是否与元数据中的值匹配，
     * 如果不匹配，则查找新的resetlogs值并更新元数据。如果找不到匹配的resetlogs值，则抛出异常。
     * 
     * 主要步骤包括：
     * 1. 获取元数据检查点的互斥锁。
     * 2. 遍历数据库化身列表，查找与当前resetlogs值匹配的化身。
     * 3. 如果resetlogs值发生变化，查找新的resetlogs值并更新元数据。
     * 4. 如果数据库化身列表为空或找不到匹配的化身，则抛出异常。
     */
    void Replicator::updateResetlogs() {
        // 设置上下文为互斥锁状态，用于更新操作
        contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_UPDATE);
        // 获取元数据检查点的互斥锁，确保线程安全
        std::unique_lock<std::mutex> const lck(metadata->mtxCheckpoint);

        // 遍历数据库化身列表，查找与当前resetlogs值匹配的化身
        for (DbIncarnation* oi: metadata->dbIncarnations) {
            if (oi->resetlogs == metadata->resetlogs) {
                metadata->dbIncarnationCurrent = oi;
                break;
            }
        }

        // 检查resetlogs是否发生变化，如果发生变化则查找新的resetlogs值
        for (const DbIncarnation* oi: metadata->dbIncarnations) {
            if (oi->resetlogsScn == metadata->nextScn &&
                metadata->dbIncarnationCurrent->resetlogs == metadata->resetlogs &&
                oi->priorIncarnation == metadata->dbIncarnationCurrent->incarnation) {
                // 记录新的resetlogs值
                ctx->info(0, "new resetlogs detected: " + std::to_string(oi->resetlogs));
                // 更新元数据中的resetlogs值
                metadata->setResetlogs(oi->resetlogs);
                // 重置序列号和文件偏移量
                metadata->sequence = Seq::zero();
                metadata->fileOffset = FileOffset::zero();
                // 设置上下文为CPU状态
                contextSet(CONTEXT::CPU);
                return;
            }
        }

        // 如果数据库化身列表为空，则直接返回
        if (metadata->dbIncarnations.empty()) {
            contextSet(CONTEXT::CPU);
            return;
        }

        // 如果找不到匹配的化身，则抛出异常
        if (metadata->dbIncarnationCurrent == nullptr) {
            contextSet(CONTEXT::CPU);
            throw RuntimeException(10045, "resetlogs (" + std::to_string(metadata->resetlogs) + ") not found in incarnation list");
        }
        // 设置上下文为CPU状态
        contextSet(CONTEXT::CPU);
    }

    void Replicator::wakeUp() {
        metadata->wakeUp(this);
    }

    /**
     * @brief 打印复制器启动信息
     * 
     * 此函数用于在复制器启动时打印相关信息，包括数据库名称、运行模式、启动时间和序列号等。
     * 根据元数据中的设置，启动信息可能包含绝对时间、相对时间或SCN（系统更改号）。
     */
    void Replicator::printStartMsg() const {
        // 初始化标志字符串
        std::string flagsStr;
        if (ctx->flags != 0)
            flagsStr = " (flags: " + std::to_string(ctx->flags) + ")";

        // 确定启动时间信息
        std::string starting;
        if (!metadata->startTime.empty())
            // 使用绝对时间
            starting = "time: " + metadata->startTime;
        else if (metadata->startTimeRel > 0)
            // 使用相对时间
            starting = "time-rel: " + std::to_string(metadata->startTimeRel);
        else if (metadata->startScn != Scn::none())
            // 使用SCN
            starting = "scn: " + metadata->startScn.toString();
        else
            // 默认使用当前时间
            starting = "NOW";

        // 确定起始序列号信息
        std::string startingSeq;
        if (metadata->startSequence != Seq::none())
            startingSeq = ", seq: " + metadata->startSequence.toString();

        // 打印启动信息
        ctx->info(0, "Replicator for " + database + " in " + getModeName() + " mode is starting" + flagsStr + " from " + starting +
                     startingSeq);
    }

    bool Replicator::processArchivedRedoLogs() {
        // 用于存储读取器返回码的变量
        Reader::REDO_CODE ret;
        // 指向解析器对象的指针
        Parser* parser;
        // 标记日志是否已被处理的布尔变量，初始值为false
        bool logsProcessed = false;

        while (!ctx->softShutdown) {
            // 如果启用了REDO跟踪，则记录当前检查的归档日志序列号
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "checking archived redo logs, seq: " + metadata->sequence.toString());
            // 更新resetlogs信息，确保读取器能够正确处理日志文件
            updateResetlogs();
            // 获取归档日志文件，用于后续处理
            archGetLog(this);

            // 检查归档重做日志队列是否为空
            if (archiveRedoQueue.empty()) {
                // 如果设置了仅处理归档日志的标志
                if (ctx->isFlagSet(Ctx::REDO_FLAGS::ARCH_ONLY)) {
                    // 如果启用了归档列表跟踪，则记录日志信息
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::ARCHIVE_LIST)))
                        ctx->logTrace(Ctx::TRACE::ARCHIVE_LIST, "archived redo log missing for seq: " + metadata->sequence.toString() +
                                                                ", sleeping");
                    // 设置上下文为睡眠状态
                    contextSet(CONTEXT::SLEEP);
                    // 休眠指定的时间（微秒）
                    usleep(ctx->archReadSleepUs);
                    // 恢复上下文为CPU状态
                    contextSet(CONTEXT::CPU);
                } else {
                    // 如果未设置仅处理归档日志的标志，则跳出循环
                    break;
                }
            }

            if (unlikely(ctx->isTraceSet(Ctx::TRACE::THREADS))) {
                std::ostringstream ss;
                ss << std::this_thread::get_id();
                ctx->logTrace(Ctx::TRACE::REDO, "searching archived redo log for seq: " + metadata->sequence.toString());
            }
            // 循环处理归档重做日志队列中的文件，直到队列为空或接收到软关闭信号
            while (!archiveRedoQueue.empty() && !ctx->softShutdown) {
                // 获取队列中优先级最高的解析器（通常是序列号最小的文件）
                parser = archiveRedoQueue.top();
                // 如果启用了REDO跟踪，则记录当前处理的文件路径、序列号和SCN
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                    ctx->logTrace(Ctx::TRACE::REDO, parser->path + " is seq: " + parser->sequence.toString() + ", scn: " + parser->firstScn.toString());

                // 当没有元数据时，从第一个文件开始处理
                if (metadata->sequence == Seq::zero()) {
                    // 设置上下文为互斥锁状态，防止并发访问
                    contextSet(CONTEXT::MUTEX, REASON::REPLICATOR_ARCH);
                    // 获取元数据检查点的互斥锁，确保线程安全
                    std::unique_lock<std::mutex> const lck(metadata->mtxCheckpoint);
                    // 将元数据的序列号设置为当前解析器的序列号
                    metadata->sequence = parser->sequence;
                    // 恢复上下文为CPU状态
                    contextSet(CONTEXT::CPU);
                }

                // 跳过序列号小于元数据序列号的旧归档重做日志
                if (parser->sequence < metadata->sequence) {
                    // 从队列中移除该解析器
                    archiveRedoQueue.pop();
                    // 删除解析器对象以释放内存
                    delete parser;
                    // 继续处理下一个文件
                    continue;
                }

                // 如果当前解析器的序列号大于元数据序列号，说明缺少中间的日志文件
                if (parser->sequence > metadata->sequence) {
                    // 记录警告信息，提示找不到指定序列号的归档日志
                    ctx->warning(60027, "couldn't find archive log for seq: " + metadata->sequence.toString() + ", found: " +
                                        parser->sequence.toString() + ", sleeping " + std::to_string(ctx->archReadSleepUs) + " us");
                    // 设置上下文为休眠状态
                    contextSet(CONTEXT::SLEEP);
                    // 休眠指定的时间，等待日志文件可用
                    usleep(ctx->archReadSleepUs);
                    // 恢复上下文为CPU状态
                    contextSet(CONTEXT::CPU);
                    // 清理归档日志列表
                    cleanArchList();
                    // 再次尝试获取归档日志
                    archGetLog(this);
                    // 继续处理下一个文件
                    continue;
                }

                // 标记日志已处理，并将解析器与归档读取器关联
                logsProcessed = true;
                parser->reader = archReader;

                // 设置归档读取器要处理的文件名
                archReader->fileName = parser->path;
                // 获取最大重试次数
                uint retry = ctx->archReadTries;

                // 循环尝试打开并更新重做日志文件，直到成功或达到最大重试次数
                while (true) {
                    // 检查并更新重做日志文件，如果成功则跳出循环
                    if (archReader->checkRedoLog() && archReader->updateRedoLog()) {
                        break;
                    }

                    // 如果重试次数已用完，则抛出异常
                    if (retry == 0)
                        throw RuntimeException(10009, "file: " + parser->path + " - failed to open after " +
                                                      std::to_string(ctx->archReadTries) + " tries");

                    // 记录日志信息，提示文件尚未准备好读取
                    ctx->info(0, "archived redo log " + parser->path + " is not ready for read, sleeping " +
                                 std::to_string(ctx->archReadSleepUs) + " us");
                    // 设置上下文为休眠状态
                    contextSet(CONTEXT::SLEEP);
                    // 休眠指定时间
                    usleep(ctx->archReadSleepUs);
                    // 恢复上下文为CPU状态
                    contextSet(CONTEXT::CPU);
                    // 减少重试次数
                    --retry;
                }

                ret = parser->parse();
                metadata->firstScn = parser->firstScn;
                metadata->nextScn = parser->nextScn;

                if (ctx->softShutdown)
                    break;

                if (ret != Reader::REDO_CODE::FINISHED) {
                    if (ret == Reader::REDO_CODE::STOPPED) {
                        archiveRedoQueue.pop();
                        delete parser;
                        break;
                    }
                    throw RuntimeException(10047, "archive log processing returned: " + std::string(Reader::REDO_MSG[static_cast<uint>(ret)]) + ", code: " +
                                                  std::to_string(static_cast<uint>(ret)));
                }

                // verifySchema(metadata->nextScn);

                ++metadata->sequence;
                archiveRedoQueue.pop();
                delete parser;

                if (ctx->stopLogSwitches > 0) {
                    --ctx->stopLogSwitches;
                    if (ctx->stopLogSwitches == 0) {
                        ctx->info(0, "shutdown started - exhausted number of log switches");
                        ctx->stopSoft();
                    }
                }
            }

            if (!logsProcessed)
                break;
        }

        return logsProcessed;
    }

    bool Replicator::processOnlineRedoLogs() {
        Parser* parser;
        bool logsProcessed = false;

        if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
            ctx->logTrace(Ctx::TRACE::REDO, "checking online redo logs, seq: " + metadata->sequence.toString());
        updateResetlogs();
        updateOnlineLogs();

        while (!ctx->softShutdown) {
            parser = nullptr;
            if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                ctx->logTrace(Ctx::TRACE::REDO, "searching online redo log for seq: " + metadata->sequence.toString());

            // Keep reading online redo logs while it is possible
            bool higher = false;
            const time_ut beginTime = ctx->clock->getTimeUt();

            while (!ctx->softShutdown) {
                for (Parser* onlineRedo: onlineRedoSet) {
                    if (onlineRedo->reader->getSequence() > metadata->sequence)
                        higher = true;

                    if (onlineRedo->reader->getSequence() == metadata->sequence &&
                            (onlineRedo->reader->getNumBlocks() == Ctx::ZERO_BLK || metadata->fileOffset <
                            FileOffset(onlineRedo->reader->getNumBlocks(), onlineRedo->reader->getBlockSize()))) {
                        parser = onlineRedo;
                    }

                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO) && ctx->logLevel >= Ctx::LOG::DEBUG))
                        ctx->logTrace(Ctx::TRACE::REDO, onlineRedo->path + " is seq: " + onlineRedo->sequence.toString() +
                                                        ", scn: " + onlineRedo->firstScn.toString() + ", blocks: " +
                                                        std::to_string(onlineRedo->reader->getNumBlocks()));
                }

                // All so far read, waiting for switch
                if (parser == nullptr && !higher) {
                    contextSet(CONTEXT::SLEEP);
                    usleep(ctx->redoReadSleepUs);
                    contextSet(CONTEXT::CPU);
                } else
                    break;

                if (ctx->softShutdown)
                    break;

                const time_ut endTime = ctx->clock->getTimeUt();
                if (beginTime + static_cast<time_ut>(ctx->refreshIntervalUs) < endTime) {
                    if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                        ctx->logTrace(Ctx::TRACE::REDO, "refresh interval reached, checking online redo logs again");

                    updateOnlineRedoLogData();
                    updateOnlineLogs();
                    goStandby();
                    break;
                }

                updateOnlineLogs();
            }

            if (parser == nullptr)
                break;

            // If online redo log is overwritten - then switch to reading archive logs
            if (ctx->softShutdown)
                break;
            logsProcessed = true;

            const Reader::REDO_CODE ret = parser->parse();
            metadata->setFirstNextScn(parser->firstScn, parser->nextScn);

            if (ctx->softShutdown)
                break;

            if (ret == Reader::REDO_CODE::FINISHED) {
                // verifySchema(metadata->nextScn);
                metadata->setNextSequence();
            } else if (ret == Reader::REDO_CODE::STOPPED || ret == Reader::REDO_CODE::OK) {
                if (unlikely(ctx->isTraceSet(Ctx::TRACE::REDO)))
                    ctx->logTrace(Ctx::TRACE::REDO, "updating redo log files, return code: " + std::to_string(static_cast<uint>(ret)) + ", sequence: " +
                                                    metadata->sequence.toString() + ", first scn: " + metadata->firstScn.toString() + ", next scn: " +
                                                    metadata->nextScn.toString());

                updateOnlineRedoLogData();
                updateOnlineLogs();
            } else if (ret == Reader::REDO_CODE::OVERWRITTEN) {
                ctx->info(0, "online redo log has been overwritten by new ctx, continuing reading from archived redo log");
                break;
            } else {
                if (parser->group == 0) {
                    throw RuntimeException(10048, "read archived redo log, code: " + std::to_string(static_cast<uint>(ret)));
                }                     throw RuntimeException(10049, "read online redo log, code: " + std::to_string(static_cast<uint>(ret)));

            }

            if (ctx->stopLogSwitches > 0) {
                --ctx->stopLogSwitches;
                if (ctx->stopLogSwitches == 0) {
                    ctx->info(0, "shutdown initiated by number of log switches");
                    ctx->stopSoft();
                }
            }
        }
        return logsProcessed;
    }
}
