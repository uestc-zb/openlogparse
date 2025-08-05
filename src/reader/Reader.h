#include <atomic>
#include <vector>

#include "../common/Thread.h"
#include "../common/types/FileOffset.h"
#include "../common/types/Scn.h"
#include "../common/types/Seq.h"
#include "../common/types/Time.h"
#include "../common/types/Types.h"

#ifndef READER_H_
#define READER_H_

namespace OpenLogReplicator {
    class Reader : public Thread {
    public:
        /**
         * @brief Redo日志读取状态码枚举
         * 
         * 定义了Redo日志读取过程中可能遇到的各种状态和错误代码
         */
        enum class REDO_CODE : unsigned char {
            OK,                 // 读取成功
            OVERWRITTEN,        // 数据被覆盖
            FINISHED,           // 读取完成
            STOPPED,            // 读取被停止
            SHUTDOWN,           // 系统关闭
            EMPTY,              // 空数据
            ERROR_READ,         // 读取错误
            ERROR_WRITE,        // 写入错误
            ERROR_SEQUENCE,     // 序列错误
            ERROR_CRC,          // CRC校验错误
            ERROR_BLOCK,        // 块错误
            ERROR_BAD_DATA,     // 数据损坏
            ERROR,              // 一般错误
            CNT                 // 枚举值计数
        };

    protected:
        static constexpr uint64_t FLAGS_END{0x0008};
        static constexpr uint64_t FLAGS_ASYNC{0x0100};
        static constexpr uint64_t FLAGS_NODATALOSS{0x0200};
        static constexpr uint64_t FLAGS_RESYNC{0x0800};
        static constexpr uint64_t FLAGS_CLOSEDTHREAD{0x1000};
        static constexpr uint64_t FLAGS_MAXPERFORMANCE{0x2000};

        /**
         * @brief 读取器状态枚举
         * 
         * 定义了读取器可能处于的不同状态
         */
        enum class STATUS : unsigned char {
            SLEEPING,    // 休眠状态
            CHECK,       // 检查状态
            UPDATE,      // 更新状态
            READ         // 读取状态
        };

        static constexpr uint PAGE_SIZE_MAX{4096};
        static constexpr uint BAD_CDC_MAX_CNT{20};

        std::string database; // 数据库名称
        int fileCopyDes{-1};
        uint64_t fileSize{0};
        Seq fileCopySequence;
        bool hintDisplayed{false};
        bool configuredBlockSum; // 是否配置了块校验和
        bool readBlocks{false};
        bool reachedZero{false};
        std::string fileNameWrite;
        int group; // 读取器所属的组
        Seq sequence;
        typeBlk numBlocksHeader{Ctx::ZERO_BLK};
        typeResetlogs resetlogs{0};
        typeActivation activation{0};
        uint8_t* headerBuffer{nullptr}; // 头部缓冲区，用于存储读取的重做日志头数据
        uint32_t compatVsn{0};
        Time firstTimeHeader{0};
        Scn firstScn{Scn::none()};
        Scn firstScnHeader{Scn::none()};
        Scn nextScn{Scn::none()};
        Scn nextScnHeader{Scn::none()};
        Time nextTime{0};
        uint blockSize{0};                                   //!< 重做日志文件的块大小，以字节为单位
        uint64_t sumRead{0};
        uint64_t sumTime{0};
        uint64_t bufferScan{0};
        uint lastRead{0};
        time_ut lastReadTime{0};
        time_ut readTime{0};
        time_ut loopTime{0};

        std::mutex mtx; // 互斥锁，用于保护读取器的共享资源访问
        std::atomic<uint64_t> bufferStart{0};
        std::atomic<uint64_t> bufferEnd{0};
        std::atomic<STATUS> status{STATUS::SLEEPING}; // 读取器当前状态，初始为休眠状态
        std::atomic<REDO_CODE> ret{REDO_CODE::OK};
        std::condition_variable condBufferFull; // 条件变量，用于缓冲区满时通知等待线程
        std::condition_variable condReaderSleeping; // 条件变量，用于读取器休眠时的线程同步
        std::condition_variable condParserSleeping; // 条件变量，用于解析器休眠时的线程同步

        virtual void redoClose() = 0;
        virtual REDO_CODE redoOpen() = 0;
        virtual int redoRead(uint8_t* buf, uint64_t offset, uint size) = 0;
        virtual uint readSize(uint prevRead);
        virtual REDO_CODE reloadHeaderRead();
        REDO_CODE checkBlockHeader(uint8_t* buffer, typeBlk blockNumber, bool showHint);
        REDO_CODE reloadHeader();
        bool read1();
        bool read2();
        void mainLoop();

    public:
        const static char* REDO_MSG[static_cast<uint>(REDO_CODE::CNT)];
        uint8_t** redoBufferList{nullptr}; // 重做日志缓冲区列表，用于存储读取的重做日志数据
        std::vector<std::string> paths;
        std::string fileName;

        Reader(Ctx* newCtx, std::string newAlias, std::string newDatabase, int newGroup, bool newConfiguredBlockSum);
        ~Reader() override;

        void initialize();
        void wakeUp() override;
        void run() override;
        void bufferAllocate(uint num);
        void bufferFree(Thread* t, uint num);
        bool bufferIsFree();
        typeSum calcChSum(uint8_t* buffer, uint size) const;
        void printHeaderInfo(std::ostringstream& ss, const std::string& path) const;
        [[nodiscard]] uint getBlockSize() const;
        [[nodiscard]] FileOffset getBufferStart() const;
        [[nodiscard]] FileOffset getBufferEnd() const;
        [[nodiscard]] REDO_CODE getRet() const;
        [[nodiscard]] Scn getFirstScn() const;
        [[nodiscard]] Scn getFirstScnHeader() const;
        [[nodiscard]] Scn getNextScn() const;
        [[nodiscard]] Time getNextTime() const;
        [[nodiscard]] typeBlk getNumBlocks() const;
        [[nodiscard]] int getGroup() const;
        [[nodiscard]] Seq getSequence() const;
        [[nodiscard]] typeResetlogs getResetlogs() const;
        [[nodiscard]] typeActivation getActivation() const;
        [[nodiscard]] uint64_t getSumRead() const;
        [[nodiscard]] uint64_t getSumTime() const;

        void setRet(REDO_CODE newRet);
        void setBufferStartEnd(FileOffset newBufferStart, FileOffset newBufferEnd);
        bool checkRedoLog();
        bool updateRedoLog();
        void setStatusRead();
        void confirmReadData(FileOffset confirmedBufferStart);
        [[nodiscard]] bool checkFinished(Thread* t, FileOffset confirmedBufferStart);
        virtual void showHint(Thread* t, std::string origPath, std::string mappedPath) const = 0;

        std::string getName() const override {
            return {"Reader: " + fileName};
        }
    };
}

#endif
