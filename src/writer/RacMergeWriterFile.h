

#ifndef RACMERGEWRITERFILE_H
#define RACMERGEWRITERFILE_H
#include "RacWriterFile.h"
#include "WriterFile.h"


namespace OpenLogReplicator {

    class RacMergeWriterFile : public WriterFile {
    private:
        std::mutex mtx;

    public:
        BuilderMsg *msg{nullptr};

        RacMergeWriterFile(Ctx *newCtx, std::string newAlias, std::string newDatabase, Builder *newBuilder,
                      Metadata *newMetadata, std::string newOutput,
                      std::string newTimestampFormat, uint64_t newMaxFileSize, uint64_t newNewLine, uint64_t newAppend,
                      uint newWiteBufferFlushSize);

        void sendMessage(BuilderMsg *msg) override;

        void sendMessage(RacWriterFile * write, BuilderMsg * msg);

        void run() override;
    };
}



#endif //RACMERGEWRITERFILE_H
