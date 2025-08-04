#ifndef RAC_WRITER_FILE_H
#define RAC_WRITER_FILE_H
#include "WriterFile.h"


namespace OpenLogReplicator {
    class RacMergeWriterFile;

    class RacWriterFile : public WriterFile {
    private:
        RacMergeWriterFile *racMergeWriterFile{nullptr};

    public:
        BuilderMsg *msg{nullptr};

        RacWriterFile(Ctx *newCtx, std::string newAlias, std::string newDatabase, Builder *newBuilder,
                      Metadata *newMetadata, std::string newOutput,
                      std::string newTimestampFormat, uint64_t newMaxFileSize, uint64_t newNewLine, uint64_t newAppend,
                      uint newWiteBufferFlushSize);

        void sendMessage(BuilderMsg *msg) override;

        void setRacMergeWriterFile(RacMergeWriterFile* racMergeWriterFile);
    };
}


#endif //RAC_WRITER_FILE_H
