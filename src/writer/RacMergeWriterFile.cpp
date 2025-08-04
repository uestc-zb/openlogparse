#include <algorithm>

#include "../builder/Builder.h"
#include "../common/exception/ConfigurationException.h"
#include "../metadata/Metadata.h"
#include "RacMergeWriterFile.h"

namespace OpenLogReplicator {
    RacMergeWriterFile::RacMergeWriterFile(Ctx *newCtx, std::string newAlias, std::string newDatabase, Builder *newBuilder,
                           Metadata *newMetadata,
                           std::string newOutput, std::string newTimestampFormat, uint64_t newMaxFileSize,
                           uint64_t newNewLine, uint64_t newAppend,
                           uint newWriteBufferFlushSize) : WriterFile(newCtx, std::move(newAlias), std::move(newDatabase),
                                                                  newBuilder, newMetadata,
                                                           std::move(newOutput),
                                                           std::move(newTimestampFormat),
                                                           newMaxFileSize,
                                                           newNewLine,
                                                           newAppend,
                                                           newWriteBufferFlushSize) {
    }

    // void RacMergeWriterFile::sendMessage(BuilderMsg *msg) {
    //     // racMergeWriterFile->sendMessage(this, msg);
    // }

    void RacMergeWriterFile::sendMessage(RacWriterFile * write, BuilderMsg * msg) {
        std::unique_lock<std::mutex> const lck(mtx);
        sendMessage(msg);
        flush();
    }

    void RacMergeWriterFile::sendMessage(BuilderMsg * msg) {
        checkFile(msg->scn, msg->sequence, msg->size + newLine);

        bufferedWrite(msg->data + msg->tagSize, msg->size - msg->tagSize);
        fileSize += msg->size - msg->tagSize;

        if (newLine > 0) {
            bufferedWrite(newLineMsg, newLine);
            fileSize += newLine;
        }
    }

    void RacMergeWriterFile::run() {

    }

}
