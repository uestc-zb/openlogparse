#include <algorithm>

#include "../builder/Builder.h"
#include "../common/exception/ConfigurationException.h"
#include "../metadata/Metadata.h"
#include "RacWriterFile.h"
#include "RacMergeWriterFile.h"

namespace OpenLogReplicator {
    RacWriterFile::RacWriterFile(Ctx *newCtx, std::string newAlias, std::string newDatabase, Builder *newBuilder,
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


    void RacWriterFile::sendMessage(BuilderMsg *msg) {
        racMergeWriterFile->sendMessage(this, msg);
        this->confirmMessage(msg);
    }

    void RacWriterFile::setRacMergeWriterFile(RacMergeWriterFile* racMergeWriterFile) {
        this->racMergeWriterFile = racMergeWriterFile;
    }
}
