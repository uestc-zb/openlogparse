/* Header for OpenLogReplicator
   Copyright (C) 2018-2025 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

OpenLogReplicator is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License as published
by the Free Software Foundation; either version 3, or (at your option)
any later version.

OpenLogReplicator is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License
along with OpenLogReplicator; see the file LICENSE;  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef OPEN_LOG_REPLICATOR_H_
#define OPEN_LOG_REPLICATOR_H_

#include <list>
#include <rapidjson/document.h>
#include <string>

#include "common/types/Types.h"

namespace OpenLogReplicator {
    class Builder;
    class Ctx;
    class Checkpoint;
    class Locales;
    class MemoryManager;
    class Metadata;
    class Replicator;
    class TransactionBuffer;
    class Writer;

    static const std::string WEB_CONFIG_FILE_NAME = "WebRequest";
    class OpenLogReplicator final {
    protected:
        std::vector<Replicator *> replicators;
        std::vector<Checkpoint *> checkpoints;
        std::vector<Locales *> localess;
        std::vector<Builder *> builders;
        std::vector<Metadata *> metadatas;
        std::vector<MemoryManager *> memoryManagers;
        std::vector<TransactionBuffer *> transactionBuffers;
        std::vector<Writer *> writers;
        std::vector<Replicator *> racReplicators;
        Replicator *replicator{nullptr};
        int fid{-1};
        char *configFileBuffer{nullptr};
        std::string configFileName;
        Ctx *ctx;

        void mainProcessMapping(const rapidjson::Value &readerJson);

    public:

        OpenLogReplicator(std::string newConfigFileName, Ctx *newCtx);

        OpenLogReplicator(const char *configFileBuffer, std::string newConfigFileName, Ctx *newCtx);

        ~OpenLogReplicator();

        int run();

    private:
        void readConfigFile(struct stat &configFileStat);

        void do_work(int instId, Locales *locales, struct stat configFileStat, const rapidjson::Value &sourceJson,
                     std::string alias,
                     uint64_t memoryMaxMb);

        Writer* createWriter(Replicator *replicator2, const rapidjson::Value &targetJson);
    };
}

#endif
