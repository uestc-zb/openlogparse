/* Header for Replicator class
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

#ifndef REPLICATOR_H_
#define REPLICATOR_H_

#include <fstream>
#include <queue>
#include <set>
#include <unordered_map>
#include <vector>

#include "../common/Ctx.h"
#include "../common/RedoLogRecord.h"
#include "../common/Thread.h"
#include "../common/exception/RedoLogException.h"

namespace OpenLogReplicator {
    class Parser;
    class Builder;
    class Metadata;
    class Reader;
    class RedoLogRecord;
    class State;
    class Transaction;
    class TransactionBuffer;

    struct parserCompare {
        bool operator()(const Parser* p1, const Parser* p2);
    };

    class Replicator : public Thread {
    protected:
        void (* archGetLog)(Replicator* replicator);
        Builder* builder;
        Metadata* metadata;
        TransactionBuffer* transactionBuffer;
        std::string database;
        std::string redoCopyPath;
        // Redo log files
        Reader* archReader{nullptr};
        std::string lastCheckedDay;
        std::priority_queue<Parser*, std::vector<Parser*>, parserCompare> archiveRedoQueue;
        std::set<Parser*> onlineRedoSet;
        std::set<Reader*> readers;
        std::vector<std::string> pathMapping;
        std::vector<std::string> redoLogsBatch;

        void cleanArchList();
        void updateOnlineLogs();
        void readerDropAll();
        static Seq getSequenceFromFileName(Replicator* replicator, const std::string& file);
        virtual std::string getModeName() const;
        virtual bool checkConnection();
        virtual bool continueWithOnline();
        virtual void verifySchema(Scn currentScn);
        virtual void createSchema();
        virtual void updateOnlineRedoLogData();

    public:
        Replicator(Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                   TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase);
        ~Replicator() override;

        virtual void initialize();
        virtual void positionReader();
        virtual void loadDatabaseMetadata();
        void run() override;
        virtual Reader* readerCreate(int group);
        void checkOnlineRedoLogs();
        virtual void goStandby();
        void addPathMapping(std::string source, std::string target);
        void addRedoLogsBatch(std::string path);
        static void archGetLogPath(Replicator* replicator);
        static void archGetLogList(Replicator* replicator);
        void applyMapping(std::string& path);
        void updateResetlogs();
        void wakeUp() override;
        void printStartMsg() const;
        bool processArchivedRedoLogs();
        bool processOnlineRedoLogs();

        friend class OpenLogReplicator;
        friend class ReplicatorOnline;
        friend class ReplicatorRacOnline;

        std::string getName() const override {
            return {"Replicator: " + alias};
        }
    };
}

#endif
