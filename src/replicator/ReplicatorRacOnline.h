//
// Created by tangh on 2025/6/23.
//

#ifndef REPLICATORRACONLINE_H
#define REPLICATORRACONLINE_H
#include "../common/types/Seq.h"
#include "ReplicatorOnline.h"

namespace OpenLogReplicator {
    class ReplicatorRacOnline final : public  ReplicatorOnline {
    protected:
        int inst_id;
        bool use_asm;
        std::string sqlGetArchiveLogList() override;
        std::string sqlGetDatabaseIncarnation() override;
        std::string sqlGetDatabaseRole() override ;
        std::string sqlGetDatabaseScn() override ;
        std::string sqlGetSequenceFromScn() override ;
        std::string sqlGetSequenceFromScnStandby() override ;
        std::string sqlGetLogfileList() override ;
        std::string sqlGetParameter() const override;

    public:
        ReplicatorRacOnline(int inst_id, Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                         TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase, std::string newUser,
                         std::string newPassword, std::string newConnectString, bool newKeepConnection);
        void setAsm(bool);
        bool getAsm();
    };
}

#endif //REPLICATORRACONLINE_H


