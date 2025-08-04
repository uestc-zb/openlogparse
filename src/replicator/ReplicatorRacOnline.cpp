

#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "../common/types/Seq.h"
#include "../metadata/Metadata.h"
#include "../parser/Parser.h"
#include "../builder/Builder.h"
#include "../common/DbColumn.h"
#include "../common/DbIncarnation.h"
#include "../common/DbTable.h"
#include "../common/XmlCtx.h"
#include "../common/exception/BootException.h"
#include "../common/exception/RuntimeException.h"
#include "../common/table/SysCCol.h"
#include "../common/table/SysCDef.h"
#include "../common/table/SysCol.h"
#include "../common/table/SysDeferredStg.h"
#include "../common/table/SysECol.h"
#include "../common/table/SysLob.h"
#include "../common/table/SysLobCompPart.h"
#include "../common/table/SysLobFrag.h"
#include "../common/table/SysObj.h"
#include "../common/table/SysTab.h"
#include "../common/table/SysTabComPart.h"
#include "../common/table/SysTabPart.h"
#include "../common/table/SysTabSubPart.h"
#include "../common/table/SysTs.h"
#include "../common/table/SysUser.h"
#include "../common/table/XdbTtSet.h"
#include "../common/table/XdbXNm.h"
#include "../common/table/XdbXQn.h"
#include "../common/table/XdbXPt.h"
#include "../metadata/Metadata.h"
#include "../metadata/RedoLog.h"
#include "../metadata/Schema.h"
#include "../parser/Parser.h"
#include "../parser/TransactionBuffer.h"
#include "../reader/Reader.h"
#include "DatabaseConnection.h"
#include "DatabaseEnvironment.h"
#include "DatabaseStatement.h"
#include "ReplicatorRacOnline.h"

namespace OpenLogReplicator {
    ReplicatorRacOnline::ReplicatorRacOnline(int inst_id, Ctx* newCtx, void (* newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                                   TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase, std::string newUser,
                                   std::string newPassword, std::string newConnectString, bool newKeepConnection) :
    ReplicatorOnline(newCtx, newArchGetLog, newBuilder, newMetadata,
                                   newTransactionBuffer, newAlias, newDatabase, newUser,
                                   newPassword, newConnectString, newKeepConnection), inst_id(inst_id), use_asm(false) {

    }

    // ---------------------------
    // SQL statements
    // ---------------------------
    std::string ReplicatorRacOnline::sqlGetArchiveLogList() {
        std::string SQL_GET_RAC_ARCHIVE_LOG_LIST
                {"SELECT"
                "   NAME"
                ",  SEQUENCE#"
                ",  FIRST_CHANGE#"
                ",  NEXT_CHANGE#"
                " FROM"
                "   SYS.GV_$ARCHIVED_LOG l"
                " JOIN SYS.GV_$INSTANCE inst ON l.INST_ID = inst.INST_ID"
                " AND l.THREAD# = inst.THREAD#"
                " WHERE"
                "   SEQUENCE# >= :i"
                "   AND RESETLOGS_ID = :j"
                "   AND NAME IS NOT NULL "
                "   AND l.INST_ID = " + std::to_string(inst_id) +
                " ORDER BY"
                "   SEQUENCE#"
                ",  DEST_ID"
                ",  IS_RECOVERY_DEST_FILE DESC"};
        return SQL_GET_RAC_ARCHIVE_LOG_LIST;
    }

    std::string ReplicatorRacOnline::sqlGetDatabaseIncarnation() {
        std::string SQL_GET_RAC_DATABASE_INCARNATION
            {"SELECT"
            "   incarnation.INCARNATION#"
            ",  incarnation.RESETLOGS_CHANGE#"
            ",  incarnation.PRIOR_RESETLOGS_CHANGE#"
            ",  incarnation.STATUS"
            ",  incarnation.RESETLOGS_ID"
            ",  incarnation.PRIOR_INCARNATION#"
            " FROM"
            "   SYS.GV_$DATABASE_INCARNATION incarnation"
            "   JOIN SYS.GV_$INSTANCE inst ON incarnation.INST_ID = inst.INST_ID"
            " WHERE incarnation.INST_ID = "
            };
        return SQL_GET_RAC_DATABASE_INCARNATION + std::to_string(inst_id);
    }

    std::string ReplicatorRacOnline::sqlGetDatabaseRole(){
        std::string SQL_GET_RAC_DATABASE_ROLE
            {"SELECT"
                "   DATABASE_ROLE"
                " FROM"
                "   SYS.GV_$DATABASE"
                " WHERE"
                "   INST_ID = "
            };
        return SQL_GET_RAC_DATABASE_ROLE + std::to_string(inst_id);
    }
    std::string ReplicatorRacOnline::sqlGetDatabaseScn(){
        std::string SQL_GET_RAC_DATABASE_SCN
            {"SELECT"
                "   CURRENT_SCN"
                " FROM"
                "   SYS.GV_$DATABASE"
                " WHERE"
                "   INST_ID = "
            };
        return SQL_GET_RAC_DATABASE_SCN + std::to_string(inst_id);
    }

    std::string ReplicatorRacOnline::sqlGetSequenceFromScn() {
        std::string SQL_GET_RAC_SEQUENCE_FROM_SCN
                {"SELECT MAX(SEQUENCE#) FROM "
                "  (SELECT"
                "     SEQUENCE#,"
                "     INST_ID,"
                "     THREAD#"
                "   FROM"
                "     SYS.GV_$LOG"
                "   WHERE"
                "     FIRST_CHANGE# - 1 <= :i"
                " UNION"
                "  SELECT"
                "     SEQUENCE#,"
                "     INST_ID,"
                "     THREAD#"
                "   FROM"
                "     SYS.GV_$ARCHIVED_LOG"
                "   WHERE"
                "     FIRST_CHANGE# - 1 <= :i"
                "     AND RESETLOGS_ID = :j) l"
                "   JOIN SYS.GV_$INSTANCE inst ON l.INST_ID = inst.INST_ID AND l.THREAD# = inst.THREAD#"
                "   WHERE l.INST_ID = "};
        return SQL_GET_RAC_SEQUENCE_FROM_SCN + std::to_string(inst_id);
    }
    std::string ReplicatorRacOnline::sqlGetSequenceFromScnStandby(){
        std::string SQL_GET_RAC_SEQUENCE_FROM_SCN_STANDBY
                {"SELECT MAX(SEQUENCE#) FROM "
                "  (SELECT"
                "     SEQUENCE#,"
                "     INST_ID,"
                "     THREAD#"
                "   FROM"
                "     SYS.GV_$STANDBY_LOG"
                "   WHERE"
                "     FIRST_CHANGE# - 1 <= :i"
                " UNION"
                "  SELECT"
                "     SEQUENCE#,"
                "     INST_ID,"
                "     THREAD#"
                "   FROM"
                "     SYS.GV_$ARCHIVED_LOG"
                "   WHERE"
                "     FIRST_CHANGE# - 1 <= :i"
                "   AND RESETLOGS_ID = :j) l"
                "   JOIN SYS.GV_$INSTANCE inst ON l.INST_ID = inst.INST_ID AND l.THREAD# = inst.THREAD#"
                "   WHERE l.INST_ID = "
        };
        return SQL_GET_RAC_SEQUENCE_FROM_SCN_STANDBY + std::to_string(inst_id);
    }
    std::string ReplicatorRacOnline::sqlGetLogfileList(){
        std::string SQL_GET_RAC_LOGFILE_LIST
                {"SELECT"
                "   LF.GROUP#"
                ",  LF.MEMBER"
                " FROM"
                "   SYS.GV_$LOGFILE LF"
                "   JOIN SYS.GV_$LOG l ON LF.INST_ID = l.INST_ID AND LF.GROUP# = l.GROUP#"
                "   JOIN SYS.GV_$INSTANCE inst ON LF.INST_ID = inst.INST_ID AND l.THREAD# = inst.THREAD#"
                " WHERE"
                "   LF.TYPE = :i"
                "   AND LF.INST_ID = " + std::to_string(inst_id) +
                " ORDER BY"
                "   LF.GROUP# ASC"
                ",  LF.IS_RECOVERY_DEST_FILE DESC"
                ",  LF.MEMBER ASC"
                };
        return SQL_GET_RAC_LOGFILE_LIST;
    }
    std::string ReplicatorRacOnline::sqlGetParameter() const{
        std::string SQL_GET_RAC_PARAMETER
                {"SELECT"
                "   VALUE"
                " FROM"
                "   SYS.GV_$PARAMETER"
                " WHERE"
                "   NAME = :i"
                "   AND INST_ID = "
            };
        return SQL_GET_RAC_PARAMETER + std::to_string(inst_id);
    }
    void ReplicatorRacOnline::setAsm(bool flag) {
        use_asm = flag;
    }

    bool ReplicatorRacOnline::getAsm() {
        return use_asm;
    }
}
