/* Redo Log OP Code 11.11
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

#include "../common/RedoLogRecord.h"
#include "OpCode.h"

#ifndef OP_CODE_0B_0B_H_
#define OP_CODE_0B_0B_H_

namespace OpenLogReplicator {
    class OpCode0B0B final : public OpCode {
    public:
        static void process0B0B(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            OpCode::process(ctx, redoLogRecord);
            typePos fieldPos = 0;
            typeField fieldNum = 0;
            typeSize fieldSize = 0;

            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0B01);
            // Field: 1
            ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0B02))
                return;
            // Field: 2
            kdoOpCode(ctx, redoLogRecord, fieldPos, fieldSize);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0B03))
                return;
            // Field: 3
            redoLogRecord->rowSizesDelta = fieldPos;
            if (unlikely(fieldSize < static_cast<typeSize>(redoLogRecord->nRow) * 2))
                throw RedoLogException(50061, "too short field 11.11.3: " + std::to_string(fieldSize) + " offset: " + redoLogRecord->fileOffset.toString());

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0B04))
                return;
            // Field: 4
            redoLogRecord->rowData = fieldNum;
            dumpRows(ctx, redoLogRecord, redoLogRecord->data(fieldPos));
        }
    };
}

#endif
