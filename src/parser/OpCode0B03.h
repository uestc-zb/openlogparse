/* Redo Log OP Code 11.3
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

#ifndef OP_CODE_0B_03_H_
#define OP_CODE_0B_03_H_

namespace OpenLogReplicator {
    class OpCode0B03 final : public OpCode {
    public:
        static void process0B03(const Ctx* ctx, RedoLogRecord* redoLogRecord) {
            OpCode::process(ctx, redoLogRecord);
            typePos fieldPos = 0;
            typeField fieldNum = 0;
            typeSize fieldSize = 0;

            RedoLogRecord::nextField(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0301);
            // Field: 1
            ktbRedo(ctx, redoLogRecord, fieldPos, fieldSize);

            if (!RedoLogRecord::nextFieldOpt(ctx, redoLogRecord, fieldNum, fieldPos, fieldSize, 0x0B0302))
                return;
            // Field: 2
            kdoOpCode(ctx, redoLogRecord, fieldPos, fieldSize);
        }
    };
}

#endif
