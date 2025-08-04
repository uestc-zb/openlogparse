/* Class to handle character set AL32UTF8
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

#include "CharacterSetAL32UTF8.h"

namespace OpenLogReplicator {
    CharacterSetAL32UTF8::CharacterSetAL32UTF8() :
            CharacterSet("AL32UTF8") {
    }

    typeUnicode CharacterSetAL32UTF8::decode(const Ctx* ctx, Xid xid, const uint8_t*& str, uint64_t& length) const {
        const uint64_t byte1 = *str++;
        --length;

        // 0xxxxxxx
        if ((byte1 & 0x80) == 0)
            return byte1;

        if (length == 0)
            return badChar(ctx, xid, byte1);

        const uint64_t byte2 = *str++;
        --length;

        if ((byte2 & 0xC0) != 0x80)
            return badChar(ctx, xid, byte1, byte2);

        // 110xxxxx 10xxxxxx
        if ((byte1 & 0xE0) == 0xC0)
            return ((byte1 & 0x1F) << 6) | (byte2 & 0x3F);

        if (length == 0)
            return badChar(ctx, xid, byte1, byte2);

        const uint64_t byte3 = *str++;
        --length;

        if ((byte3 & 0xC0) != 0x80)
            return badChar(ctx, xid, byte1, byte2, byte3);

        // 1110xxxx 10xxxxxx 10xxxxxx
        if ((byte1 & 0xF0) == 0xE0)
            return ((byte1 & 0x0F) << 12) | ((byte2 & 0x3F) << 6) | (byte3 & 0x3F);

        if (length == 0)
            return badChar(ctx, xid, byte1, byte2, byte3);

        const uint64_t byte4 = *str++;
        --length;

        if ((byte4 & 0xC0) != 0x80)
            return badChar(ctx, xid, byte1, byte2, byte3, byte4);

        // 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
        if ((byte1 & 0xF8) == 0xF0) {
            const typeUnicode character = ((byte1 & 0x07) << 18) | ((byte2 & 0x3F) << 12) | ((byte3 & 0x3F) << 6) | (byte4 & 0x3F);
            if (character <= 0x10FFFF && (character < 0xD800 || character > 0xDFFF))
                return character;
        }

        return badChar(ctx, xid, byte1, byte2, byte3, byte4);
    }
}
