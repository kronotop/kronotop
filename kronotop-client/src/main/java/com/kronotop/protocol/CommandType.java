/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.protocol;

import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

public enum CommandType implements ProtocolKeyword {
    AUTH,
    BEGIN,
    ROLLBACK,
    COMMIT,
    NAMESPACE,
    ZSET,
    ZGET,
    ZDEL,
    ZDELRANGE,
    ZGETRANGE,
    ZGETKEY,
    SNAPSHOTREAD,
    ZMUTATE,
    ZGETRANGESIZE,
    GETAPPROXIMATESIZE,
    GETREADVERSION,
    ZINC_I64("ZINC.I64"),
    ZGET_I64("ZGET.I64"),
    ZINC_F64("ZINC.F64"),
    ZGET_F64("ZGET.F64"),
    ZINC_D128("ZINC.D128"),
    ZGET_D128("ZGET.D128");

    public final byte[] bytes;

    CommandType() {
        bytes = name().getBytes(StandardCharsets.US_ASCII);
    }

    CommandType(String command) {
        bytes = command.getBytes(StandardCharsets.US_ASCII);
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }
}