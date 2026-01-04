/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.cli.resp;

/**
 * RESP3 protocol data types.
 */
public enum RespType {
    BLOB_STRING('$'),
    SIMPLE_STRING('+'),
    SIMPLE_ERROR('-'),
    NUMBER(':'),
    NULL('_'),
    DOUBLE(','),
    BOOLEAN('#'),
    BLOB_ERROR('!'),
    VERBATIM_STRING('='),
    BIG_NUMBER('('),
    ARRAY('*'),
    MAP('%'),
    SET('~'),
    ATTRIBUTE('|'),
    PUSH('>'),
    // Streaming types
    STREAMED_STRING_CHUNK(';'),
    STREAMED_END('.');

    private final char prefix;

    RespType(char prefix) {
        this.prefix = prefix;
    }

    public char getPrefix() {
        return prefix;
    }

    public static RespType fromPrefix(char prefix) {
        for (RespType type : values()) {
            if (type.prefix == prefix) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown RESP3 type prefix: " + prefix);
    }
}
