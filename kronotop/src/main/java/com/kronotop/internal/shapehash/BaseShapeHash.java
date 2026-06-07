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

package com.kronotop.internal.shapehash;

import com.kronotop.bucket.bql.ast.*;

import java.util.Arrays;
import java.util.List;

public class BaseShapeHash extends FNV1A {

    // Operator type constants for hashing
    protected static final int OP_EQ = 1;
    protected static final int OP_NE = 2;
    protected static final int OP_GT = 3;
    protected static final int OP_GTE = 4;
    protected static final int OP_LT = 5;
    protected static final int OP_LTE = 6;
    protected static final int OP_IN = 7;
    protected static final int OP_NIN = 8;
    protected static final int OP_ALL = 9;
    protected static final int OP_SIZE = 10;
    protected static final int OP_EXISTS = 11;
    protected static final int OP_AND = 12;
    protected static final int OP_OR = 13;
    protected static final int OP_NOT = 14;
    protected static final int OP_ELEMMATCH = 15;

    // Value type constants for hashing
    protected static final int TYPE_STRING = 1;
    protected static final int TYPE_INT32 = 2;
    protected static final int TYPE_INT64 = 3;
    protected static final int TYPE_DOUBLE = 4;
    protected static final int TYPE_DECIMAL128 = 5;
    protected static final int TYPE_BOOLEAN = 6;
    protected static final int TYPE_NULL = 7;
    protected static final int TYPE_DATETIME = 8;
    protected static final int TYPE_TIMESTAMP = 9;
    protected static final int TYPE_BINARY = 10;
    protected static final int TYPE_ARRAY = 11;
    protected static final int TYPE_DOCUMENT = 12;
    protected static final int TYPE_VERSIONSTAMP = 13;
    protected static final int TYPE_OBJECT_ID = 14;
    protected static final int TYPE_UNKNOWN = 99;

    protected static int valueType(Object value) {
        return switch (value) {
            case StringVal ignored -> TYPE_STRING;
            case Int32Val ignored -> TYPE_INT32;
            case Int64Val ignored -> TYPE_INT64;
            case DoubleVal ignored -> TYPE_DOUBLE;
            case Decimal128Val ignored -> TYPE_DECIMAL128;
            case BooleanVal ignored -> TYPE_BOOLEAN;
            case NullVal ignored -> TYPE_NULL;
            case DateTimeVal ignored -> TYPE_DATETIME;
            case TimestampVal ignored -> TYPE_TIMESTAMP;
            case BinaryVal ignored -> TYPE_BINARY;
            case ArrayVal ignored -> TYPE_ARRAY;
            case DocumentVal ignored -> TYPE_DOCUMENT;
            case VersionstampVal ignored -> TYPE_VERSIONSTAMP;
            case ObjectIdVal ignored -> TYPE_OBJECT_ID;
            case List<?> ignored -> TYPE_ARRAY;
            case Boolean ignored -> TYPE_BOOLEAN;  // For EXISTS operand
            case Integer ignored -> TYPE_INT32;     // For SIZE operand
            case null -> TYPE_NULL;
            default -> TYPE_UNKNOWN;
        };
    }

    /**
     * Mixes list size and sorted value types into a hash.
     */
    protected static long mixListTypes(long hash, List<?> list) {
        long h = mix(hash, list.size());
        if (!list.isEmpty()) {
            int[] types = new int[list.size()];
            for (int i = 0; i < list.size(); i++) {
                types[i] = valueType(list.get(i));
            }
            Arrays.sort(types);
            for (int type : types) {
                h = mix(h, type);
            }
        }
        return h;
    }
}
