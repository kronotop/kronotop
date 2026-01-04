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

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents a RESP3 protocol value.
 */
public sealed interface RespValue {

    RespType type();

    // Simple types

    record BlobString(String value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.BLOB_STRING;
        }
    }

    record SimpleString(String value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.SIMPLE_STRING;
        }
    }

    record SimpleError(String code, String message) implements RespValue {
        @Override
        public RespType type() {
            return RespType.SIMPLE_ERROR;
        }
    }

    record Number(long value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.NUMBER;
        }
    }

    record Null() implements RespValue {
        @Override
        public RespType type() {
            return RespType.NULL;
        }
    }

    record Double(double value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.DOUBLE;
        }
    }

    record Boolean(boolean value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.BOOLEAN;
        }
    }

    record BlobError(String code, String message) implements RespValue {
        @Override
        public RespType type() {
            return RespType.BLOB_ERROR;
        }
    }

    record VerbatimString(String format, String value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.VERBATIM_STRING;
        }
    }

    record BigNumber(BigInteger value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.BIG_NUMBER;
        }
    }

    // Aggregate types

    record Array(List<RespValue> values) implements RespValue {
        @Override
        public RespType type() {
            return RespType.ARRAY;
        }
    }

    record RespMap(Map<RespValue, RespValue> values) implements RespValue {
        @Override
        public RespType type() {
            return RespType.MAP;
        }
    }

    record RespSet(Set<RespValue> values) implements RespValue {
        @Override
        public RespType type() {
            return RespType.SET;
        }
    }

    record Attribute(Map<RespValue, RespValue> attributes, RespValue value) implements RespValue {
        @Override
        public RespType type() {
            return RespType.ATTRIBUTE;
        }
    }

    record Push(String kind, List<RespValue> values) implements RespValue {
        @Override
        public RespType type() {
            return RespType.PUSH;
        }
    }
}
