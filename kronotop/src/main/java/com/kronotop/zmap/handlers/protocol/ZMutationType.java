/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.zmap.handlers.protocol;

import com.apple.foundationdb.MutationType;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;

public enum ZMutationType {
    ADD,
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    APPEND_IF_FITS,
    MAX,
    MIN,
    SET_VERSIONSTAMPED_VALUE,
    BYTE_MIN,
    BYTE_MAX,
    COMPARE_AND_CLEAR;

    /**
     * Parses a mutation type name. The name is not case sensitive.
     *
     * @param value the mutation type name as it is written on the wire
     * @return the matching mutation type
     * @throws IllegalCommandArgumentException if the name is not a known mutation type
     */
    public static ZMutationType getValue(String value) {
        try {
            return ZMutationType.valueOf(StringUtil.toUpperCaseAscii(value));
        } catch (IllegalArgumentException ignored) {
            throw new IllegalCommandArgumentException(String.format("Unknown mutation type: '%s'", value));
        }
    }

    public static MutationType getMutationType(ZMutationType type) {
        return switch (type) {
            case ADD -> MutationType.ADD;
            case BIT_AND -> MutationType.BIT_AND;
            case BIT_OR -> MutationType.BIT_OR;
            case BIT_XOR -> MutationType.BIT_XOR;
            case APPEND_IF_FITS -> MutationType.APPEND_IF_FITS;
            case MAX -> MutationType.MAX;
            case MIN -> MutationType.MIN;
            case SET_VERSIONSTAMPED_VALUE -> MutationType.SET_VERSIONSTAMPED_VALUE;
            case BYTE_MIN -> MutationType.BYTE_MIN;
            case BYTE_MAX -> MutationType.BYTE_MAX;
            case COMPARE_AND_CLEAR -> MutationType.COMPARE_AND_CLEAR;
        };
    }
}
