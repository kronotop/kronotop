/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.foundationdb.zmap.protocol;

import com.apple.foundationdb.MutationType;

public enum ZMutationType {
    ADD,
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    APPEND_IF_FITS,
    MAX,
    MIN,
    SET_VERSIONSTAMPED_KEY,
    SET_VERSIONSTAMPED_VALUE,
    BYTE_MIN,
    BYTE_MAX,
    COMPARE_AND_CLEAR;

    public static MutationType getMutationType(ZMutationType type) {
        switch (type) {
            case ADD:
                return MutationType.ADD;
            case BIT_AND:
                return MutationType.BIT_AND;
            case BIT_OR:
                return MutationType.BIT_OR;
            case BIT_XOR:
                return MutationType.BIT_XOR;
            case APPEND_IF_FITS:
                return MutationType.APPEND_IF_FITS;
            case MAX:
                return MutationType.MAX;
            case MIN:
                return MutationType.MIN;
            case SET_VERSIONSTAMPED_KEY:
                return MutationType.SET_VERSIONSTAMPED_KEY;
            case SET_VERSIONSTAMPED_VALUE:
                return MutationType.SET_VERSIONSTAMPED_VALUE;
            case BYTE_MIN:
                return MutationType.BYTE_MIN;
            case COMPARE_AND_CLEAR:
                return MutationType.COMPARE_AND_CLEAR;
            default:
                throw new IllegalArgumentException(String.format("invalid ZMutationType: '%s'", type));
        }
    }

}
