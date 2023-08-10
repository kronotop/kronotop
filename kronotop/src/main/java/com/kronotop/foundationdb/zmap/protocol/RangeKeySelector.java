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

import com.apple.foundationdb.KeySelector;

public enum RangeKeySelector {
    FIRST_GREATER_OR_EQUAL,
    FIRST_GREATER_THAN,
    LAST_LESS_THAN,
    LAST_LESS_OR_EQUAL;

    public static KeySelector getKeySelector(RangeKeySelector rangeKeySelector, byte[] key) {
        switch (rangeKeySelector) {
            case FIRST_GREATER_OR_EQUAL:
                // Default.
                return KeySelector.firstGreaterOrEqual(key);
            case FIRST_GREATER_THAN:
                return KeySelector.firstGreaterThan(key);
            case LAST_LESS_OR_EQUAL:
                return KeySelector.lastLessOrEqual(key);
            case LAST_LESS_THAN:
                return KeySelector.lastLessThan(key);
            default:
                throw new IllegalArgumentException(String.format("unknown argument: %s", rangeKeySelector));
        }
    }
}
