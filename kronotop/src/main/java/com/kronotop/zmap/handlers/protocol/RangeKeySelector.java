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

import com.apple.foundationdb.KeySelector;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;

public enum RangeKeySelector {
    FIRST_GREATER_OR_EQUAL,
    FIRST_GREATER_THAN,
    LAST_LESS_THAN,
    LAST_LESS_OR_EQUAL;

    public static KeySelector getKeySelector(RangeKeySelector selector, byte[] key) {
        return switch (selector) {
            case FIRST_GREATER_OR_EQUAL ->
                // Default.
                    KeySelector.firstGreaterOrEqual(key);
            case FIRST_GREATER_THAN -> KeySelector.firstGreaterThan(key);
            case LAST_LESS_OR_EQUAL -> KeySelector.lastLessOrEqual(key);
            case LAST_LESS_THAN -> KeySelector.lastLessThan(key);
        };
    }

    public static RangeKeySelector getValue(String value) {
        try {
            return RangeKeySelector.valueOf(StringUtil.toUpperCaseAscii(value));
        } catch (IllegalArgumentException ignored) {
            throw new IllegalCommandArgumentException(String.format("Unknown range key selector: '%s'", value));
        }
    }
}
