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

package com.kronotop.protocol.zmap;

import io.lettuce.core.protocol.CommandArgs;

public class ZGetRangeArgs {
    private byte[] begin;
    private byte[] end;
    private int limit;
    private boolean reverse;
    private String beginKeySelector;
    private String endKeySelector;

    public ZGetRangeArgs begin(byte[] begin) {
        this.begin = begin;
        return this;
    }

    public ZGetRangeArgs end(byte[] end) {
        this.end = end;
        return this;
    }

    public ZGetRangeArgs limit(int limit) {
        this.limit = limit;
        return this;
    }

    public ZGetRangeArgs reverse() {
        this.reverse = true;
        return this;
    }

    public ZGetRangeArgs beginKeySelector(String beginKeySelector) {
        this.beginKeySelector = beginKeySelector;
        return this;
    }

    public ZGetRangeArgs endKeySelector(String endKeySelector) {
        this.endKeySelector = endKeySelector;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (begin == null) {
            throw new IllegalArgumentException("begin cannot be empty");
        }
        args.add(begin);

        if (end == null) {
            throw new IllegalArgumentException("end cannot be empty");
        }
        args.add(end);

        if (limit > 0) {
            args.add(ZGetRangeKeywords.LIMIT);
            args.add(limit);
        }

        if (reverse) {
            args.add(ZGetRangeKeywords.REVERSE);
        }

        if (beginKeySelector != null) {
            if (!beginKeySelector.isEmpty() && !beginKeySelector.isBlank()) {
                args.add(ZGetRangeKeywords.BEGIN_KEY_SELECTOR);
                args.add(beginKeySelector);
            }
        }

        if (endKeySelector != null) {
            if (!endKeySelector.isEmpty() && !endKeySelector.isBlank()) {
                args.add(ZGetRangeKeywords.END_KEY_SELECTOR);
                args.add(endKeySelector);
            }
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static ZGetRangeArgs begin(byte[] begin) {
            return new ZGetRangeArgs().begin(begin);
        }

        public static ZGetRangeArgs end(byte[] end) {
            return new ZGetRangeArgs().end(end);
        }

        public static ZGetRangeArgs limit(int limit) {
            return new ZGetRangeArgs().limit(limit);
        }

        public static ZGetRangeArgs reverse() {
            return new ZGetRangeArgs().reverse();
        }

        public static ZGetRangeArgs beginKeySelector(String beginKeySelector) {
            return new ZGetRangeArgs().beginKeySelector(beginKeySelector);
        }

        public static ZGetRangeArgs endKeySelector(String endKeySelector) {
            return new ZGetRangeArgs().endKeySelector(endKeySelector);
        }
    }
}
