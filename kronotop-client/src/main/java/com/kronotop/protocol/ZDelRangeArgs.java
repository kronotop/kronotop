/*
 * Copyright (c) 2023-2025 Kronotop
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

import io.lettuce.core.protocol.CommandArgs;

public class ZDelRangeArgs {
    private byte[] begin;
    private byte[] end;

    public ZDelRangeArgs begin(byte[] begin) {
        this.begin = begin;
        return this;
    }

    public ZDelRangeArgs end(byte[] end) {
        this.end = end;
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
    }

    public static class Builder {
        private Builder() {
        }

        public static ZDelRangeArgs begin(byte[] begin) {
            return new ZDelRangeArgs().begin(begin);
        }

        public static ZDelRangeArgs end(byte[] end) {
            return new ZDelRangeArgs().end(end);
        }
    }
}
