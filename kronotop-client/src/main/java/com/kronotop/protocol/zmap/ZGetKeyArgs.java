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

public class ZGetKeyArgs {
    private byte[] key;
    private String keySelector;

    public ZGetKeyArgs key(byte[] key) {
        this.key = key;
        return this;
    }

    public ZGetKeyArgs keySelector(String keySelector) {
        this.keySelector = keySelector;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        args.add(key);
        if (keySelector != null) {
            if (!keySelector.isEmpty() && !keySelector.isBlank()) {
                args.add(ZGetKeyKeywords.KEY_SELECTOR);
                args.add(keySelector);
            }
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static ZGetKeyArgs key(byte[] key) {
            return new ZGetKeyArgs().key(key);
        }

        public static ZGetKeyArgs keySelector(String keySelector) {
            return new ZGetKeyArgs().keySelector(keySelector);
        }
    }
}
