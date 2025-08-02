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

package com.kronotop.commandbuilder.kronotop;

import io.lettuce.core.protocol.CommandArgs;

public class BucketQueryArgs {
    private int limit;
    private boolean reverse;
    private int shard;

    public BucketQueryArgs limit(int limit) {
        this.limit = limit;
        return this;
    }

    public BucketQueryArgs reverse() {
        reverse = true;
        return this;
    }

    public BucketQueryArgs shard(int shard) {
        this.shard = shard;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (limit > 0) {
            args.add("LIMIT");
            args.add(limit);
        }

        if (reverse) {
            args.add("REVERSE");
        }

        if (shard > 0) {
            args.add("SHARD");
            args.add(shard);
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static BucketQueryArgs limit(int limit) {
            return new BucketQueryArgs().limit(limit);
        }

        public static BucketQueryArgs reverse() {
            return new BucketQueryArgs().reverse();
        }

        public static BucketQueryArgs shard(int shard) {
            return new BucketQueryArgs().shard(shard);
        }
    }
}