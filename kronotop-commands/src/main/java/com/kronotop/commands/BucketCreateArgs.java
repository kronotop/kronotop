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

package com.kronotop.commands;

import io.lettuce.core.protocol.CommandArgs;

import java.util.List;

public class BucketCreateArgs {
    private List<Integer> shards;
    private String indexes;
    private String collation;
    private boolean ifNotExists;

    public BucketCreateArgs shards(List<Integer> shards) {
        this.shards = shards;
        return this;
    }

    public BucketCreateArgs indexes(String indexes) {
        this.indexes = indexes;
        return this;
    }

    public BucketCreateArgs collation(String collation) {
        this.collation = collation;
        return this;
    }

    public BucketCreateArgs ifNotExists() {
        this.ifNotExists = true;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (shards != null) {
            args.add("SHARDS");
            for (int shard : shards) {
                args.add(shard);
            }
        }

        if (indexes != null) {
            args.add("INDEXES");
            args.add(indexes);
        }

        if (collation != null) {
            args.add("COLLATION");
            args.add(collation);
        }

        if (ifNotExists) {
            args.add("IF-NOT-EXISTS");
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static BucketCreateArgs shards(List<Integer> shards) {
            return new BucketCreateArgs().shards(shards);
        }

        public static BucketCreateArgs indexes(String indexes) {
            return new BucketCreateArgs().indexes(indexes);
        }

        public static BucketCreateArgs collation(String collation) {
            return new BucketCreateArgs().collation(collation);
        }

        public static BucketCreateArgs ifNotExists() {
            return new BucketCreateArgs().ifNotExists();
        }
    }
}
