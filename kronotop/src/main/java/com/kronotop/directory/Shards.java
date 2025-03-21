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

package com.kronotop.directory;

import java.util.List;

public class Shards extends KronotopDirectoryNode {

    public Shards(List<String> layout) {
        super(layout);
        layout.add("shards");
    }

    public Redis redis() {
        return new Redis(layout);
    }

    public Bucket bucket() {
        return new Bucket(layout);
    }

    public static class Redis extends Common {
        public Redis(List<String> layout) {
            super(layout, "redis");
        }
    }

    public static class Bucket extends Common {
        public Bucket(List<String> layout) {
            super(layout, "bucket");
        }
    }

    static class Common extends KronotopDirectoryNode {
        public Common(List<String> layout, String name) {
            super(layout);
            layout.add(name);
        }

        public Shard shard(int shardId) {
            return new Shard(layout, shardId);
        }

        public static class Shard extends KronotopDirectoryNode {
            public Shard(List<String> layout, int shardId) {
                super(layout);
                layout.add(Integer.toString(shardId));
            }
        }
    }
}
