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

public class Bucket extends KronotopDirectoryNode {
    public Bucket(List<String> layout) {
        super(layout);
        layout.add("bucket");
    }

    public Shard shard(int shardId) {
        return new Shard(layout, shardId);
    }

    public static class Shard extends KronotopDirectoryNode {
        public Shard(List<String> layout, int shardId) {
            super(layout);
            layout.add(Integer.toString(shardId));
        }

        public Maintenance maintenance() {
            return new Maintenance(layout);
        }

        public LastSeenVersions lastSeenVersions() {
            return new LastSeenVersions(layout);
        }

        public static class Maintenance extends KronotopDirectoryNode {
            public Maintenance(List<String> layout) {
                super(layout);
                layout.add("maintenance");
            }

            public Index index() {
                return new Index(layout);
            }

            public static class Index extends KronotopDirectoryNode {
                public Index(List<String> layout) {
                    super(layout);
                    layout.add("index");
                }

                public Tasks tasks() {
                    return new Tasks(layout);
                }

                public static class Tasks extends KronotopDirectoryNode {
                    public Tasks(List<String> layout) {
                        super(layout);
                        layout.add("tasks");
                    }
                }
            }
        }

        public static class LastSeenVersions extends KronotopDirectoryNode {
            public LastSeenVersions(List<String> layout) {
                super(layout);
                layout.add("lastSeenVersions");
            }
        }
    }
}