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

package com.kronotop.directory;

import com.kronotop.cluster.sharding.ShardKind;

import java.util.List;

public class Volumes extends KronotopDirectoryNode {

    public Volumes(List<String> layout) {
        super(layout);
        layout.add("volumes");
    }

    public Stash stash() {
        return new Stash(layout);
    }

    public Bucket bucket() {
        return new Bucket(layout);
    }

    public static class Stash extends ShardKindCommon {
        public Stash(List<String> layout) {
            super(layout, ShardKind.STASH);
        }
    }

    public static class Bucket extends ShardKindCommon {
        public Bucket(List<String> layout) {
            super(layout, ShardKind.BUCKET);
        }
    }

    private static class ShardKindCommon extends KronotopDirectoryNode {
        public ShardKindCommon(List<String> layout, ShardKind shardKind) {
            super(layout);
            layout.add(shardKind.name());
        }

        public Volume volume(String name) {
            return new Volume(layout, name);
        }

        public static class Volume extends KronotopDirectoryNode {
            public Volume(List<String> layout, String name) {
                super(layout);
                layout.add(name);
            }

            public Standby standby(String memberId) {
                return new Standby(layout, memberId);
            }

            public static class Standby extends KronotopDirectoryNode {
                public Standby(List<String> layout, String memberId) {
                    super(layout);
                    layout.add("standby");
                    layout.add(memberId);
                }
            }
        }
    }
}