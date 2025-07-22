// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

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

        public Index index(String bucket, String index) {
            return new Index(layout, bucket, index);
        }
    }
}