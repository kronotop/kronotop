// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.commandbuilder.kronotop;

import io.lettuce.core.protocol.CommandArgs;

public class BucketInsertArgs {
    private int shard;

    public BucketInsertArgs shard(int shard) {
        this.shard = shard;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (shard > 0) {
            args.add("SHARD");
            args.add(shard);
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static BucketInsertArgs shard(int shard) {
            return new BucketInsertArgs().shard(shard);
        }
    }
}