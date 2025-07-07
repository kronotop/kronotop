// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.commandbuilder.kronotop;

import io.lettuce.core.protocol.CommandArgs;

public class BucketQueryArgs {
    private int limit;
    private boolean reverse;

    public BucketQueryArgs limit(int limit) {
        this.limit = limit;
        return this;
    }

    public BucketQueryArgs reverse() {
        reverse = true;
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
    }
}