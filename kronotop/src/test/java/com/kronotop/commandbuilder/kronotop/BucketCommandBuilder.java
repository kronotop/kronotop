// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.commandbuilder.kronotop;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.output.StringListOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class BucketCommandBuilder<K, V> extends BaseKronotopCommandBuilder<K, V> {
    public BucketCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    @SafeVarargs
    public final Command<K, V, List<String>> insert(String bucket, V... documents) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(bucket).addValues(documents);
        return createCommand(CommandType.BUCKET_INSERT, new StringListOutput<>(codec), args);
    }

    enum CommandType implements ProtocolKeyword {
        BUCKET_INSERT("BUCKET.INSERT");

        public final byte[] bytes;

        CommandType(String name) {
            bytes = name.getBytes(StandardCharsets.US_ASCII);
        }

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }
}
