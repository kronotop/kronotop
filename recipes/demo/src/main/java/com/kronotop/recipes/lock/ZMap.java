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

package com.kronotop.recipes.lock;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

import java.nio.charset.StandardCharsets;

// Source: ZMapCommandBuilder

/**
 * A thin RESP transport over Lettuce. One instance holds one connection, which is one
 * Kronotop session, so a {@code BEGIN..COMMIT} block stays on a single transaction. Every method
 * maps one-to-one to a command in recipes/distributed-lock-on-zmap.md. ZMap and the
 * transaction commands are not part of Lettuce's built-in command set, so they are sent
 * with the generic {@code dispatch} API and a small {@link ProtocolKeyword} enum.
 */
public final class ZMap implements AutoCloseable {

    private static final ByteArrayCodec CODEC = ByteArrayCodec.INSTANCE;
    private static final byte[] RETURNING = "RETURNING".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] VERSIONSTAMP = "versionstamp".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] COMPARE_AND_CLEAR = "COMPARE_AND_CLEAR".getBytes(StandardCharsets.US_ASCII);
    private final RedisClient client;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisCommands<byte[], byte[]> sync;
    ZMap(String host, int port) {
        this.client = RedisClient.create(RedisURI.create(host, port));
        this.connection = client.connect(CODEC);
        this.sync = connection.sync();
    }

    private CommandArgs<byte[], byte[]> args() {
        return new CommandArgs<>(CODEC);
    }

    /**
     * Start an explicit transaction in this session.
     */
    void begin() {
        sync.dispatch(Keyword.BEGIN, new StatusOutput<>(CODEC), args());
    }

    /**
     * Commit the open transaction.
     */
    void commit() {
        sync.dispatch(Keyword.COMMIT, new StatusOutput<>(CODEC), args());
    }

    /**
     * Commit and return the transaction's versionstamp as raw bytes. The versionstamp
     * increases monotonically across the cluster, so the caller uses it as a fencing token.
     */
    byte[] commitReturningVersionstamp() {
        return sync.dispatch(Keyword.COMMIT, new ValueOutput<>(CODEC), args().add(RETURNING).add(VERSIONSTAMP));
    }

    /**
     * Abort the open transaction.
     */
    void rollback() {
        sync.dispatch(Keyword.ROLLBACK, new StatusOutput<>(CODEC), args());
    }

    /**
     * Read a key. Returns {@code null} when the key is missing.
     */
    byte[] zget(byte[] key) {
        return sync.dispatch(Keyword.ZGET, new ValueOutput<>(CODEC), args().addKey(key));
    }

    /**
     * Write a key.
     */
    void zset(byte[] key, byte[] value) {
        sync.dispatch(Keyword.ZSET, new StatusOutput<>(CODEC), args().addKey(key).addValue(value));
    }

    /**
     * Clear {@code key} only if its stored value still equals {@code expected}, byte for byte.
     * As an FDB atomic mutation this adds a write conflict range but no read conflict range, so
     * it never fails with NOT_COMMITTED. Issue it with no transaction open: inside a BEGIN it
     * would join that transaction and only take effect at COMMIT.
     */
    void compareAndClear(byte[] key, byte[] expected) {
        sync.dispatch(Keyword.ZMUTATE, new StatusOutput<>(CODEC),
                args().addKey(key).addValue(expected).add(COMPARE_AND_CLEAR));
    }

    @Override
    public void close() {
        connection.close();
        client.shutdown();
    }

    /**
     * Command names Kronotop understands, but Lettuce does not know natively.
     */
    private enum Keyword implements ProtocolKeyword {
        BEGIN, COMMIT, ROLLBACK, ZGET, ZSET, ZMUTATE;

        private final byte[] bytes = name().getBytes(StandardCharsets.US_ASCII);

        @Override
        public byte[] getBytes() {
            return bytes;
        }
    }
}
