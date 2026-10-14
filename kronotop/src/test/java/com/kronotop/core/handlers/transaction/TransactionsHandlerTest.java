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

package com.kronotop.core.handlers.transaction;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.TickArgs;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.transaction.InstrumentedTransaction;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import com.kronotop.server.resp3.*;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransactionsHandlerTest extends BaseHandlerTest {
    private final KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

    private Map<String, RedisMessage> findById(long id) {
        ByteBuf buf = Unpooled.buffer();
        cmd.transactions().encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
            Map<String, RedisMessage> fields = toFields(child);
            if (((IntegerRedisMessage) fields.get("id")).value() == id) {
                return fields;
            }
        }
        return null;
    }

    private Map<String, RedisMessage> toFields(RedisMessage message) {
        Map<String, RedisMessage> fields = new HashMap<>();
        if (message instanceof MapRedisMessage map) {
            for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
                fields.put(string(entry.getKey()), entry.getValue());
            }
        } else {
            List<RedisMessage> flat = ((ArrayRedisMessage) message).children();
            for (int i = 0; i < flat.size(); i += 2) {
                fields.put(string(flat.get(i)), flat.get(i + 1));
            }
        }
        return fields;
    }

    private static String string(RedisMessage message) {
        return ((FullBulkStringRedisMessage) message).content().toString(io.netty.util.CharsetUtil.UTF_8);
    }

    private Session session() {
        return Session.extractSessionFromChannel(channel);
    }

    private long sessionTransactionId() {
        InstrumentedTransaction tr = (InstrumentedTransaction) session().attr(SessionAttributes.TRANSACTION).get();
        assertNotNull(tr);
        return tr.getId();
    }

    private void begin() {
        ByteBuf buf = Unpooled.buffer();
        cmd.begin().encode(buf);
        assertOK(runCommand(channel, buf));
    }

    @Test
    void shouldListInternalTransaction() {
        switchProtocol(RESPVersion.RESP3);
        try (InstrumentedTransaction tr = context.getTransactionManager().create()) {
            Map<String, RedisMessage> fields = findById(tr.getId());
            assertNotNull(fields);
            assertEquals(InstrumentedTransaction.INTERNAL, ((IntegerRedisMessage) fields.get("client_id")).value());
            assertEquals("active", string(fields.get("state")));
            assertInstanceOf(NullRedisMessage.class, fields.get("command"));
            assertInstanceOf(NullRedisMessage.class, fields.get("query"));
            assertEquals(BooleanRedisMessage.FALSE, fields.get("watching"));
        }
    }

    @Test
    void shouldListIdleBeginTransaction() {
        switchProtocol(RESPVersion.RESP3);
        begin();
        Map<String, RedisMessage> fields = findById(sessionTransactionId());
        assertNotNull(fields);
        assertEquals(session().getClientId(), ((IntegerRedisMessage) fields.get("client_id")).value());
        assertEquals("idle", string(fields.get("state")));
        assertTrue(((IntegerRedisMessage) fields.get("duration_ms")).value() >= 0);
    }

    @Test
    void shouldShowLastCommandInBeginBlock() {
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        useNamespace(channel, namespace);
        begin();

        ZMapCommandBuilder<String, String> zmap = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        zmap.zset("key", "value").encode(buf);
        assertOK(runCommand(channel, buf));

        Map<String, RedisMessage> fields = findById(sessionTransactionId());
        assertNotNull(fields);
        assertEquals("ZSET", string(fields.get("command")));
        assertEquals("ZMap", string(fields.get("model")));
        assertEquals(namespace, string(fields.get("namespace")));
        assertInstanceOf(NullRedisMessage.class, fields.get("bucket"));
        assertInstanceOf(NullRedisMessage.class, fields.get("query"));
        assertEquals("idle", string(fields.get("state")));
        assertEquals(1, ((IntegerRedisMessage) fields.get("writes")).value());
    }

    @Test
    void shouldIgnoreCommandWithOwnTransaction() {
        // Behavior: TICK opens its own transaction, so it does not replace the last command of the BEGIN block.
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        useNamespace(channel, namespace);
        begin();

        ZMapCommandBuilder<String, String> zmap = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        zmap.zset("key", "value").encode(buf);
        assertOK(runCommand(channel, buf));

        buf = Unpooled.buffer();
        cmd.tick(TickArgs.Builder.fresh()).encode(buf);
        assertInstanceOf(IntegerRedisMessage.class, runCommand(channel, buf));

        Map<String, RedisMessage> fields = findById(sessionTransactionId());
        assertNotNull(fields);
        assertEquals("ZSET", string(fields.get("command")));
        assertEquals("ZMap", string(fields.get("model")));
        assertEquals(namespace, string(fields.get("namespace")));
    }

    @Test
    void shouldShowParameterizedQuery() {
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        createBucket(channel, namespace, TEST_BUCKET);
        begin();

        Object msg = runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_QUERY,
                List.of(TEST_BUCKET, "{\"age\": {\"$gt\": 18}}"));
        assertInstanceOf(MapRedisMessage.class, msg);

        Map<String, RedisMessage> fields = findById(sessionTransactionId());
        assertNotNull(fields);
        assertEquals("BUCKET.QUERY", string(fields.get("command")));
        assertEquals("Bucket", string(fields.get("model")));
        assertEquals(namespace, string(fields.get("namespace")));
        assertEquals(TEST_BUCKET, string(fields.get("bucket")));
        assertEquals("{\"age\": {\"$gt\": ?}}", string(fields.get("query")));
    }

    @Test
    void shouldFlattenMapsForResp2() {
        begin();
        Map<String, RedisMessage> fields = findById(sessionTransactionId());
        assertNotNull(fields);
        assertEquals("idle", string(fields.get("state")));
        assertInstanceOf(FullBulkStringRedisMessage.class, fields.get("query"));
    }

    @Test
    void shouldRejectArguments() {
        Object msg = runRaw(channel, com.kronotop.commands.CommandType.TRANSACTIONS, List.of("extra"));
        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals("ERR wrong number of arguments for 'TRANSACTIONS' command", ((ErrorRedisMessage) msg).content());
    }
}
