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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.CommandType;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.InstrumentedTransaction;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TransactionHandlerTest extends BaseHandlerTest {
    private final KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
    private final ZMapCommandBuilder<String, String> zmap = new ZMapCommandBuilder<>(StringCodec.ASCII);

    private static String string(RedisMessage message) {
        return ((FullBulkStringRedisMessage) message).content().toString(CharsetUtil.UTF_8);
    }

    private Map<String, RedisMessage> findById(long id) {
        ByteBuf buf = Unpooled.buffer();
        cmd.transactionList().encode(buf);
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

    private Map<String, RedisMessage> findSlowById(long id) {
        ByteBuf buf = Unpooled.buffer();
        cmd.transactionSlow().encode(buf);
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

    // Newest slow transaction of this session that ran the command, or null.
    private Map<String, RedisMessage> findSlowByCommand(String command) {
        ByteBuf buf = Unpooled.buffer();
        cmd.transactionSlow().encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        for (RedisMessage child : ((ArrayRedisMessage) msg).children()) {
            Map<String, RedisMessage> fields = toFields(child);
            if (((IntegerRedisMessage) fields.get("client_id")).value() == session().getClientId()
                    && fields.get("command") instanceof FullBulkStringRedisMessage
                    && command.equals(string(fields.get("command")))) {
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

    private Object kill(long id) {
        ByteBuf buf = Unpooled.buffer();
        cmd.transactionKill(id).encode(buf);
        return runCommand(channel, buf);
    }

    private Object zset() {
        ByteBuf buf = Unpooled.buffer();
        zmap.zset("key", "value").encode(buf);
        return runCommand(channel, buf);
    }

    private Object zget() {
        ByteBuf buf = Unpooled.buffer();
        zmap.zget("key").encode(buf);
        return runCommand(channel, buf);
    }

    private void assertError(Object msg, String content) {
        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals(content, ((ErrorRedisMessage) msg).content());
    }

    @Test
    void shouldKillBeginTransaction() {
        createNamespace(channel, namespace);
        useNamespace(channel, namespace);

        begin();
        assertOK(zset());
        long id = sessionTransactionId();

        assertOK(kill(id));
        assertNotNull(context.getTransactionManager().get(id));

        // A write is buffered on the client side. A read hits FoundationDB and fails.
        assertError(zget(), "TRANSACTION_CANCELLED Operation aborted because the transaction was cancelled");

        ByteBuf buf = Unpooled.buffer();
        cmd.rollback().encode(buf);
        assertOK(runCommand(channel, buf));
        assertNull(context.getTransactionManager().get(id));
    }

    @Test
    void shouldRejectUnknownId() {
        assertError(kill(999999), "TRANSACTION no such transaction: 999999");
    }

    @Test
    void shouldRejectInternalTransaction() {
        try (InstrumentedTransaction tr = context.getTransactionManager().create()) {
            assertError(kill(tr.getId()), "TRANSACTION internal transactions cannot be killed");
            assertSame(tr, context.getTransactionManager().get(tr.getId()));
        }
    }

    @Test
    void shouldRejectUnknownSubcommand() {
        assertError(runRaw(channel, CommandType.TRANSACTION, List.of("FOO")), "ERR unknown subcommand: 'FOO'");
    }

    @Test
    void shouldRejectMissingId() {
        assertError(runRaw(channel, CommandType.TRANSACTION, List.of("KILL")),
                "ERR wrong number of arguments for 'TRANSACTION KILL' command");
    }

    @Test
    void shouldListInternalTransaction() {
        switchProtocol(RESPVersion.RESP3);
        try (InstrumentedTransaction tr = context.getTransactionManager().create()) {
            Map<String, RedisMessage> fields = findById(tr.getId());
            assertNotNull(fields);
            assertEquals(InstrumentedTransaction.INTERNAL, ((IntegerRedisMessage) fields.get("client_id")).value());
            assertEquals("active", string(fields.get("state")));
            assertEquals(0, ((IntegerRedisMessage) fields.get("idle_ms")).value());
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
        assertTrue(((IntegerRedisMessage) fields.get("idle_ms")).value() >= 0);
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
    void shouldListOwnTransactionOfCommand() {
        // Behavior: a command that opens its own transaction with create(session) is listed with the client id
        // and the command, and the BEGIN transaction keeps the last command that ran on it.
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        useNamespace(channel, namespace);
        begin();

        ZMapCommandBuilder<String, String> zmap = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        zmap.zset("key", "value").encode(buf);
        assertOK(runCommand(channel, buf));

        session().startCommand("NAMESPACE.LIST", null);
        try (InstrumentedTransaction own = context.getTransactionManager().create(session())) {
            session().setNamespace(namespace);

            Map<String, RedisMessage> ownFields = findById(own.getId());
            assertNotNull(ownFields);
            assertEquals(session().getClientId(), ((IntegerRedisMessage) ownFields.get("client_id")).value());
            assertEquals("NAMESPACE.LIST", string(ownFields.get("command")));
            assertInstanceOf(NullRedisMessage.class, ownFields.get("model"));
            assertEquals(namespace, string(ownFields.get("namespace")));

            Map<String, RedisMessage> beginFields = findById(sessionTransactionId());
            assertNotNull(beginFields);
            assertEquals("ZSET", string(beginFields.get("command")));
            assertEquals("ZMap", string(beginFields.get("model")));
        }
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
    void shouldShowBucketOfInsertInBeginBlock() {
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        createBucket(channel, namespace, TEST_BUCKET);
        useNamespace(channel, namespace);
        begin();

        BucketCommandBuilder<byte[], byte[]> bucket = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        bucket.insert(TEST_BUCKET, List.of(BSONUtil.jsonToDocumentThenBytes("{\"age\": 18}"))).encode(buf);
        Object msg = runCommand(channel, buf);
        assertNotNull(msg);
        assertInstanceOf(ArrayRedisMessage.class, msg, msg.toString());

        Map<String, RedisMessage> fields = findById(sessionTransactionId());
        assertNotNull(fields);
        assertEquals("BUCKET.INSERT", string(fields.get("command")));
        assertEquals("Bucket", string(fields.get("model")));
        assertEquals(namespace, string(fields.get("namespace")));
        assertEquals(TEST_BUCKET, string(fields.get("bucket")));
        assertInstanceOf(NullRedisMessage.class, fields.get("query"));
    }

    @Test
    void shouldLogBucketOfOwnTransaction() {
        // Behavior: a command that opens its own transaction with create(session) records its bucket.
        // test.conf sets the slow log threshold to 0, so the closed transaction is in TRANSACTION SLOW.
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        createBucket(channel, namespace, TEST_BUCKET);

        Map<String, RedisMessage> fields = findSlowByCommand("BUCKET.CREATE");
        assertNotNull(fields);
        assertEquals("Bucket", string(fields.get("model")));
        assertEquals(namespace, string(fields.get("namespace")));
        assertEquals(TEST_BUCKET, string(fields.get("bucket")));
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
        assertError(runRaw(channel, CommandType.TRANSACTION, List.of("LIST", "extra")),
                "ERR wrong number of arguments for 'TRANSACTION LIST' command");
    }

    @Test
    void shouldLogSlowCommandTransaction() {
        // test.conf sets the slow log threshold to 0, so every closed transaction is logged.
        switchProtocol(RESPVersion.RESP3);
        createNamespace(channel, namespace);
        useNamespace(channel, namespace);
        begin();
        assertOK(zset());
        long id = sessionTransactionId();

        ByteBuf buf = Unpooled.buffer();
        cmd.commit().encode(buf);
        assertOK(runCommand(channel, buf));

        Map<String, RedisMessage> fields = findSlowById(id);
        assertNotNull(fields);
        assertEquals(session().getClientId(), ((IntegerRedisMessage) fields.get("client_id")).value());
        assertEquals("ZSET", string(fields.get("command")));
        assertEquals("ZMap", string(fields.get("model")));
        assertEquals(namespace, string(fields.get("namespace")));
        assertEquals(BooleanRedisMessage.TRUE, fields.get("committed"));
        assertEquals(BooleanRedisMessage.FALSE, fields.get("conflicted"));
        assertEquals(1, ((IntegerRedisMessage) fields.get("writes")).value());
        assertTrue(((IntegerRedisMessage) fields.get("timestamp")).value() > 0);
        assertTrue(((IntegerRedisMessage) fields.get("commit_latency_ms")).value() >= 0);
        assertNull(findById(id));
    }

    @Test
    void shouldLogInternalTransaction() {
        switchProtocol(RESPVersion.RESP3);
        InstrumentedTransaction tr = context.getTransactionManager().create();
        tr.close();

        Map<String, RedisMessage> fields = findSlowById(tr.getId());
        assertNotNull(fields);
        assertEquals(InstrumentedTransaction.INTERNAL, ((IntegerRedisMessage) fields.get("client_id")).value());
        assertInstanceOf(NullRedisMessage.class, fields.get("command"));
        assertInstanceOf(NullRedisMessage.class, fields.get("query"));
        assertEquals(BooleanRedisMessage.FALSE, fields.get("committed"));
    }

    @Test
    void shouldReturnNewestFirst() {
        switchProtocol(RESPVersion.RESP3);
        InstrumentedTransaction first = context.getTransactionManager().create();
        first.close();
        InstrumentedTransaction second = context.getTransactionManager().create();
        second.close();

        ByteBuf buf = Unpooled.buffer();
        cmd.transactionSlow().encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        // Background tasks may log their own transactions, so compare positions instead of indexes.
        int firstIndex = -1;
        int secondIndex = -1;
        List<RedisMessage> children = ((ArrayRedisMessage) msg).children();
        for (int i = 0; i < children.size(); i++) {
            long id = ((IntegerRedisMessage) toFields(children.get(i)).get("id")).value();
            if (id == first.getId()) {
                firstIndex = i;
            } else if (id == second.getId()) {
                secondIndex = i;
            }
        }
        assertTrue(secondIndex >= 0);
        assertTrue(secondIndex < firstIndex);
    }

    @Test
    void shouldRejectSlowArguments() {
        assertError(runRaw(channel, CommandType.TRANSACTION, List.of("SLOW", "extra")),
                "ERR wrong number of arguments for 'TRANSACTION SLOW' command");
    }
}
