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

package com.kronotop.core.handlers.session;

import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.commands.*;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.namespace.handlers.Namespace;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SessionCloseHandlerTest extends BaseHandlerTest {

    private int createQueryCursor() {
        List<byte[]> docs = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}")
        );
        insertDocuments(docs);

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(1)).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage response = (MapRedisMessage) msg;
        RedisMessage cursorIdMsg = findInMapMessage(response, "cursor_id");
        assertInstanceOf(IntegerRedisMessage.class, cursorIdMsg);
        return Math.toIntExact(((IntegerRedisMessage) cursorIdMsg).value());
    }

    private RedisMessage findInMapMessage(MapRedisMessage mapRedisMessage, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            FullBulkStringRedisMessage keyMessage = (FullBulkStringRedisMessage) entry.getKey();
            if (keyMessage.content().toString(StandardCharsets.UTF_8).equals(key)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean isQueryCursorsEmpty() {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.cursors("QUERY").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage response = (MapRedisMessage) msg;
        RedisMessage queryMap = findInMapMessage(response, "QUERY");
        assertInstanceOf(MapRedisMessage.class, queryMap);
        return ((MapRedisMessage) queryMap).children().isEmpty();
    }

    protected void insertDocuments(List<byte[]> documents) {
        createBucket(TEST_BUCKET);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = documents.toArray(new byte[0][]);
        cmd.insert(TEST_BUCKET, docs).encode(buf);
        runCommand(channel, buf);
    }

    @Test
    void shouldCloseSessionWithActiveCursors() {
        // Behavior: SESSION.CLOSE clears all active cursors.
        switchProtocol(RESPVersion.RESP3);

        int cursorId = createQueryCursor();
        assertTrue(cursorId > 0);
        assertFalse(isQueryCursorsEmpty());

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.sessionClose().encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        assertTrue(isQueryCursorsEmpty());
    }

    @Test
    void shouldCloseSessionWithActiveTransaction() {
        // Behavior: SESSION.CLOSE rolls back and closes any active FDB transaction.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        // Begin a transaction
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Close the session
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sessionClose().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Verify no active transaction (rollback should fail with no transaction)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.rollback().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            assertEquals("TRANSACTION there is no transaction in progress.", ((ErrorRedisMessage) response).content());
        }
    }

    @Test
    void shouldCloseCleanSession() {
        // Behavior: SESSION.CLOSE on a clean session returns OK without errors.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        ByteBuf buf = Unpooled.buffer();
        cmd.sessionClose().encode(buf);
        Object response = runCommand(channel, buf);

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    @Test
    void shouldResetCursorIdsAfterClose() {
        // Behavior: SESSION.CLOSE resets the cursor ID counter to 1.
        switchProtocol(RESPVersion.RESP3);

        // Create the first cursor
        int firstCursorId = createQueryCursor();
        assertEquals(1, firstCursorId);

        // Close session
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sessionClose().encode(buf);
            runCommand(channel, buf);
        }

        // Create another cursor - should start from 1 again
        switchProtocol(RESPVersion.RESP3);
        int secondCursorId = createQueryCursor();
        assertEquals(1, secondCursorId);
    }

    @Test
    void shouldResetConfigurationAttributesToDefaults() {
        // Behavior: SESSION.CLOSE resets all configuration attributes to their default values.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        // Change some session attributes
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sessionAttributeSet(SessionAttributeKeywords.LIMIT, "50").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Verify the attribute was changed
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sessionAttributeList().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage list = (ArrayRedisMessage) response;
            assertTrue(containsAttributeValue(list, "limit", 50L));
        }

        // Close session
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sessionClose().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Verify default limit (100) is restored
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.sessionAttributeList().encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage list = (ArrayRedisMessage) response;
            assertTrue(containsAttributeValue(list, "limit", 100L));
        }
    }

    @Test
    void shouldResetSnapshotReadAfterClose() {
        // Behavior: SESSION.CLOSE turns snapshot read off, so a pooled connection cannot leak it.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }
        assertTrue(channel.attr(SessionAttributes.SNAPSHOT_READ).get());

        closeSession();

        assertNull(channel.attr(SessionAttributes.SNAPSHOT_READ).get());
    }

    @Test
    void shouldKeepReadonlyFlagAfterClose() {
        // Behavior: SESSION.CLOSE keeps the readonly flag set by the READONLY command. The flag belongs
        // to the connection, so only READWRITE clears it.
        {
            // READONLY has no command builder method, so send the raw RESP frame.
            ByteBuf buf = Unpooled.buffer();
            buf.writeBytes("*1\r\n$8\r\nREADONLY\r\n".getBytes(StandardCharsets.US_ASCII));
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }
        assertTrue(channel.attr(SessionAttributes.READONLY).get());

        closeSession();

        assertTrue(channel.attr(SessionAttributes.READONLY).get());
    }

    @Test
    void shouldResetCurrentNamespaceAfterClose() {
        // Behavior: SESSION.CLOSE switches the session back to the default namespace.
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        String target = TEST_NAMESPACE + "." + namespace;

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(target).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceUse(target).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }
        assertEquals(target, currentNamespace());

        closeSession();

        assertEquals(TEST_NAMESPACE, currentNamespace());
    }

    @Test
    void shouldClearOpenNamespacesAfterClose() {
        // Behavior: SESSION.CLOSE empties the open namespace cache of the session.
        ZMapCommandBuilder<String, String> zmap = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        zmap.zset("session-close-key", "value").encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);

        Map<String, Namespace> openNamespaces = channel.attr(SessionAttributes.OPEN_NAMESPACES).get();
        assertFalse(openNamespaces.isEmpty());

        closeSession();

        assertTrue(channel.attr(SessionAttributes.OPEN_NAMESPACES).get().isEmpty());
    }

    @Test
    void shouldKeepClientAttributesAfterClose() {
        // Behavior: SESSION.CLOSE keeps the client name set with CLIENT SETNAME. Client attributes
        // belong to the connection, not to the session state that SESSION.CLOSE resets.
        RedisCommandBuilder<String, String> redis = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        redis.clientSetname("app-1").encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals("app-1", channel.attr(SessionAttributes.CLIENT_ATTRIBUTES).get().get("name"));

        closeSession();

        assertEquals("app-1", channel.attr(SessionAttributes.CLIENT_ATTRIBUTES).get().get("name"));
    }

    private void closeSession() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.sessionClose().encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private String currentNamespace() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.namespaceCurrent().encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        return ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.UTF_8);
    }

    private boolean containsAttributeValue(ArrayRedisMessage list, String attributeName, long expectedValue) {
        List<RedisMessage> children = list.children();
        for (int i = 0; i < children.size() - 1; i += 2) {
            RedisMessage keyMsg = children.get(i);
            RedisMessage valueMsg = children.get(i + 1);
            if (keyMsg instanceof FullBulkStringRedisMessage fbsm && fbsm.content().toString(StandardCharsets.UTF_8).equals(attributeName)) {
                if (valueMsg instanceof IntegerRedisMessage irm) {
                    return irm.value() == expectedValue;
                }
            }
        }
        return false;
    }
}
