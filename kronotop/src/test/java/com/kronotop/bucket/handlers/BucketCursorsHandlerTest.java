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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketCursorsHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldReturnEmptyCursorsWhenNoCursorsExist() {
        // Behavior: BUCKET.CURSORS returns empty maps for all operation types when no cursors exist.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.cursors().encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage response = (MapRedisMessage) msg;

        // Should have QUERY, UPDATE, DELETE keys
        RedisMessage queryMap = findInMapMessage(response, "QUERY");
        RedisMessage updateMap = findInMapMessage(response, "UPDATE");
        RedisMessage deleteMap = findInMapMessage(response, "DELETE");

        assertNotNull(queryMap);
        assertNotNull(updateMap);
        assertNotNull(deleteMap);

        // All should be empty maps
        assertInstanceOf(MapRedisMessage.class, queryMap);
        assertInstanceOf(MapRedisMessage.class, updateMap);
        assertInstanceOf(MapRedisMessage.class, deleteMap);

        assertTrue(((MapRedisMessage) queryMap).children().isEmpty());
        assertTrue(((MapRedisMessage) updateMap).children().isEmpty());
        assertTrue(((MapRedisMessage) deleteMap).children().isEmpty());
    }

    @Test
    void shouldReturnQueryCursorsWhenFilteredByOperation() {
        // Behavior: BUCKET.CURSORS QUERY returns only query cursors with cursor ID and query JSON.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Create a QUERY cursor with limit
        int cursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage queryResponse = (MapRedisMessage) msg;
            RedisMessage rawCursorId = findInMapMessage(queryResponse, "cursor_id");
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());
        }

        // Call BUCKET.CURSORS QUERY
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors("QUERY").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;

            // Should only have the QUERY key
            assertEquals(1, response.children().size());

            RedisMessage queryMap = findInMapMessage(response, "QUERY");
            assertNotNull(queryMap);
            assertInstanceOf(MapRedisMessage.class, queryMap);

            MapRedisMessage queryCursors = (MapRedisMessage) queryMap;
            assertEquals(1, queryCursors.children().size());

            // Find the cursor in the map
            boolean found = false;
            for (Map.Entry<RedisMessage, RedisMessage> entry : queryCursors.children().entrySet()) {
                assertInstanceOf(IntegerRedisMessage.class, entry.getKey());
                int id = Math.toIntExact(((IntegerRedisMessage) entry.getKey()).value());
                if (id == cursorId) {
                    found = true;
                    assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());
                }
            }
            assertTrue(found, "Cursor should be found in BUCKET.CURSORS output");
        }
    }

    @Test
    void shouldReturnAllOperationCursors() {
        // Behavior: BUCKET.CURSORS without arguments returns cursors for all operation types.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        int queryCursorId;
        int updateCursorId;
        int deleteCursorId;

        // Create QUERY cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            queryCursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Create UPDATE cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", "{\"$set\": {\"status\": \"active\"}}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            updateCursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Create DELETE cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 40}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            deleteCursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Call BUCKET.CURSORS
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors().encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;

            // Should have all three operation types
            RedisMessage queryMap = findInMapMessage(response, "QUERY");
            RedisMessage updateMap = findInMapMessage(response, "UPDATE");
            RedisMessage deleteMap = findInMapMessage(response, "DELETE");

            assertNotNull(queryMap);
            assertNotNull(updateMap);
            assertNotNull(deleteMap);

            // Verify QUERY cursor
            MapRedisMessage queryCursors = (MapRedisMessage) queryMap;
            assertEquals(1, queryCursors.children().size());
            for (Map.Entry<RedisMessage, RedisMessage> entry : queryCursors.children().entrySet()) {
                assertEquals(queryCursorId, Math.toIntExact(((IntegerRedisMessage) entry.getKey()).value()));
                assertEquals("{\"age\": {\"$gt\": 20}}",
                        ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.UTF_8));
            }

            // Verify UPDATE cursor
            MapRedisMessage updateCursors = (MapRedisMessage) updateMap;
            assertEquals(1, updateCursors.children().size());
            for (Map.Entry<RedisMessage, RedisMessage> entry : updateCursors.children().entrySet()) {
                assertEquals(updateCursorId, Math.toIntExact(((IntegerRedisMessage) entry.getKey()).value()));
                assertEquals("{\"age\": {\"$gt\": 20}}",
                        ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.UTF_8));
            }

            // Verify DELETE cursor
            MapRedisMessage deleteCursors = (MapRedisMessage) deleteMap;
            assertEquals(1, deleteCursors.children().size());
            for (Map.Entry<RedisMessage, RedisMessage> entry : deleteCursors.children().entrySet()) {
                assertEquals(deleteCursorId, Math.toIntExact(((IntegerRedisMessage) entry.getKey()).value()));
                assertEquals("{\"age\": {\"$gt\": 40}}",
                        ((FullBulkStringRedisMessage) entry.getValue()).content().toString(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void shouldReturnCursorWithQueryAsJson() {
        // Behavior: A cursor query is serialized as JSON regardless of the original format.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        int cursorId;
        String queryFilter = "{\"age\": {\"$gt\": 20}}";

        // Create the cursor with a query filter
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, queryFilter, BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            cursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Call BUCKET.CURSORS QUERY
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors("QUERY").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;

            RedisMessage queryMap = findInMapMessage(response, "QUERY");
            assertNotNull(queryMap);
            MapRedisMessage queryCursors = (MapRedisMessage) queryMap;

            // Find the cursor and verify query content
            for (Map.Entry<RedisMessage, RedisMessage> entry : queryCursors.children().entrySet()) {
                int id = Math.toIntExact(((IntegerRedisMessage) entry.getKey()).value());
                if (id == cursorId) {
                    assertInstanceOf(FullBulkStringRedisMessage.class, entry.getValue());
                    FullBulkStringRedisMessage queryMessage = (FullBulkStringRedisMessage) entry.getValue();
                    String queryContent = queryMessage.content().toString(StandardCharsets.UTF_8);
                    // Verify it contains the expected fields
                    assertTrue(queryContent.contains("age"), "Query should contain 'age' field");
                    assertTrue(queryContent.contains("$gt"), "Query should contain '$gt' operator");
                }
            }
        }
    }

    @Test
    void shouldRejectInvalidOperationType() {
        // Behavior: BUCKET.CURSORS with an invalid operation type returns an error.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.cursors("INVALID_OP").encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorResponse = (ErrorRedisMessage) msg;
        assertTrue(errorResponse.content().contains("Unknown"), "Error should indicate unknown operation");
    }

    @Test
    void shouldTrackMultipleCursorsPerOperation() {
        // Behavior: Multiple cursors for the same operation are tracked independently.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 55}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        int cursor1;
        int cursor2;

        // Create first QUERY cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$lt\": 40}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            cursor1 = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Create second QUERY cursor with different filter
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 40}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            cursor2 = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Call BUCKET.CURSORS QUERY
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors("QUERY").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;

            RedisMessage queryMap = findInMapMessage(response, "QUERY");
            assertNotNull(queryMap);
            MapRedisMessage queryCursors = (MapRedisMessage) queryMap;

            // Should have both cursors
            assertEquals(2, queryCursors.children().size(), "Should have 2 cursors tracked");

            // Verify both cursor IDs are present
            boolean foundCursor1 = false;
            boolean foundCursor2 = false;
            for (Map.Entry<RedisMessage, RedisMessage> entry : queryCursors.children().entrySet()) {
                int id = Math.toIntExact(((IntegerRedisMessage) entry.getKey()).value());
                if (id == cursor1) foundCursor1 = true;
                if (id == cursor2) foundCursor2 = true;
            }
            assertTrue(foundCursor1, "Cursor1 should be tracked");
            assertTrue(foundCursor2, "Cursor2 should be tracked");
        }
    }

    @Test
    void shouldRemoveCursorFromListAfterClose() {
        // Behavior: Closed cursors are removed from BUCKET.CURSORS output.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        int cursorId;

        // Create a QUERY cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            cursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Verify cursor appears in BUCKET.CURSORS
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors("QUERY").encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage response = (MapRedisMessage) msg;
            assertInstanceOf(MapRedisMessage.class, msg);
            RedisMessage queryMap = findInMapMessage(response, "QUERY");
            MapRedisMessage queryCursors = (MapRedisMessage) queryMap;

            assertEquals(1, queryCursors.children().size(), "Cursor should be present before close");
        }

        // Close the cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", cursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Verify cursor is removed from BUCKET.CURSORS
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors("QUERY").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            RedisMessage queryMap = findInMapMessage(response, "QUERY");
            MapRedisMessage queryCursors = (MapRedisMessage) queryMap;

            assertTrue(queryCursors.children().isEmpty(), "Cursor should be removed after close");
        }
    }

    @Test
    void shouldReturnEmptyCursorsWhenNoCursorsExist_RESP2() {
        // Behavior: BUCKET.CURSORS in RESP2 mode returns empty arrays for all operation types when no cursors exist.
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        ByteBuf buf = Unpooled.buffer();
        cmd.cursors().encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage response = (ArrayRedisMessage) msg;

        // Should have 6 elements: [QUERY, [], UPDATE, [], DELETE, []]
        List<RedisMessage> children = response.children();
        assertEquals(6, children.size());

        // Verify structure: operation name followed by an empty array
        assertInstanceOf(FullBulkStringRedisMessage.class, children.get(0));
        assertEquals("QUERY", ((FullBulkStringRedisMessage) children.get(0)).content().toString(StandardCharsets.UTF_8));
        assertInstanceOf(ArrayRedisMessage.class, children.get(1));
        assertTrue(((ArrayRedisMessage) children.get(1)).children().isEmpty());

        assertInstanceOf(FullBulkStringRedisMessage.class, children.get(2));
        assertEquals("UPDATE", ((FullBulkStringRedisMessage) children.get(2)).content().toString(StandardCharsets.UTF_8));
        assertInstanceOf(ArrayRedisMessage.class, children.get(3));
        assertTrue(((ArrayRedisMessage) children.get(3)).children().isEmpty());

        assertInstanceOf(FullBulkStringRedisMessage.class, children.get(4));
        assertEquals("DELETE", ((FullBulkStringRedisMessage) children.get(4)).content().toString(StandardCharsets.UTF_8));
        assertInstanceOf(ArrayRedisMessage.class, children.get(5));
        assertTrue(((ArrayRedisMessage) children.get(5)).children().isEmpty());
    }

    @Test
    void shouldReturnQueryCursorsWhenFilteredByOperation_RESP2() {
        // Behavior: BUCKET.CURSORS QUERY in RESP2 mode returns only query cursors as a flattened array.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Create a QUERY cursor with limit
        int cursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage queryResponse = (MapRedisMessage) msg;
            RedisMessage rawCursorId = findInMapMessage(queryResponse, "cursor_id");
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());
        }

        // Switch to RESP2 and call BUCKET.CURSORS QUERY
        switchProtocol(cmd, RESPVersion.RESP2);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors("QUERY").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage response = (ArrayRedisMessage) msg;

            // Should have 2 elements: [QUERY, [cursor_id, query_json]]
            List<RedisMessage> children = response.children();
            assertEquals(2, children.size());

            assertInstanceOf(FullBulkStringRedisMessage.class, children.get(0));
            assertEquals("QUERY", ((FullBulkStringRedisMessage) children.get(0)).content().toString(StandardCharsets.UTF_8));

            assertInstanceOf(ArrayRedisMessage.class, children.get(1));
            ArrayRedisMessage cursorsArray = (ArrayRedisMessage) children.get(1);
            assertEquals(2, cursorsArray.children().size()); // cursor_id, query_json

            assertInstanceOf(IntegerRedisMessage.class, cursorsArray.children().get(0));
            assertEquals(cursorId, Math.toIntExact(((IntegerRedisMessage) cursorsArray.children().get(0)).value()));

            assertInstanceOf(FullBulkStringRedisMessage.class, cursorsArray.children().get(1));
        }
    }

    @Test
    void shouldReturnAllOperationCursors_RESP2() {
        // Behavior: BUCKET.CURSORS in RESP2 mode returns all operation types as a flattened array.
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        int queryCursorId;
        int updateCursorId;
        int deleteCursorId;

        // Create QUERY cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            queryCursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Create UPDATE cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", "{\"$set\": {\"status\": \"active\"}}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            updateCursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Create DELETE cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gt\": 40}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            deleteCursorId = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Switch to RESP2 and call BUCKET.CURSORS
        switchProtocol(cmd, RESPVersion.RESP2);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.cursors().encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage response = (ArrayRedisMessage) msg;

            // Should have 6 elements: [QUERY, [...], UPDATE, [...], DELETE, [...]]
            List<RedisMessage> children = response.children();
            assertEquals(6, children.size());

            // Verify QUERY
            assertInstanceOf(FullBulkStringRedisMessage.class, children.get(0));
            assertEquals("QUERY", ((FullBulkStringRedisMessage) children.get(0)).content().toString(StandardCharsets.UTF_8));
            assertInstanceOf(ArrayRedisMessage.class, children.get(1));
            ArrayRedisMessage queryArray = (ArrayRedisMessage) children.get(1);
            assertEquals(2, queryArray.children().size());
            assertEquals(queryCursorId, Math.toIntExact(((IntegerRedisMessage) queryArray.children().get(0)).value()));
            String queryJson = ((FullBulkStringRedisMessage) queryArray.children().get(1)).content().toString(StandardCharsets.UTF_8);
            assertEquals("{\"age\": {\"$gt\": 20}}", queryJson);

            // Verify UPDATE
            assertInstanceOf(FullBulkStringRedisMessage.class, children.get(2));
            assertEquals("UPDATE", ((FullBulkStringRedisMessage) children.get(2)).content().toString(StandardCharsets.UTF_8));
            assertInstanceOf(ArrayRedisMessage.class, children.get(3));
            ArrayRedisMessage updateArray = (ArrayRedisMessage) children.get(3);
            assertEquals(2, updateArray.children().size());
            assertEquals(updateCursorId, Math.toIntExact(((IntegerRedisMessage) updateArray.children().get(0)).value()));
            String updateJson = ((FullBulkStringRedisMessage) updateArray.children().get(1)).content().toString(StandardCharsets.UTF_8);
            assertEquals("{\"age\": {\"$gt\": 20}}", updateJson);

            // Verify DELETE
            assertInstanceOf(FullBulkStringRedisMessage.class, children.get(4));
            assertEquals("DELETE", ((FullBulkStringRedisMessage) children.get(4)).content().toString(StandardCharsets.UTF_8));
            assertInstanceOf(ArrayRedisMessage.class, children.get(5));
            ArrayRedisMessage deleteArray = (ArrayRedisMessage) children.get(5);
            assertEquals(2, deleteArray.children().size());
            assertEquals(deleteCursorId, Math.toIntExact(((IntegerRedisMessage) deleteArray.children().get(0)).value()));
            String deleteJson = ((FullBulkStringRedisMessage) deleteArray.children().get(1)).content().toString(StandardCharsets.UTF_8);
            assertEquals("{\"age\": {\"$gt\": 40}}", deleteJson);
        }
    }
}
