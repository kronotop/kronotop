/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketCloseHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldCloseCursorAndPreventFurtherAccess() {
        // Step 1: Insert test documents
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"New York\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35, \"city\": \"London\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45, \"city\": \"Paris\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 28, \"city\": \"Tokyo\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 50, \"city\": \"Berlin\"}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // Insert documents
        Map<ObjectId, byte[]> insertedDocs = insertDocumentsAndGetObjectIds(testDocuments);
        assertEquals(5, insertedDocs.size(), "Should have inserted 5 documents");

        // Step 2: Run a query with limit to get a cursor
        int cursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(2)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage queryResponse = (MapRedisMessage) msg;

            // Extract cursor_id from the response
            RedisMessage rawCursorId = findInMapMessage(queryResponse, "cursor_id");
            assertNotNull(rawCursorId, "Query response should contain cursor_id field");
            assertInstanceOf(IntegerRedisMessage.class, rawCursorId);
            cursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());

            // Verify we got entries
            RedisMessage entries = findInMapMessage(queryResponse, "entries");
            assertNotNull(entries, "Query response should contain entries");
            assertInstanceOf(ArrayRedisMessage.class, entries);
            ArrayRedisMessage entriesArray = (ArrayRedisMessage) entries;
            assertEquals(2, entriesArray.children().size(), "Should have 2 entries with limit=2");
        }

        // Step 3: Close the cursor via BUCKET.CLOSE
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", cursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage closeResponse = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, closeResponse.content(), "First close should return OK");
        }

        // Step 4: Try to close the same cursor again - should return "no cursor found" error
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", cursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorResponse = (ErrorRedisMessage) msg;
            assertEquals("ERR no cursor found", errorResponse.content(),
                    "Second close attempt should return 'no cursor found' error");
        }

        // Step 5: Verify that trying to advance the closed cursor also fails
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            // Should return an error since cursor was closed
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorResponse = (ErrorRedisMessage) msg;
            assertTrue(errorResponse.content().contains("cursor") || errorResponse.content().contains("found"),
                    "Advance should fail with cursor-related error after cursor is closed");
        }
    }

    @Test
    void shouldCloseCursorsForDifferentOperations() {
        // Test closing cursors for different operations (QUERY, UPDATE, DELETE)
        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Test QUERY cursor
        int queryCursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage queryResponse = (MapRedisMessage) msg;
            assertNotNull(queryResponse);
            RedisMessage rawCursorId = findInMapMessage(queryResponse, "cursor_id");
            queryCursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());
        }

        // Close QUERY cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", queryCursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Test UPDATE cursor
        int updateCursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(TEST_BUCKET, "{\"age\": {\"$gt\": 20}}", "{\"$set\": {\"status\": \"active\"}}",
                    BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage updateResponse = (MapRedisMessage) msg;
            assertNotNull(updateResponse);
            RedisMessage rawCursorId = findInMapMessage(updateResponse, "cursor_id");
            updateCursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());
        }

        // Close UPDATE cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("UPDATE", updateCursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Try to close a non-existent cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", 99999).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            assertEquals("ERR no cursor found", ((ErrorRedisMessage) msg).content());
        }
    }

    @Test
    void shouldCloseDeleteCursor() {
        // Behavior: BUCKET.CLOSE should work for DELETE operation cursors, removing the cursor
        // and preventing further ADVANCE calls.

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Create a DELETE cursor with limit
        int deleteCursorId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{\"age\": {\"$gte\": 25}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage deleteResponse = (MapRedisMessage) msg;
            RedisMessage rawCursorId = findInMapMessage(deleteResponse, "cursor_id");
            assertNotNull(rawCursorId, "DELETE response should contain cursor_id");
            deleteCursorId = Math.toIntExact(((IntegerRedisMessage) rawCursorId).value());
        }

        // Close DELETE cursor
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("DELETE", deleteCursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Verify the cursor is closed - the second close should fail
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("DELETE", deleteCursorId).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            assertEquals("ERR no cursor found", ((ErrorRedisMessage) msg).content());
        }
    }

    @Test
    void shouldNotAffectOtherCursorsWhenClosingOne() {
        // Behavior: Closing one cursor should not affect other active cursors in the same session.
        // Each cursor is independent and can be advanced or closed separately.

        List<byte[]> testDocuments = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 35}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 45}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 55}")
        );

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        insertDocumentsAndGetObjectIds(testDocuments);

        // Create first QUERY cursor
        int cursor1;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$lt\": 40}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage response = (MapRedisMessage) msg;
            assertNotNull(response);
            cursor1 = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Create a second QUERY cursor
        int cursor2;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, "{\"age\": {\"$gte\": 40}}", BucketQueryArgs.Builder.limit(1)).encode(buf);
            Object msg = runCommand(channel, buf);

            MapRedisMessage response = (MapRedisMessage) msg;
            assertNotNull(response);
            cursor2 = Math.toIntExact(((IntegerRedisMessage) findInMapMessage(response, "cursor_id")).value());
        }

        // Close cursor1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", cursor1).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Verify cursor1 is closed
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursor1).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
        }

        // Verify cursor2 is still active - ADVANCE should succeed
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.advanceQuery(cursor2).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage response = (MapRedisMessage) msg;
            RedisMessage entries = findInMapMessage(response, "entries");
            assertNotNull(entries, "cursor2 should still return entries after cursor1 is closed");
        }

        // Close cursor2
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.close("QUERY", cursor2).encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }
    }

    @Test
    void shouldRejectInvalidOperationType() {
        // Behavior: BUCKET.CLOSE with an invalid operation type should return an error.

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.close("INVALID_OP", 123).encode(buf);
        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorResponse = (ErrorRedisMessage) msg;
        assertTrue(errorResponse.content().contains("ERR"),
                "Invalid operation type should return an error");
    }
}