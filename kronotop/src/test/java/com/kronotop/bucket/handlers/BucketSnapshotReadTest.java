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
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketQueryArgs;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SnapshotReadArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketSnapshotReadTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    private void enableSnapshotRead(KronotopCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
    }

    private void disableSnapshotRead(KronotopCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.snapshotRead(SnapshotReadArgs.Builder.off()).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
    }

    private void beginTransaction(KronotopCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.begin().encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
    }

    private void commitTransaction(KronotopCommandBuilder<String, String> cmd) {
        ByteBuf buf = Unpooled.buffer();
        cmd.commit().encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
    }

    @Test
    void shouldQueryWithSnapshotReadInAutoCommitMode() {
        // Behavior: SNAPSHOTREAD ON returns correct results in auto-commit mode.

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(documents);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        enableSnapshotRead(cmd);

        // Query with snapshot read ON
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
        }

        disableSnapshotRead(cmd);

        // Query with snapshot read OFF — same results
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
        }
    }

    @Test
    void shouldQueryWithSnapshotReadInExplicitTransaction() {
        // Behavior: SNAPSHOTREAD ON within BEGIN/COMMIT reads committed data correctly.

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(documents);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        beginTransaction(cmd);
        enableSnapshotRead(cmd);

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
        }

        commitTransaction(cmd);
    }

    @Test
    void shouldQueryWithSnapshotReadAndLimit() {
        // Behavior: SNAPSHOTREAD ON respects LIMIT correctly.

        List<byte[]> documents = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            documents.add(BSONUtil.jsonToDocumentThenBytes(String.format("{\"seq\": %d}", i)));
        }
        insertDocumentsAndGetObjectIds(documents);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        enableSnapshotRead(cmd);

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}", BucketQueryArgs.Builder.limit(3)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(3, entries.size());
        }
    }

    @Test
    void shouldQueryWithSnapshotReadUsingSecondaryIndex() {
        // Behavior: SNAPSHOTREAD ON works with secondary index scans (IndexScanNode path).

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age-index", "age", BsonType.INT32, false, IndexStatus.WAITING);
        createIndexThenWaitForReadiness(ageIndex);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(documents);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        enableSnapshotRead(cmd);

        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{\"age\": {\"$eq\": 30}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
            for (BsonDocument doc : entries) {
                assertEquals(30, doc.getInt32("age").getValue());
            }
        }
    }

    @Test
    void shouldInsertIgnoreSnapshotRead() {
        // Behavior: SNAPSHOTREAD ON does not affect INSERT behavior.

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        enableSnapshotRead(cmd);

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}")
        );
        Map<ObjectId, byte[]> inserted = insertDocumentsAndGetObjectIds(documents);
        assertEquals(2, inserted.size());

        // Verify the documents are persisted by querying
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
        }
    }

    @Test
    void shouldDeleteIgnoreSnapshotRead() {
        // Behavior: SNAPSHOTREAD ON does not affect DELETE behavior.

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(documents);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        enableSnapshotRead(cmd);

        // Delete all documents
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.delete(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> deletedIds = extractObjectIds(msg);
            assertEquals(2, deletedIds.size());
        }

        // Verify documents are gone
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertTrue(entries.isEmpty());
        }
    }

    @Test
    void shouldUpdateIgnoreSnapshotRead() {
        // Behavior: SNAPSHOTREAD ON does not affect UPDATE behavior.

        List<byte[]> documents = Arrays.asList(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(documents);

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        BucketCommandBuilder<String, String> bucketCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(bucketCmd, RESPVersion.RESP3);

        enableSnapshotRead(cmd);

        // Update all documents to set status field
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"status\": \"active\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<ObjectId> updatedIds = extractObjectIds(msg);
            assertEquals(2, updatedIds.size());
        }

        // Verify documents are updated
        {
            ByteBuf buf = Unpooled.buffer();
            bucketCmd.query(TEST_BUCKET, "{}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);

            List<BsonDocument> entries = extractEntries(msg);
            assertEquals(2, entries.size());
            for (BsonDocument doc : entries) {
                assertEquals("active", doc.getString("status").getValue());
            }
        }
    }
}
