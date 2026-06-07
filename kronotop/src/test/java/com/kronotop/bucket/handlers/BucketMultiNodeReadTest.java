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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.TestUtil;
import com.kronotop.bucket.*;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeEntry;
import com.kronotop.volume.VolumeSession;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketMultiNodeReadTest extends BaseBucketMultiNodeTest {

    @Override
    protected boolean runWithTCPServer() {
        return true;
    }

    @Test
    void shouldQueryDocumentsAcrossMultipleShards() {
        // Behavior: A QUERY on node1 returns documents from both local (shard 0) and remote
        // (shard 4) shards. The remote shard's documents are fetched via SEGMENT.RANGE RPC
        // over TCP to node2.

        String bucketName = "multi-shard-bucket";

        // Create a bucket with shards 0 (node1) and 4 (node2)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(bucketName, BucketCreateArgs.Builder.shards(List.of(0, 4))).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail(err.content());
            }
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Switch node1's channel to RESP3
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hello(3).encode(buf);
            runCommand(node1.getChannel(), buf);
        }

        // Switch node2's channel to RESP3
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hello(3).encode(buf);
            runCommand(node2.getChannel(), buf);
        }

        // INSERT {"name": "Alice"} on node1 → shard 0 (local to node1)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}");
            insertCmd.insert(bucketName, document).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("INSERT on node1 failed: " + err.content());
            }
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // INSERT {"name": "Bob"} on node2 → shard 4 (local to node2)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}");
            insertCmd.insert(bucketName, document).encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("INSERT on node2 failed: " + err.content());
            }
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // QUERY "{}" on node1 — should return documents from both shards
        {
            BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("QUERY on node1 failed: " + err.content());
            }

            List<BsonDocument> entries = extractEntries(response);
            assertEquals(2, entries.size());

            Set<String> names = new HashSet<>();
            for (BsonDocument doc : entries) {
                names.add(doc.getString("name").getValue());
            }
            assertTrue(names.contains("Alice"), "Expected Alice in results");
            assertTrue(names.contains("Bob"), "Expected Bob in results");
        }
    }

    @Test
    void shouldMigrateDocumentToLocalShardOnUpdate() {
        // Behavior: When node1 executes UPDATE on a document that lives on a remote shard (shard 4,
        // owned by node2), the document must migrate to a local shard on node1. The updated document
        // is inserted into a local shard's Volume, dropped from the remote shard's Volume, and all
        // index entries are updated with the new shardId.

        String bucketName = "multi-shard-update-bucket";

        // Create a bucket with shards 0 (node1) and 4 (node2)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(bucketName, BucketCreateArgs.Builder.shards(List.of(0, 4))).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail(err.content());
            }
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Create a secondary index on the "name" field and wait for it to become READY
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(bucketName, "{\"name\": {\"bson_type\": \"string\"}}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("INDEX CREATE failed: " + err.content());
            }
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }
        {
            String namespace = node1.getContext().getConfig().getString("default_namespace");
            await().atMost(15, TimeUnit.SECONDS).until(() -> {
                try (Transaction tr = node1.getContext().getFoundationDB().createTransaction()) {
                    BucketMetadata metadata = BucketMetadataUtil.reload(node1.getContext(), tr, namespace, bucketName);
                    Index index = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
                    return index != null && index.definition().status() == IndexStatus.READY;
                }
            });
        }

        // Switch both nodes to RESP3
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hello(3).encode(buf);
            runCommand(node1.getChannel(), buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hello(3).encode(buf);
            runCommand(node2.getChannel(), buf);
        }

        // INSERT {"name": "Alice", "age": 25} on node1 → shard 0 (local to node1)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}");
            insertCmd.insert(bucketName, document).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("INSERT Alice on node1 failed: " + err.content());
            }
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // INSERT {"name": "Bob", "age": 30} on node2 → shard 4 (local to node2)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}");
            insertCmd.insert(bucketName, document).encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("INSERT Bob on node2 failed: " + err.content());
            }
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // UPDATE Alice on node1 — local update, shard 0 stays
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(bucketName, "{\"name\": \"Alice\"}", "{\"$set\": {\"age\": 26}}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("UPDATE Alice on node1 failed: " + err.content());
            }
            List<ObjectId> updatedIds = extractObjectIds(response);
            assertEquals(1, updatedIds.size(), "Expected 1 object updated (Alice)");
        }

        // UPDATE Bob on node1 — Bob lives on shard 4 (node2), should migrate to shard 0 (node1)
        ObjectId bobObjectId;
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(bucketName, "{\"name\": \"Bob\"}", "{\"$set\": {\"age\": 31}}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("UPDATE Bob on node1 failed: " + err.content());
            }
            List<ObjectId> updatedIds = extractObjectIds(response);
            assertEquals(1, updatedIds.size(), "Expected 1 object updated (Bob)");
            bobObjectId = updatedIds.getFirst();
        }

        // QUERY "{}" on node1 — should return both documents with updated ages
        {
            BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("QUERY on node1 failed: " + err.content());
            }

            List<BsonDocument> entries = extractEntries(response);
            assertEquals(2, entries.size(), "Expected 2 documents in query result");

            for (BsonDocument doc : entries) {
                String name = doc.getString("name").getValue();
                int age = doc.getInt32("age").getValue();
                if (name.equals("Alice")) {
                    assertEquals(26, age, "Alice's age should be updated to 26");
                } else if (name.equals("Bob")) {
                    assertEquals(31, age, "Bob's age should be updated to 31");
                } else {
                    fail("Unexpected document with name: " + name);
                }
            }
        }

        // Verify at the index level: Bob's primary index entry should point to a local shard (0-3)
        try (Transaction tr = node1.getContext().getFoundationDB().createTransaction()) {
            String namespace = node1.getContext().getConfig().getString("default_namespace");
            BucketMetadata metadata = BucketMetadataUtil.open(node1.getContext(), tr, namespace, bucketName);

            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
            assertNotNull(primaryIndex, "Primary index should exist");

            byte[] indexKey = primaryIndex.subspace().pack(
                    Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), bobObjectId.toByteArray())
            );
            byte[] indexEntryBytes = tr.get(indexKey).join();
            assertNotNull(indexEntryBytes, "Primary index entry for Bob should exist");

            IndexEntry indexEntry = IndexEntry.decode(indexEntryBytes);
            assertTrue(indexEntry.shardId() >= 0 && indexEntry.shardId() <= 3,
                    "Bob's index entry should point to a local shard (0-3), but was: " + indexEntry.shardId());

            // Verify at the volume level: the document should be readable from node1's local volume
            BucketService node1BucketService = node1.getContext().getService(BucketService.NAME);
            EntryMetadata entryMetadata = EntryMetadata.decode(indexEntry.entryMetadata());
            try {
                ByteBuffer document = node1BucketService.getShard(indexEntry.shardId()).volume().getByEntryMetadata(entryMetadata);
                assertNotNull(document, "Document should be readable from local volume");

                byte[] docBytes = new byte[document.remaining()];
                document.get(docBytes);
                BsonDocument bobDoc = BSONUtil.toBsonDocument(docBytes);
                assertEquals("Bob", bobDoc.getString("name").getValue());
                assertEquals(31, bobDoc.getInt32("age").getValue(), "Bob's age should be 31 in local volume");
            } catch (IOException e) {
                fail("Failed to read document from local volume: " + e.getMessage());
            }

            // Verify the secondary index ("name") also points to a local shard after migration
            Index nameIndex = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);
            assertNotNull(nameIndex, "Secondary index on 'name' should exist");

            byte[] secondaryKey = nameIndex.subspace().pack(
                    Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), "Bob", bobObjectId.toByteArray())
            );
            byte[] secondaryEntryBytes = tr.get(secondaryKey).join();
            assertNotNull(secondaryEntryBytes, "Secondary index entry for Bob should exist");

            IndexEntry secondaryEntry = IndexEntry.decode(secondaryEntryBytes);
            assertTrue(secondaryEntry.shardId() >= 0 && secondaryEntry.shardId() <= 3,
                    "Bob's secondary index entry should point to a local shard (0-3), but was: " + secondaryEntry.shardId());

            // The secondary index entry metadata should match the primary index entry metadata
            assertArrayEquals(indexEntry.entryMetadata(), secondaryEntry.entryMetadata(),
                    "Secondary index entry metadata should match primary index entry metadata");
        }

        // Verify at node2's volume level: shard 4 should no longer contain Bob's entry
        try (Transaction tr = node2.getContext().getFoundationDB().createTransaction()) {
            String namespace = node2.getContext().getConfig().getString("default_namespace");
            BucketMetadata metadata = BucketMetadataUtil.open(node2.getContext(), tr, namespace, bucketName);

            BucketService node2BucketService = node2.getContext().getService(BucketService.NAME);
            BucketShard remoteShard = node2BucketService.getShard(4);
            assertNotNull(remoteShard, "Node2 should own shard 4");

            VolumeSession session = new VolumeSession(tr, metadata.prefix());
            int entryCount = 0;
            for (VolumeEntry ignored : remoteShard.volume().getRange(session)) {
                entryCount++;
            }
            assertEquals(0, entryCount, "Shard 4 on node2 should have no entries after migration");
        }
    }

    @Test
    void shouldNotMigrateRemoteDocumentOnNoOpUpdate() {
        // Behavior: When node1 executes UPDATE on a document that lives on a remote shard (shard 4,
        // owned by node2) and the update leaves the document unchanged, the document must stay on
        // the remote shard untouched. No migration happens, the primary index entry keeps pointing
        // to the remote shard, and the document's id does not appear in object_ids.

        String bucketName = "multi-shard-noop-update-bucket";

        // Create a bucket with shards 0 (node1) and 4 (node2)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(bucketName, BucketCreateArgs.Builder.shards(List.of(0, 4))).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail(err.content());
            }
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Switch both nodes to RESP3
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hello(3).encode(buf);
            runCommand(node1.getChannel(), buf);
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hello(3).encode(buf);
            runCommand(node2.getChannel(), buf);
        }

        // INSERT {"name": "Bob", "age": 30} on node2 → shard 4 (local to node2)
        ObjectId bobObjectId;
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}");
            insertCmd.insert(bucketName, document).encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("INSERT Bob on node2 failed: " + err.content());
            }
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage arrayMessage = (ArrayRedisMessage) response;
            assertEquals(1, arrayMessage.children().size());
            bobObjectId = TestUtil.bulkStringToObjectId((FullBulkStringRedisMessage) arrayMessage.children().getFirst());
        }

        // UPDATE Bob on node1 with the value he already holds — a no-op, no migration
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.update(bucketName, "{\"name\": \"Bob\"}", "{\"$set\": {\"age\": 30}}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("UPDATE Bob on node1 failed: " + err.content());
            }
            List<ObjectId> updatedIds = extractObjectIds(response);
            assertTrue(updatedIds.isEmpty(), "No-op update should not report any object ids");
        }

        // Verify at the index level: Bob's primary index entry should still point to shard 4
        try (Transaction tr = node1.getContext().getFoundationDB().createTransaction()) {
            String namespace = node1.getContext().getConfig().getString("default_namespace");
            BucketMetadata metadata = BucketMetadataUtil.open(node1.getContext(), tr, namespace, bucketName);

            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
            assertNotNull(primaryIndex, "Primary index should exist");

            byte[] indexKey = primaryIndex.subspace().pack(
                    Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), bobObjectId.toByteArray())
            );
            byte[] indexEntryBytes = tr.get(indexKey).join();
            assertNotNull(indexEntryBytes, "Primary index entry for Bob should exist");

            IndexEntry indexEntry = IndexEntry.decode(indexEntryBytes);
            assertEquals(4, indexEntry.shardId(),
                    "Bob's index entry should still point to the remote shard 4");
        }

        // Verify at node2's volume level: shard 4 still holds Bob's entry with unchanged content
        try (Transaction tr = node2.getContext().getFoundationDB().createTransaction()) {
            String namespace = node2.getContext().getConfig().getString("default_namespace");
            BucketMetadata metadata = BucketMetadataUtil.open(node2.getContext(), tr, namespace, bucketName);

            BucketService node2BucketService = node2.getContext().getService(BucketService.NAME);
            BucketShard remoteShard = node2BucketService.getShard(4);
            assertNotNull(remoteShard, "Node2 should own shard 4");

            VolumeSession session = new VolumeSession(tr, metadata.prefix());
            int entryCount = 0;
            for (VolumeEntry entry : remoteShard.volume().getRange(session)) {
                entryCount++;
                ByteBuffer document = entry.entry();
                byte[] docBytes = new byte[document.remaining()];
                document.get(docBytes);
                BsonDocument bobDoc = BSONUtil.toBsonDocument(docBytes);
                assertEquals("Bob", bobDoc.getString("name").getValue());
                assertEquals(30, bobDoc.getInt32("age").getValue(), "Bob's age should remain 30 on the remote shard");
            }
            assertEquals(1, entryCount, "Shard 4 on node2 should still hold exactly 1 entry");
        }

        // QUERY "{}" on node1 — Bob is still readable across shards, content unchanged
        {
            BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("QUERY on node1 failed: " + err.content());
            }

            List<BsonDocument> entries = extractEntries(response);
            assertEquals(1, entries.size(), "Expected 1 document in query result");
            assertEquals("Bob", entries.getFirst().getString("name").getValue());
            assertEquals(30, entries.getFirst().getInt32("age").getValue(), "Bob's age should remain 30");
        }
    }
}
