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
import com.kronotop.cluster.Member;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.network.Address;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketMultiNodeTest extends BaseBucketMultiNodeTest {
    private static final String TEST_BUCKET = "test-bucket";

    @Test
    void shouldRejectInsertWhenBucketShardNotOwnedByCurrentNode() {
        // Behavior: BUCKET.INSERT returns a REJECT error when the target bucket's shard
        // is not owned by the node receiving the command. The error includes the shard ID
        // and the address of the owning node.

        // Create a bucket on node2 with shard 4 (owned by node2)
        BucketCommandBuilder<String, String> createCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf createBuf = Unpooled.buffer();
        createCmd.create(TEST_BUCKET, BucketCreateArgs.Builder.shards(List.of(4))).encode(createBuf);

        Object createResponse = runCommand(node2.getChannel(), createBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, createResponse);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) createResponse).content());

        // Send BUCKET.INSERT on node1 (which does NOT own shard 4)
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"test\"}");
        insertCmd.insert(TEST_BUCKET, document).encode(insertBuf);

        Object insertResponse = runCommand(node1.getChannel(), insertBuf);
        assertInstanceOf(ErrorRedisMessage.class, insertResponse);

        String content = ((ErrorRedisMessage) insertResponse).content();
        assertTrue(content.startsWith("REJECT"), "Expected REJECT error but got: " + content);
        assertTrue(content.contains("4"), "Expected shard ID 4 in error but got: " + content);

        Member node2Member = node2.getContext().getMember();
        Address node2Address = node2Member.getExternalAddress();
        String expectedAddress = node2Address.getHost() + ":" + node2Address.getPort();
        assertTrue(content.contains(expectedAddress), "Expected node2 address in error but got: " + content);
    }

    @Test
    void shouldCommitSuccessfulInsertAfterRejectOnDifferentBucket() {
        // Behavior: Within a BEGIN...COMMIT block, a successful BUCKET.INSERT into a locally owned
        // bucket is not lost when a subsequent BUCKET.INSERT into a different bucket (on a remote
        // shard) returns REJECT. The transaction commits successfully, and the first insert is queryable.

        // Create bucket-a on node1 with shard 0 (owned by node1)
        BucketCommandBuilder<String, String> createCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        {
            ByteBuf buf = Unpooled.buffer();
            createCmd.create("bucket-a", BucketCreateArgs.Builder.shards(List.of(0))).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Create bucket-b on node2 with shard 4 (owned by node2)
        {
            ByteBuf buf = Unpooled.buffer();
            createCmd.create("bucket-b", BucketCreateArgs.Builder.shards(List.of(4))).encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Switch node1's channel to RESP3
        {
            ByteBuf buf = Unpooled.buffer();
            createCmd.hello(3).encode(buf);
            runCommand(node1.getChannel(), buf);
        }

        KronotopCommandBuilder<String, String> kronotopCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        // BEGIN transaction on node1
        {
            ByteBuf buf = Unpooled.buffer();
            kronotopCmd.begin().encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // INSERT into bucket-a on node1 (should succeed — shard 0 is local)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}");
            insertCmd.insert("bucket-a", document).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // INSERT into bucket-b on node1 (should be rejected — shard 4 is owned by node2)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}");
            insertCmd.insert("bucket-b", document).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            assertTrue(((ErrorRedisMessage) response).content().startsWith("REJECT"));
        }

        // COMMIT transaction on node1
        {
            ByteBuf buf = Unpooled.buffer();
            kronotopCmd.commit().encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // QUERY bucket-a on node1 — the Alice document should be there
        {
            BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query("bucket-a", "{}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);

            List<BsonDocument> entries = extractEntries(response);
            assertEquals(1, entries.size());
            assertEquals("Alice", entries.get(0).getString("name").getValue());
        }
    }

    @Test
    void shouldRejectUpdateWhenBucketShardNotOwnedByCurrentNode() {
        // Behavior: BUCKET.UPDATE returns a REJECT error when the target bucket's shard
        // is not owned by the node receiving the command. The error includes the shard ID
        // and the address of the owning node.

        // Create a bucket on node2 with shard 4 (owned by node2)
        BucketCommandBuilder<String, String> createCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf createBuf = Unpooled.buffer();
        createCmd.create(TEST_BUCKET, BucketCreateArgs.Builder.shards(List.of(4))).encode(createBuf);

        Object createResponse = runCommand(node2.getChannel(), createBuf);
        assertInstanceOf(SimpleStringRedisMessage.class, createResponse);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) createResponse).content());

        // Insert a document on node2 (which owns shard 4)
        BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf insertBuf = Unpooled.buffer();
        byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"test\"}");
        insertCmd.insert(TEST_BUCKET, document).encode(insertBuf);

        Object insertResponse = runCommand(node2.getChannel(), insertBuf);
        assertInstanceOf(ArrayRedisMessage.class, insertResponse);

        // Send BUCKET.UPDATE on node1 (which does NOT own shard 4)
        BucketCommandBuilder<String, String> updateCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf updateBuf = Unpooled.buffer();
        updateCmd.update(TEST_BUCKET, "{}", "{\"$set\": {\"name\": \"updated\"}}").encode(updateBuf);

        Object updateResponse = runCommand(node1.getChannel(), updateBuf);
        assertInstanceOf(ErrorRedisMessage.class, updateResponse);

        String content = ((ErrorRedisMessage) updateResponse).content();
        assertTrue(content.startsWith("REJECT"), "Expected REJECT error but got: " + content);
        assertTrue(content.contains("4"), "Expected shard ID 4 in error but got: " + content);

        Member node2Member = node2.getContext().getMember();
        Address node2Address = node2Member.getExternalAddress();
        String expectedAddress = node2Address.getHost() + ":" + node2Address.getPort();
        assertTrue(content.contains(expectedAddress), "Expected node2 address in error but got: " + content);
    }

    @Test
    void shouldRejectDeleteWhenBucketHasVectorIndexAndShardNotOwnedByCurrentNode() {
        // Behavior: BUCKET.DELETE returns a REJECT error when the target bucket has a vector
        // index and the shard is not owned by the node receiving the command. The error includes
        // the shard ID and the address of the owning node.

        // Create a bucket on node2 with shard 4 (owned by node2)
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.shards(List.of(4))).encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Create a vector index on the bucket (on node2)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}").encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        // Insert a document on node2 (which owns shard 4)
        {
            BucketCommandBuilder<byte[], byte[]> insertCmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            byte[] document = BSONUtil.jsonToDocumentThenBytes("{\"name\": \"test\", \"embedding\": [1.0, 2.0, 3.0]}");
            insertCmd.insert(TEST_BUCKET, document).encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        // Send BUCKET.DELETE on node1 (which does NOT own shard 4)
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(TEST_BUCKET, "{}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            assertInstanceOf(ErrorRedisMessage.class, response);

            String content = ((ErrorRedisMessage) response).content();
            assertTrue(content.startsWith("REJECT"), "Expected REJECT error but got: " + content);
            assertTrue(content.contains("4"), "Expected shard ID 4 in error but got: " + content);

            Member node2Member = node2.getContext().getMember();
            Address node2Address = node2Member.getExternalAddress();
            String expectedAddress = node2Address.getHost() + ":" + node2Address.getPort();
            assertTrue(content.contains(expectedAddress), "Expected node2 address in error but got: " + content);
        }
    }
}
