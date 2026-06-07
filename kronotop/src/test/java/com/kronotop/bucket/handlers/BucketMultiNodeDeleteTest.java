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
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketMultiNodeDeleteTest extends BaseBucketMultiNodeTest {

    @Override
    protected boolean runWithTCPServer() {
        return true;
    }

    @Test
    void shouldDeleteDocumentsAcrossMultipleShards() {
        // Behavior: A DELETE with an empty filter on node2 removes documents from both
        // the local shard (shard 4, owned by node2) and the remote shard (shard 0, owned
        // by node1).

        String bucketName = "multi-shard-delete-bucket";

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

        // DELETE "{}" on node2 — should delete both documents across shards
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.delete(bucketName, "{}").encode(buf);
            Object response = runCommand(node2.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("DELETE on node2 failed: " + err.content());
            }
            List<ObjectId> deletedIds = extractObjectIds(response);
            assertEquals(2, deletedIds.size(), "Expected 2 documents deleted across both shards");
        }

        // QUERY "{}" on node1 — should return 0 entries (all data deleted)
        {
            BucketCommandBuilder<String, String> queryCmd = new BucketCommandBuilder<>(StringCodec.UTF8);
            ByteBuf buf = Unpooled.buffer();
            queryCmd.query(bucketName, "{}").encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("QUERY on node1 failed: " + err.content());
            }
            List<BsonDocument> entries = extractEntries(response);
            assertEquals(0, entries.size(), "Expected 0 documents after delete");
        }
    }
}
