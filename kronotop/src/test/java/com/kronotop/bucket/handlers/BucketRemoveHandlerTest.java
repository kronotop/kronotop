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

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketRemoveHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldRemoveBucketSuccessfully() {
        // Behavior: Inserting a document then calling BUCKET.REMOVE returns OK and marks the bucket metadata as removed.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        {
            // Remove the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Verify bucket is marked as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertTrue(metadata.removed());
        }
    }

    @Test
    void shouldThrowErrorWhenRemovingNonExistentBucket() {
        // Behavior: Attempting to remove a non-existent bucket returns a NOSUCHBUCKET error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.remove("non-existent-bucket").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existent-bucket'", actualMessage.content());
    }

    @Test
    void shouldThrowErrorWhenRemovingAlreadyRemovedBucket() {
        // Behavior: Attempting to remove an already-removed bucket returns a BUCKETBEINGREMOVED error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Insert a document to create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
        }

        {
            // Remove the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            // Try to remove the bucket again
            ByteBuf buf = Unpooled.buffer();
            cmd.remove(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
            assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", actualMessage.content());
        }
    }

    @Test
    void shouldRemoveBucketInGivenNamespace() {
        // Behavior: NAMESPACE removes the bucket in the given namespace. The bucket with the same
        // name in the session's current namespace stays.
        String otherNamespace = newNamespaceWithBucket(TEST_BUCKET, List.of());

        assertOK(run(bucketCmd.remove(TEST_BUCKET, otherNamespace)));

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(BucketMetadataUtil.forceOpen(context, tr, otherNamespace, TEST_BUCKET).removed());
            assertFalse(BucketMetadataUtil.forceOpen(context, tr, TEST_NAMESPACE, TEST_BUCKET).removed());
        }
    }

    @Test
    void shouldRejectRemoveWhenNamespaceDoesNotExist() {
        // Behavior: NAMESPACE with an unknown namespace returns NOSUCHNAMESPACE.
        assertNoSuchNamespace(run(bucketCmd.remove(TEST_BUCKET, MISSING_NAMESPACE)));
    }

    @Test
    void shouldRejectInvalidNamespaceArgument() {
        // Behavior: BUCKET.REMOVE rejects NAMESPACE without a value, an unknown keyword and an argument after the namespace.
        assertErrorReply("ERR NAMESPACE argument must be followed by a namespace",
                runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_REMOVE, List.of("test-bucket", "NAMESPACE")));
        assertErrorReply("ERR Unknown 'BOGUS' argument",
                runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_REMOVE, List.of("test-bucket", "BOGUS")));
        assertErrorReply("ERR wrong number of arguments for 'BUCKET.REMOVE' command",
                runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_REMOVE, List.of("test-bucket", "NAMESPACE", "ns", "EXTRA")));
    }
}
