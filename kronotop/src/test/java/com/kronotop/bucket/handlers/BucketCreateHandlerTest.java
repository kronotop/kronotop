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
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketCreateHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldCreateBucketSuccessfully() {
        // Behavior: BUCKET.CREATE creates a new bucket and returns OK. The bucket metadata
        // is persisted in FDB with a primary index registered.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());

        // Verify metadata is persisted in FDB with a primary index
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNotNull(metadata);
            assertFalse(metadata.removed());
            assertNotNull(metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.ALL));
        }
    }

    @Test
    void shouldFailWhenBucketAlreadyExists() {
        // Behavior: Calling BUCKET.CREATE on an already-existing bucket returns a
        // BUCKETALREADYEXISTS error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Try to create the same bucket again
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
            assertEquals("BUCKETALREADYEXISTS Bucket already exists: test-bucket", errorMessage.content());
        }
    }

    @Test
    void shouldCreateBucketThenInsertDocuments() {
        // Behavior: A bucket explicitly created via BUCKET.CREATE is fully functional and
        // accepts document inserts via BUCKET.INSERT.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Create the bucket explicitly
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Insert a document into the created bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.insert(TEST_BUCKET, TEST_DOCUMENT).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
            assertEquals(1, actualMessage.children().size());
        }
    }

    @Test
    void shouldFailOnRemovedBucket() {
        // Behavior: A removed bucket's volume prefix persists in FDB, so calling BUCKET.CREATE
        // on a removed bucket returns BUCKETALREADYEXISTS.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Explicitly create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
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

        // Invalidate the metadata cache so BUCKET.CREATE hits FDB
        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        {
            // Try to create the bucket again — should fail because the volume prefix still exists
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
            assertEquals("BUCKETALREADYEXISTS Bucket already exists: test-bucket", errorMessage.content());
        }
    }

    @Test
    void shouldCreateBucketWithIndexes() {
        // Behavior: BUCKET.CREATE with INDEXES creates the bucket and registers the
        // specified secondary index in metadata.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes("{\"username\": {\"bson_type\": \"string\"}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNotNull(metadata);

            // Primary index should be present
            assertNotNull(metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.ALL));

            // Secondary index for "username" should be present
            Index usernameIndex = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            assertNotNull(usernameIndex);
            assertEquals(BsonType.STRING, usernameIndex.definition().bsonType());
        }
    }

    @Test
    void shouldCreateBucketWithMultipleIndexFields() {
        // Behavior: BUCKET.CREATE with INDEXES containing multiple fields creates all
        // specified secondary indexes in a single command.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes("{\"username\": {\"bson_type\": \"string\"}, \"age\": {\"bson_type\": \"int32\"}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNotNull(metadata);

            Index usernameIndex = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            assertNotNull(usernameIndex);
            assertEquals(BsonType.STRING, usernameIndex.definition().bsonType());

            Index ageIndex = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            assertNotNull(ageIndex);
            assertEquals(BsonType.INT32, ageIndex.definition().bsonType());
        }
    }

    @Test
    void shouldReturnOKWhenBucketAlreadyExistsWithIfNotExists() {
        // Behavior: BUCKET.CREATE with IF_NOT_EXISTS returns OK even if the bucket already
        // exists, making the operation idempotent.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            // Create the bucket
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Create the same bucket again with IF_NOT_EXISTS — should return OK
            ByteBuf buf = Unpooled.buffer();
            cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.ifNotExists()).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void shouldFailWhenShardHasNoRoute() {
        // Behavior: BUCKET.CREATE with a shard ID that has no route in the cluster returns
        // an error indicating no route was found.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.shards(List.of(9999))).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
        assertEquals("ERR No route found for Bucket shard: 9999", errorMessage.content());
    }

    @Test
    void shouldFailWhenCreatingBucketWithVectorIndexAndMultipleShards() {
        // Behavior: BUCKET.CREATE with a $vector index and multiple shards returns an error
        // because vector indexes require single-shard buckets.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder
                        .shards(List.of(1, 2))
                        .indexes("{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"))
                .encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
        assertEquals("ERR Vector indexes require single-shard buckets", errorMessage.content());
    }

    @Test
    void shouldCreateBucketWithVectorIndexAndSingleShard() {
        // Behavior: BUCKET.CREATE with a $vector index and a single shard succeeds.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder
                        .shards(List.of(1))
                        .indexes("{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}"))
                .encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    @Test
    void shouldFailWithInvalidIndexSchema() {
        // Behavior: BUCKET.CREATE with an invalid INDEXES JSON (unknown bson_type) returns
        // an error and does not create the bucket.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.indexes("{\"username\": {\"bson_type\": \"invalid_type\"}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldCreateBucketWithCollation() {
        // Behavior: BUCKET.CREATE with COLLATION persists the collation spec in FDB.
        // Reloading metadata returns the collation with the specified locale and strength.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.collation("{\"locale\": \"en\", \"strength\": 2}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Collation collation = metadata.collation();
            assertNotNull(collation);
            assertEquals("en", collation.locale());
            assertEquals(2, collation.strength());
        }
    }

    @Test
    void shouldCreateBucketWithCollationDefaultValues() {
        // Behavior: BUCKET.CREATE with COLLATION containing only locale applies defaults
        // for all optional fields.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.collation("{\"locale\": \"tr\"}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            Collation collation = metadata.collation();
            assertNotNull(collation);
            assertEquals("tr", collation.locale());
            assertEquals(3, collation.strength());
            assertFalse(collation.caseLevel());
            assertEquals("off", collation.caseFirst());
            assertFalse(collation.numericOrdering());
            assertEquals("non-ignorable", collation.alternate());
            assertFalse(collation.backwards());
            assertFalse(collation.normalization());
        }
    }

    @Test
    void shouldCreateBucketWithCollationAndIndexes() {
        // Behavior: BUCKET.CREATE with both COLLATION and INDEXES persists both.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder
                .collation("{\"locale\": \"en\", \"strength\": 2}")
                .indexes("{\"username\": {\"bson_type\": \"string\"}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNotNull(metadata.collation());
            assertEquals("en", metadata.collation().locale());

            Index usernameIndex = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            assertNotNull(usernameIndex);
            assertEquals(BsonType.STRING, usernameIndex.definition().bsonType());
        }
    }

    @Test
    void shouldCreateBucketWithoutCollation() {
        // Behavior: BUCKET.CREATE without COLLATION results in null collation (binary default).
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNull(metadata.collation());
        }
    }

    @Test
    void shouldFailWithInvalidCollationStrength() {
        // Behavior: BUCKET.CREATE with invalid collation strength returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.collation("{\"locale\": \"en\", \"strength\": 99}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldFailWithInvalidCollationCaseFirst() {
        // Behavior: BUCKET.CREATE with invalid caseFirst returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.collation("{\"locale\": \"en\", \"case_first\": \"bad\"}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldFailWithUnknownCollationField() {
        // Behavior: BUCKET.CREATE with an unknown collation field returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.collation("{\"locale\": \"en\", \"extra\": true}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldFailWithMalformedCollationJson() {
        // Behavior: BUCKET.CREATE with malformed collation JSON returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder.collation("not-json")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
    }

    @Test
    void shouldCreateBucketWithIndexLevelCollation() {
        // Behavior: BUCKET.CREATE with INDEXES containing per-index collation persists the collation in the index definition.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder
                .indexes("{\"username\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 2}}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNull(metadata.collation());

            Index usernameIndex = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            assertNotNull(usernameIndex);
            SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, usernameIndex.subspace());
            assertNotNull(definition.collation());
            assertEquals("tr", definition.collation().locale());
            assertEquals(2, definition.collation().strength());
        }
    }

    @Test
    void shouldInheritBucketCollationOnInlineStringIndex() {
        // Behavior: BUCKET.CREATE with COLLATION and INDEXES where the index has no explicit collation
        // inherits the bucket-level collation on the STRING index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder
                .collation("{\"locale\": \"tr\"}")
                .indexes("{\"username\": {\"bson_type\": \"string\"}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNotNull(metadata.collation());
            assertEquals("tr", metadata.collation().locale());

            Index usernameIndex = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, usernameIndex.subspace());
            assertNotNull(definition.collation());
            assertEquals("tr", definition.collation().locale());
            assertEquals(3, definition.collation().strength());
        }
    }

    @Test
    void shouldCreateBucketWithBucketAndIndexLevelCollation() {
        // Behavior: BUCKET.CREATE with both bucket-level COLLATION and index-level collation persists both independently.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        ByteBuf buf = Unpooled.buffer();
        cmd.create(TEST_BUCKET, BucketCreateArgs.Builder
                .collation("{\"locale\": \"en\"}")
                .indexes("{\"username\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 2}}}")).encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            assertNotNull(metadata.collation());
            assertEquals("en", metadata.collation().locale());

            Index usernameIndex = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
            SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, usernameIndex.subspace());
            assertNotNull(definition.collation());
            assertEquals("tr", definition.collation().locale());
            assertEquals(2, definition.collation().strength());
        }
    }
}
