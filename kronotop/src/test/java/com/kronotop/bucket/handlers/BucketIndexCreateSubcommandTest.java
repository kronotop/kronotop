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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.Collation;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketIndexCreateSubcommandTest extends BaseIndexHandlerTest {

    @Test
    void shouldFailWhenBucketDoesNotExist() {
        // Behavior: BUCKET.INDEX CREATE on a non-existing bucket returns a
        // BUCKETNOTFOUND error instead of auto-creating the bucket.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate("non-existing-bucket", "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldCreateIndexWithMultipleFields() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"selector-one\": {\"bson_type\": \"int32\"}, \"selector-two\": {\"bson_type\": \"string\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldReturnErrorForInvalidType() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"selector\": {\"bson_type\": \"int322\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Unknown BSON type: int322", actualMessage.content());
    }


    @Test
    void shouldCreateIndexForValidTypes() {
        String template = "{\"selector-%s\": {\"bson_type\": \"%s\"}}";
        List<String> validTypes = List.of("int32", "string", "double", "binary", "boolean", "datetime", "timestamp", "int64", "objectid");
        // TODO: Enable this when we implement decimal128 indexes - "decimal128");
        for (String validType : validTypes) {
            BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            ByteBuf buf = Unpooled.buffer();
            String directive = String.format(template, validType, validType);
            cmd.indexCreate(TEST_BUCKET, directive).encode(buf);
            Object msg = runCommand(channel, buf);
            if (msg instanceof ErrorRedisMessage errorRedisMessage) {
                fail("For '" + directive + "', should not return error: " + errorRedisMessage.content());
            }
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }
    }

    @Test
    void shouldThrowAnErrorWhenIndexAlreadyExists() {
        String definition = "{\"selector\": {\"bson_type\": \"int32\"}, \"username\": {\"bson_type\": \"string\"}}";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, definition).encode(buf);
            Object msg = runCommand(channel, buf);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals(Response.OK, actualMessage.content());
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, definition).encode(buf);
            Object msg = runCommand(channel, buf);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertNotNull(actualMessage);
            assertEquals("ERR An index on field 'selector' already exists", actualMessage.content());
        }
    }

    @Test
    void shouldReturnErrorForInvalidIndexDefinition() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"some\": \"key\"}").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("ERR Invalid index schema", actualMessage.content());
    }

    @Test
    void shouldCreateTaskForSecondaryIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"name\": \"test\", \"bson_type\": \"string\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());

        // Verify the task was created
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            List<Versionstamp> taskIds = IndexTaskUtil.getTaskIds(tx, TEST_NAMESPACE, TEST_BUCKET, "test");
            assertEquals(1, taskIds.size(), "Expected exactly one task to be created");
        }
    }

    @Test
    void shouldCreateIndexWithMultiKeyTrue() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"tags\": {\"bson_type\": \"string\", \"multi_key\": true}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());

        // Verify the index was created with multiKey flag
        var indexDefinition = loadIndexDefinition("tags");
        assertNotNull(indexDefinition);
        assertTrue(indexDefinition.multiKey());
    }

    @Test
    void shouldCreateIndexWithMultiKeyFalse() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"category\": {\"bson_type\": \"string\", \"multi_key\": false}}").encode(buf);
        Object msg = runCommand(channel, buf);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(Response.OK, actualMessage.content());

        // Verify the index was created with multiKey flag set to false
        var indexDefinition = loadIndexDefinition("category");
        assertNotNull(indexDefinition);
        assertFalse(indexDefinition.multiKey());
    }

    @Test
    void shouldReturnErrorWhenBsonTypeIsNull() {
        // Behavior: When bson_type is explicitly set to null in the index schema JSON,
        // the server returns "ERR 'bson_type' cannot be null".
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"selector\": {\"bson_type\": null}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR 'bson_type' cannot be null", errorMessage.content());
    }

    @Test
    void shouldReturnErrorWhenBsonTypeIsMissing() {
        // Behavior: When the bson_type key is absent from the index schema JSON,
        // the server returns "ERR 'bson_type' cannot be null".
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"selector\": {}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR 'bson_type' cannot be null", errorMessage.content());
    }

    @Test
    void shouldThrowBucketBeingRemovedExceptionWhenCreatingIndexOnRemovedBucket() {
        // Behavior: INDEX CREATE on a bucket that has been marked as removed returns
        // a BUCKETBEINGREMOVED error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"name\": {\"name\": \"test-index\", \"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // Wait for the background index building task to complete before marking removed
        {
            BucketMetadata metadata = TransactionUtil.execute(context,
                    tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
            );
            Index index = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
        }

        // Get the bucket metadata and mark it as removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET);
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.setRemoved(tx, metadata);
            tr.commit().join();
        }

        // Flush the bucket metadata cache so open reads the dropped status
        Runnable cleanup = context.getBucketMetadataCache().createEvictionWorker(context::now, 0);
        cleanup.run();

        // Wait until the cache entry is actually evicted
        await().atMost(Duration.ofSeconds(5)).until(() ->
                context.getBucketMetadataCache().get(TEST_NAMESPACE, TEST_BUCKET) == null
        );

        // Try to create an index on the dropped bucket
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"age\": {\"bson_type\": \"int32\"}}").encode(buf);
            Object msg = runCommand(channel, buf);

            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertEquals("BUCKETBEINGREMOVED Bucket 'test-bucket' is being removed", errorMessage.content());
        }
    }

    @Test
    void shouldCreateCompoundIndex() {
        // Behavior: A valid $compound directive creates a compound index and returns OK.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"category\", \"bson_type\": \"string\"}, {\"selector\": \"price\", \"bson_type\": \"double\"}]}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        // Verify the compound index is loadable from metadata
        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
        );
        Collection<CompoundIndex> compounds = metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.ALL);
        assertEquals(1, compounds.size());
        CompoundIndex compoundIndex = compounds.iterator().next();
        assertEquals(2, compoundIndex.definition().fields().size());
        assertEquals("category", compoundIndex.definition().fields().get(0).selector());
        assertEquals("price", compoundIndex.definition().fields().get(1).selector());
    }

    @Test
    void shouldCreateCompoundIndexWithExplicitName() {
        // Behavior: A $compound entry with an explicit name uses that name.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"name\": \"my_compound\", \"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"b\", \"bson_type\": \"int32\"}]}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
        );
        CompoundIndex idx = metadata.compoundIndexes().getIndexByName("my_compound", IndexSelectionPolicy.ALL);
        assertNotNull(idx);
    }

    @Test
    void shouldAutoGenerateCompoundIndexName() {
        // Behavior: A $compound entry without a name gets an auto-generated name.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"category\", \"bson_type\": \"string\"}, {\"selector\": \"price\", \"bson_type\": \"double\"}]}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
        );
        CompoundIndex idx = metadata.compoundIndexes().getIndexByName("compound:category.STRING_price.DOUBLE", IndexSelectionPolicy.ALL);
        assertNotNull(idx);
    }

    @Test
    void shouldRejectCompoundIndexWithLessThanTwoFields() {
        // Behavior: A $compound entry with fewer than 2 fields returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}]}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldRejectCompoundIndexWithDuplicateSelectors() {
        // Behavior: A $compound entry with duplicate selectors returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"a\", \"bson_type\": \"int32\"}]}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldCreateMixedSingleFieldAndCompoundIndexes() {
        // Behavior: A payload with both single-field and compound indexes creates both.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"username\": {\"bson_type\": \"string\"}, \"$compound\": [{\"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"b\", \"bson_type\": \"int32\"}]}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
        );
        // Single field: _id (primary) + username
        assertNotNull(metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL));
        // Compound
        assertEquals(1, metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.ALL).size());
    }

    @Test
    void shouldRejectCompoundIndexWithMoreThan32Fields() {
        // Behavior: A $compound entry with more than 32 fields returns an error.
        StringBuilder fieldsJson = new StringBuilder("[");
        for (int i = 1; i <= 33; i++) {
            if (i > 1) fieldsJson.append(", ");
            fieldsJson.append(String.format("{\"selector\": \"field%d\", \"bson_type\": \"string\"}", i));
        }
        fieldsJson.append("]");
        String schema = "{\"$compound\": [{\"fields\": " + fieldsJson + "}]}";

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Compound index supports at most 32 fields", errorMessage.content());
    }

    @Test
    void shouldFailWhenCreatingVectorIndexOnMultiShardBucket() {
        // Behavior: BUCKET.INDEX CREATE with a $vector directive on a multi-shard bucket
        // returns an error because vector indexes require single-shard buckets.
        String multiShardBucket = "multi-shard-bucket";
        createBucket(multiShardBucket, List.of(1, 2), null);

        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(multiShardBucket, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Vector indexes require single-shard buckets", errorMessage.content());
    }

    @Test
    void shouldCreateVectorIndexOnSingleShardBucket() {
        // Behavior: BUCKET.INDEX CREATE with a $vector directive on a single-shard bucket succeeds.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexCreate(TEST_BUCKET, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
    }

    @Test
    void shouldRejectCompoundIndexNameCollisionWithSingleField() {
        // Behavior: A compound index cannot reuse the name of an existing single-field index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        // Create a single-field index named "shared_name"
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"field\": {\"bson_type\": \"string\", \"name\": \"shared_name\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        // Try to create a compound index with the same name
        {
            ByteBuf buf = Unpooled.buffer();
            String schema = "{\"$compound\": [{\"name\": \"shared_name\", \"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"b\", \"bson_type\": \"int32\"}]}]}";
            cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR 'shared_name' has already exist", errorMessage.content());
        }
    }

    @Test
    void shouldCreateSingleFieldIndexWithCollation() {
        // Behavior: A STRING index with a collation spec is created and the collation is persisted.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 2}}}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        SingleFieldIndexDefinition definition = loadIndexDefinition("name");
        assertNotNull(definition.collation());
        assertEquals("tr", definition.collation().locale());
        assertEquals(2, definition.collation().strength());
    }

    @Test
    void shouldCreateCompoundIndexWithCollation() {
        // Behavior: A compound index with a collation spec is created and the collation is persisted.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"city\", \"bson_type\": \"string\"}, {\"selector\": \"price\", \"bson_type\": \"double\"}], \"collation\": {\"locale\": \"en\"}}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());

        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
        );
        Collection<CompoundIndex> compounds = metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.ALL);
        assertEquals(1, compounds.size());
        CompoundIndexDefinition definition = compounds.iterator().next().definition();
        assertNotNull(definition.collation());
        assertEquals("en", definition.collation().locale());
    }

    @Test
    void shouldRejectCollationOnNonStringIndex() {
        // Behavior: Collation on a non-STRING index returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"age\": {\"bson_type\": \"int32\", \"collation\": {\"locale\": \"en\"}}}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Collation is only supported for STRING indexes"));
    }

    @Test
    void shouldRejectCollationOnCompoundIndexWithNoStringField() {
        // Behavior: Collation on a compound index with no STRING field returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"a\", \"bson_type\": \"int32\"}, {\"selector\": \"b\", \"bson_type\": \"double\"}], \"collation\": {\"locale\": \"en\"}}]}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertTrue(errorMessage.content().contains("Collation requires at least one STRING field in compound index"));
    }

    @Test
    void shouldRejectCollationWithInvalidLocale() {
        // Behavior: Collation with a blank locale returns an error.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"\"}}}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
    }

    @Test
    void shouldCreateIndexWithoutCollationHasNullCollation() {
        // Behavior: An index created without a collation spec has null collation in its definition.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"name\": {\"bson_type\": \"string\"}}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = loadIndexDefinition("name");
        assertNull(definition.collation());
    }

    @Test
    void shouldInheritBucketCollationOnSingleFieldStringIndex() {
        // Behavior: A STRING index created on a bucket with bucket-level collation inherits
        // the bucket collation when no index-level collation is specified.
        String collatedBucket = "collated-bucket";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(collatedBucket, BucketCreateArgs.Builder.collation("{\"locale\": \"tr\"}")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(collatedBucket, "{\"name\": {\"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, collatedBucket);
            Index index = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, index.subspace());
            assertNotNull(definition.collation());
            assertEquals("tr", definition.collation().locale());
            assertEquals(3, definition.collation().strength());
        }
    }

    @Test
    void shouldNotInheritBucketCollationOnNonStringIndex() {
        // Behavior: A non-STRING index created on a bucket with bucket-level collation does
        // not inherit the collation.
        String collatedBucket = "collated-bucket-int";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(collatedBucket, BucketCreateArgs.Builder.collation("{\"locale\": \"tr\"}")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(collatedBucket, "{\"age\": {\"bson_type\": \"int32\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, collatedBucket);
            Index index = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, index.subspace());
            assertNull(definition.collation());
        }
    }

    @Test
    void shouldOverrideBucketCollationWithIndexLevelCollation() {
        // Behavior: An index-level collation takes precedence over bucket-level collation.
        String collatedBucket = "collated-bucket-override";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(collatedBucket, BucketCreateArgs.Builder.collation("{\"locale\": \"en\"}")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(collatedBucket, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 2}}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, collatedBucket);
            Index index = metadata.indexes().getIndex("name", IndexSelectionPolicy.ALL);
            SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, index.subspace());
            assertNotNull(definition.collation());
            assertEquals("tr", definition.collation().locale());
            assertEquals(2, definition.collation().strength());
        }
    }

    @Test
    void shouldInheritBucketCollationOnCompoundIndexWithStringField() {
        // Behavior: A compound index with a STRING field inherits bucket-level collation
        // when no index-level collation is specified.
        String collatedBucket = "collated-bucket-compound";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(collatedBucket, BucketCreateArgs.Builder.collation("{\"locale\": \"fr\"}")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"city\", \"bson_type\": \"string\"}, {\"selector\": \"price\", \"bson_type\": \"double\"}]}]}";
            cmd.indexCreate(collatedBucket, schema).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, collatedBucket)
        );
        Collection<CompoundIndex> compounds = metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.ALL);
        assertEquals(1, compounds.size());
        CompoundIndexDefinition definition = compounds.iterator().next().definition();
        assertNotNull(definition.collation());
        assertEquals("fr", definition.collation().locale());
    }

    @Test
    void shouldNotInheritBucketCollationOnCompoundIndexWithNoStringField() {
        // Behavior: A compound index with no STRING fields does not inherit bucket-level collation.
        String collatedBucket = "collated-bucket-compound-nstr";
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(collatedBucket, BucketCreateArgs.Builder.collation("{\"locale\": \"fr\"}")).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"x\", \"bson_type\": \"int32\"}, {\"selector\": \"y\", \"bson_type\": \"double\"}]}]}";
            cmd.indexCreate(collatedBucket, schema).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        BucketMetadata metadata = TransactionUtil.execute(context,
                tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, collatedBucket)
        );
        Collection<CompoundIndex> compounds = metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.ALL);
        assertEquals(1, compounds.size());
        CompoundIndexDefinition definition = compounds.iterator().next().definition();
        assertNull(definition.collation());
    }

    @Test
    void shouldCreateSingleFieldIndexWithCollationDefaults() {
        // Behavior: A collation with only locale specified gets default values for all other fields.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        String schema = "{\"city\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"fr\"}}}";
        cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, msg);

        SingleFieldIndexDefinition definition = loadIndexDefinition("city");
        Collation collation = definition.collation();
        assertNotNull(collation);
        assertEquals("fr", collation.locale());
        assertEquals(3, collation.strength());
        assertFalse(collation.caseLevel());
        assertEquals("off", collation.caseFirst());
        assertFalse(collation.numericOrdering());
        assertEquals("non-ignorable", collation.alternate());
        assertFalse(collation.backwards());
        assertFalse(collation.normalization());
    }

    @Test
    void shouldRejectDuplicateSingleFieldIndexOnSameSelector() {
        // Behavior: Creating a second single-field index on the same field with a different bsonType is rejected.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"int32\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR An index on field 'username' already exists", errorMessage.content());
        }
    }

    @Test
    void shouldRejectDuplicateCompoundIndexOnSameFields() {
        // Behavior: Creating a second compound index on the same ordered field set is rejected.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            String schema = "{\"$compound\": [{\"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"b\", \"bson_type\": \"int32\"}]}]}";
            cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            String schema = "{\"$compound\": [{\"name\": \"different-name\", \"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"b\", \"bson_type\": \"int32\"}]}]}";
            cmd.indexCreate(TEST_BUCKET, schema).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertTrue(errorMessage.content().startsWith("ERR A compound index on the same fields already exists:"));
        }
    }

    @Test
    void shouldRejectDuplicateVectorIndexOnSameField() {
        // Behavior: Creating a second vector index on the same field is rejected.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 5, \"distance\": \"euclidean\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR A vector index on field 'embedding' already exists", errorMessage.content());
        }
    }
}
