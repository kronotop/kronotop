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

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.*;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class BucketIndexDescribeSubcommandTest extends BaseIndexHandlerTest {

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe("non-existing-bucket", "not-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldReturnErrorIfIndexDoesNotExist() {
        getBucketMetadata(TEST_BUCKET); // creates the bucket with the default id index
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "not-existing-index").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHINDEX No such index: 'not-existing-index'", actualMessage.content());
    }

    @Test
    void shouldDescribeIndex() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"username\": {\"bson_type\": \"string\"}}").encode(buf);
            runCommand(channel, buf);
        }

        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        String indexName = "selector:username.bsonType:STRING";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            switch (key.content().toString(StandardCharsets.UTF_8)) {
                case "index_type" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("single_field", value.content().toString(StandardCharsets.UTF_8));
                }
                case "id" -> {
                    IntegerRedisMessage value = (IntegerRedisMessage) entry.getValue();
                    Index index = metadata.indexes().getIndex("username", IndexSelectionPolicy.ALL);
                    assertEquals(index.definition().id(), value.value());
                }
                case "selector" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("username", value.content().toString(StandardCharsets.UTF_8));
                }
                case "bson_type" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("STRING", value.content().toString(StandardCharsets.UTF_8));
                }
                case "status" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    String status = value.content().toString(StandardCharsets.UTF_8);
                    // A freshly created index is either still queued (WAITING) or already picked up by
                    // the background builder (BUILDING); the exact value is a timing race.
                    assertTrue(Objects.equals(status, IndexStatus.WAITING.name())
                            || Objects.equals(status, IndexStatus.BUILDING.name()));
                }
                case "collation" -> {
                    MapRedisMessage collationMap = (MapRedisMessage) entry.getValue();
                    assertEquals(9, collationMap.children().size());
                    for (Map.Entry<RedisMessage, RedisMessage> ce : collationMap.children().entrySet()) {
                        assertInstanceOf(NullRedisMessage.class, ce.getValue());
                    }
                }
                case "statistics" -> {
                    MapRedisMessage value = (MapRedisMessage) entry.getValue();
                    for (Map.Entry<RedisMessage, RedisMessage> statsEntry : value.children().entrySet()) {
                        FullBulkStringRedisMessage statsKey = (FullBulkStringRedisMessage) statsEntry.getKey();
                        if (statsKey.content().toString(StandardCharsets.UTF_8).equals("cardinality")) {
                            IntegerRedisMessage cardinality = (IntegerRedisMessage) statsEntry.getValue();
                            assertEquals(0, cardinality.value());
                        }
                    }
                }
                default -> fail("Unexpected key: " + key.content().toString(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void shouldDescribeVectorIndex() {
        // Behavior: INDEX DESCRIBE returns vector-specific fields (selector, dimensions, distance) for a vector index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}").encode(buf);
            runCommand(channel, buf);
        }

        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        String indexName = "vector:embedding.dimensions:3.distance:COSINE";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            switch (key.content().toString(StandardCharsets.UTF_8)) {
                case "index_type" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("vector", value.content().toString(StandardCharsets.UTF_8));
                }
                case "id" -> {
                    IntegerRedisMessage value = (IntegerRedisMessage) entry.getValue();
                    VectorIndex vectorIndex = metadata.vectorIndexes().getIndexByName(indexName, IndexSelectionPolicy.ALL);
                    assertEquals(vectorIndex.definition().id(), value.value());
                }
                case "selector" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("embedding", value.content().toString(StandardCharsets.UTF_8));
                }
                case "dimensions" -> {
                    IntegerRedisMessage value = (IntegerRedisMessage) entry.getValue();
                    assertEquals(3, value.value());
                }
                case "distance" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("COSINE", value.content().toString(StandardCharsets.UTF_8));
                }
                case "status" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    String status = value.content().toString(StandardCharsets.UTF_8);
                    assertTrue(Objects.equals(status, IndexStatus.WAITING.name())
                            || Objects.equals(status, IndexStatus.BUILDING.name())
                    );
                }
                case "statistics" -> {
                    MapRedisMessage value = (MapRedisMessage) entry.getValue();
                    for (Map.Entry<RedisMessage, RedisMessage> statsEntry : value.children().entrySet()) {
                        FullBulkStringRedisMessage statsKey = (FullBulkStringRedisMessage) statsEntry.getKey();
                        if (statsKey.content().toString(StandardCharsets.UTF_8).equals("cardinality")) {
                            IntegerRedisMessage cardinality = (IntegerRedisMessage) statsEntry.getValue();
                            assertEquals(0, cardinality.value());
                        }
                    }
                }
                default -> fail("Unexpected key: " + key.content().toString(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void shouldDescribeCompoundIndex() {
        // Behavior: INDEX DESCRIBE returns compound-specific fields (fields array, statistics) for a compound index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$compound\": [{\"name\": \"test-compound\", \"fields\": [{\"selector\": \"age\", \"bson_type\": \"int32\"}, {\"selector\": \"name\", \"bson_type\": \"string\"}]}]}").encode(buf);
            runCommand(channel, buf);
        }

        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        String indexName = "test-compound";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            switch (key.content().toString(StandardCharsets.UTF_8)) {
                case "index_type" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    assertEquals("compound", value.content().toString(StandardCharsets.UTF_8));
                }
                case "id" -> {
                    IntegerRedisMessage value = (IntegerRedisMessage) entry.getValue();
                    CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(indexName, IndexSelectionPolicy.ALL);
                    assertEquals(compoundIndex.definition().id(), value.value());
                }
                case "fields" -> {
                    ArrayRedisMessage value = (ArrayRedisMessage) entry.getValue();
                    List<RedisMessage> fieldArray = value.children();
                    assertEquals(2, fieldArray.size());

                    // First field: age/int32
                    MapRedisMessage firstField = (MapRedisMessage) fieldArray.get(0);
                    for (Map.Entry<RedisMessage, RedisMessage> fe : firstField.children().entrySet()) {
                        String fk = ((FullBulkStringRedisMessage) fe.getKey()).content().toString(StandardCharsets.UTF_8);
                        String fv = ((FullBulkStringRedisMessage) fe.getValue()).content().toString(StandardCharsets.UTF_8);
                        if (fk.equals("selector")) assertEquals("age", fv);
                        if (fk.equals("bson_type")) assertEquals("INT32", fv);
                    }

                    // Second field: name/string
                    MapRedisMessage secondField = (MapRedisMessage) fieldArray.get(1);
                    for (Map.Entry<RedisMessage, RedisMessage> fe : secondField.children().entrySet()) {
                        String fk = ((FullBulkStringRedisMessage) fe.getKey()).content().toString(StandardCharsets.UTF_8);
                        String fv = ((FullBulkStringRedisMessage) fe.getValue()).content().toString(StandardCharsets.UTF_8);
                        if (fk.equals("selector")) assertEquals("name", fv);
                        if (fk.equals("bson_type")) assertEquals("STRING", fv);
                    }
                }
                case "status" -> {
                    FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
                    String status = value.content().toString(StandardCharsets.UTF_8);
                    // A freshly created index is either still queued (WAITING) or already picked up by
                    // the background builder (BUILDING); the exact value is a timing race.
                    assertTrue(Objects.equals(status, IndexStatus.WAITING.name())
                            || Objects.equals(status, IndexStatus.BUILDING.name()));
                }
                case "collation" -> {
                    MapRedisMessage collationMap = (MapRedisMessage) entry.getValue();
                    assertEquals(9, collationMap.children().size());
                    for (Map.Entry<RedisMessage, RedisMessage> ce : collationMap.children().entrySet()) {
                        assertInstanceOf(NullRedisMessage.class, ce.getValue());
                    }
                }
                case "statistics" -> {
                    MapRedisMessage value = (MapRedisMessage) entry.getValue();
                    for (Map.Entry<RedisMessage, RedisMessage> statsEntry : value.children().entrySet()) {
                        FullBulkStringRedisMessage statsKey = (FullBulkStringRedisMessage) statsEntry.getKey();
                        if (statsKey.content().toString(StandardCharsets.UTF_8).equals("cardinality")) {
                            IntegerRedisMessage cardinality = (IntegerRedisMessage) statsEntry.getValue();
                            assertEquals(0, cardinality.value());
                        }
                    }
                }
                default -> fail("Unexpected key: " + key.content().toString(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void shouldDescribeSingleFieldIndexWithCollation() {
        // Behavior: INDEX DESCRIBE returns collation details when a single-field index has collation.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 2}}}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        String indexName = "selector:name.bsonType:STRING";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            if (key.content().toString(StandardCharsets.UTF_8).equals("collation")) {
                MapRedisMessage collationMap = (MapRedisMessage) entry.getValue();
                assertEquals(9, collationMap.children().size());
                for (Map.Entry<RedisMessage, RedisMessage> ce : collationMap.children().entrySet()) {
                    String ck = ((FullBulkStringRedisMessage) ce.getKey()).content().toString(StandardCharsets.UTF_8);
                    switch (ck) {
                        case "locale" ->
                                assertEquals("tr", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        case "strength" -> assertEquals(2, ((IntegerRedisMessage) ce.getValue()).value());
                        case "case_level" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "case_first" ->
                                assertEquals("off", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        case "numeric_ordering" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "alternate" ->
                                assertEquals("non-ignorable", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        case "backwards" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "normalization" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "max_variable" ->
                                assertEquals("punct", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        default -> fail("Unexpected collation key: " + ck);
                    }
                }
            }
        }
    }

    @Test
    void shouldDescribeCompoundIndexWithCollation() {
        // Behavior: INDEX DESCRIBE returns collation details when a compound index has collation.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$compound\": [{\"name\": \"test-collated-compound\", \"fields\": [{\"selector\": \"city\", \"bson_type\": \"string\"}, {\"selector\": \"price\", \"bson_type\": \"double\"}], \"collation\": {\"locale\": \"en\"}}]}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        String indexName = "test-collated-compound";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;

        Map<RedisMessage, RedisMessage> fields = actualMessage.children();
        for (Map.Entry<RedisMessage, RedisMessage> entry : fields.entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            if (key.content().toString(StandardCharsets.UTF_8).equals("collation")) {
                MapRedisMessage collationMap = (MapRedisMessage) entry.getValue();
                assertEquals(9, collationMap.children().size());
                for (Map.Entry<RedisMessage, RedisMessage> ce : collationMap.children().entrySet()) {
                    String ck = ((FullBulkStringRedisMessage) ce.getKey()).content().toString(StandardCharsets.UTF_8);
                    switch (ck) {
                        case "locale" ->
                                assertEquals("en", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        case "strength" -> assertEquals(3, ((IntegerRedisMessage) ce.getValue()).value());
                        case "case_level" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "case_first" ->
                                assertEquals("off", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        case "numeric_ordering" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "alternate" ->
                                assertEquals("non-ignorable", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        case "backwards" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "normalization" -> assertFalse(((BooleanRedisMessage) ce.getValue()).value());
                        case "max_variable" ->
                                assertEquals("punct", ((FullBulkStringRedisMessage) ce.getValue()).content().toString(StandardCharsets.UTF_8));
                        default -> fail("Unexpected collation key: " + ck);
                    }
                }
            }
        }
    }
}