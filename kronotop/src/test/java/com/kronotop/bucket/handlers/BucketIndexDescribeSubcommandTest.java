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
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
        // Behavior: On a RESP3 session INDEX DESCRIBE returns a map with the single-field index details.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
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
                    SingleFieldIndex index = metadata.singleFieldIndexes().getIndex("username", IndexSelectionPolicy.ALL);
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
                case "unique" -> {
                    BooleanRedisMessage value = (BooleanRedisMessage) entry.getValue();
                    assertFalse(value.value());
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
                    assertEquals(0, collationMap.children().size());
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
    void shouldDescribeUniqueSingleFieldIndex() {
        // Behavior: On a RESP3 session INDEX DESCRIBE reports unique=true when a single-field index is created with "unique": true.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"email\": {\"bson_type\": \"string\", \"unique\": true}}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        String indexName = "selector:email.bsonType:STRING";

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, indexName).encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        boolean sawUnique = false;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            if (key.content().toString(StandardCharsets.UTF_8).equals("unique")) {
                sawUnique = true;
                assertTrue(((BooleanRedisMessage) entry.getValue()).value());
            }
        }
        assertTrue(sawUnique, "DESCRIBE output must contain the 'unique' field");
    }

    @Test
    void shouldDescribeUniqueCompoundIndex() {
        // Behavior: On a RESP3 session INDEX DESCRIBE reports unique=true when a compound index is created with "unique": true.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$compound\": [{\"name\": \"u_idx\", \"unique\": true, \"fields\": [{\"selector\": \"a\", \"bson_type\": \"string\"}, {\"selector\": \"b\", \"bson_type\": \"int32\"}]}]}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "u_idx").encode(buf);
        Object msg = runCommand(channel, buf);
        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        assertNotNull(actualMessage);

        boolean sawUnique = false;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) entry.getKey();
            if (key.content().toString(StandardCharsets.UTF_8).equals("unique")) {
                sawUnique = true;
                assertTrue(((BooleanRedisMessage) entry.getValue()).value());
            }
        }
        assertTrue(sawUnique, "DESCRIBE output must contain the 'unique' field");
    }

    @Test
    void shouldDescribeVectorIndex() {
        // Behavior: On a RESP3 session INDEX DESCRIBE returns vector-specific fields (selector, dimensions, distance) for a vector index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
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
        // Behavior: On a RESP3 session INDEX DESCRIBE returns compound-specific fields (fields array, statistics) for a compound index.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
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
                case "unique" -> {
                    BooleanRedisMessage value = (BooleanRedisMessage) entry.getValue();
                    assertFalse(value.value());
                }
                case "collation" -> {
                    MapRedisMessage collationMap = (MapRedisMessage) entry.getValue();
                    assertEquals(0, collationMap.children().size());
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
        // Behavior: On a RESP3 session INDEX DESCRIBE returns collation details when a single-field index has collation.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
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
        // Behavior: On a RESP3 session INDEX DESCRIBE returns collation details when a compound index has collation.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        switchProtocol(cmd, RESPVersion.RESP3);
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

    /**
     * Reads a flattened key/value list into a map. Keys are bulk strings.
     */
    private Map<String, RedisMessage> pairsToMap(List<RedisMessage> children) {
        assertEquals(0, children.size() % 2, "flattened list must hold key/value pairs");
        Map<String, RedisMessage> result = new LinkedHashMap<>();
        for (int index = 0; index < children.size(); index += 2) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) children.get(index);
            result.put(key.content().toString(StandardCharsets.UTF_8), children.get(index + 1));
        }
        return result;
    }

    private String stringValueOf(RedisMessage message) {
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }

    @Test
    void shouldDescribeSingleFieldIndexWhenRESP2() {
        // Behavior: RESP2 has no map type, so INDEX DESCRIBE returns a flat array and unique is sent as 1.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"email\": {\"bson_type\": \"string\", \"unique\": true}}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "selector:email.bsonType:STRING").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        List<RedisMessage> children = ((ArrayRedisMessage) msg).children();
        assertEquals(List.of("index_type", "id", "selector", "bson_type", "status", "unique", "collation", "statistics"),
                new ArrayList<>(pairsToMap(children).keySet()));

        Map<String, RedisMessage> fields = pairsToMap(children);
        assertEquals("single_field", stringValueOf(fields.get("index_type")));
        assertEquals("email", stringValueOf(fields.get("selector")));
        assertEquals("STRING", stringValueOf(fields.get("bson_type")));
        assertInstanceOf(IntegerRedisMessage.class, fields.get("id"));
        assertEquals(1, ((IntegerRedisMessage) fields.get("unique")).value());

        // No collation was given, so the collation array is empty.
        assertInstanceOf(ArrayRedisMessage.class, fields.get("collation"));
        assertTrue(((ArrayRedisMessage) fields.get("collation")).children().isEmpty());

        assertInstanceOf(ArrayRedisMessage.class, fields.get("statistics"));
        Map<String, RedisMessage> statistics = pairsToMap(((ArrayRedisMessage) fields.get("statistics")).children());
        assertEquals(0, ((IntegerRedisMessage) statistics.get("cardinality")).value());
    }

    @Test
    void shouldDescribeCompoundIndexWhenRESP2() {
        // Behavior: On RESP2 the compound fields array holds flat arrays instead of maps.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$compound\": [{\"name\": \"test-compound\", \"fields\": [{\"selector\": \"age\", \"bson_type\": \"int32\"}, {\"selector\": \"name\", \"bson_type\": \"string\"}]}]}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "test-compound").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Map<String, RedisMessage> fields = pairsToMap(((ArrayRedisMessage) msg).children());
        assertEquals("compound", stringValueOf(fields.get("index_type")));
        assertEquals(0, ((IntegerRedisMessage) fields.get("unique")).value());

        assertInstanceOf(ArrayRedisMessage.class, fields.get("fields"));
        List<RedisMessage> indexFields = ((ArrayRedisMessage) fields.get("fields")).children();
        assertEquals(2, indexFields.size());

        assertInstanceOf(ArrayRedisMessage.class, indexFields.get(0));
        Map<String, RedisMessage> first = pairsToMap(((ArrayRedisMessage) indexFields.get(0)).children());
        assertEquals("age", stringValueOf(first.get("selector")));
        assertEquals("INT32", stringValueOf(first.get("bson_type")));

        assertInstanceOf(ArrayRedisMessage.class, indexFields.get(1));
        Map<String, RedisMessage> second = pairsToMap(((ArrayRedisMessage) indexFields.get(1)).children());
        assertEquals("name", stringValueOf(second.get("selector")));
        assertEquals("STRING", stringValueOf(second.get("bson_type")));
    }

    @Test
    void shouldDescribeVectorIndexWhenRESP2() {
        // Behavior: On RESP2 a vector index is described as a flat array with no unique or collation field.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"$vector\": {\"field\": \"embedding\", \"dimensions\": 3, \"distance\": \"cosine\"}}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "vector:embedding.dimensions:3.distance:COSINE").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Map<String, RedisMessage> fields = pairsToMap(((ArrayRedisMessage) msg).children());
        assertEquals(List.of("index_type", "id", "selector", "dimensions", "distance", "status", "statistics"),
                new ArrayList<>(fields.keySet()));
        assertEquals("vector", stringValueOf(fields.get("index_type")));
        assertEquals("embedding", stringValueOf(fields.get("selector")));
        assertEquals(3, ((IntegerRedisMessage) fields.get("dimensions")).value());
        assertEquals("COSINE", stringValueOf(fields.get("distance")));

        assertInstanceOf(ArrayRedisMessage.class, fields.get("statistics"));
        Map<String, RedisMessage> statistics = pairsToMap(((ArrayRedisMessage) fields.get("statistics")).children());
        assertEquals(0, ((IntegerRedisMessage) statistics.get("cardinality")).value());
    }

    @Test
    void shouldRenderCollationAsFlatArrayWhenRESP2() {
        // Behavior: RESP2 has no boolean type, so the collation flags are sent as 1 or 0 in a flat array.
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"name\": {\"bson_type\": \"string\", \"collation\": {\"locale\": \"tr\", \"strength\": 2}}}").encode(buf);
            runCommand(channel, buf);
        }

        refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        ByteBuf buf = Unpooled.buffer();
        cmd.indexDescribe(TEST_BUCKET, "selector:name.bsonType:STRING").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Map<String, RedisMessage> fields = pairsToMap(((ArrayRedisMessage) msg).children());
        assertInstanceOf(ArrayRedisMessage.class, fields.get("collation"));
        List<RedisMessage> collationChildren = ((ArrayRedisMessage) fields.get("collation")).children();
        assertEquals(18, collationChildren.size());

        Map<String, RedisMessage> collation = pairsToMap(collationChildren);
        assertEquals("tr", stringValueOf(collation.get("locale")));
        assertEquals(2, ((IntegerRedisMessage) collation.get("strength")).value());
        assertEquals("off", stringValueOf(collation.get("case_first")));
        assertEquals("non-ignorable", stringValueOf(collation.get("alternate")));
        assertEquals("punct", stringValueOf(collation.get("max_variable")));
        for (String flag : List.of("case_level", "numeric_ordering", "backwards", "normalization")) {
            assertInstanceOf(IntegerRedisMessage.class, collation.get(flag), flag + " must be an integer on RESP2");
            assertEquals(0, ((IntegerRedisMessage) collation.get(flag)).value());
        }
    }
}
