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
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.bson.BsonType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BucketUniqueIndexTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        // Behavior: every test runs against a bucket with a unique single-field index on "email".
        SingleFieldIndexDefinition emailIndex = SingleFieldIndexDefinition.create(
                "email-index", "email", BsonType.STRING, false, IndexStatus.WAITING, null, true);
        createIndexThenWaitForReadiness(emailIndex);
    }

    @Test
    void shouldRejectSecondInsertWithDuplicateUniqueValue() {
        // Behavior: inserting a second document with a value already present in a unique index fails.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"email\": \"a@x.com\", \"name\": \"A\"}"));
        assertDuplicateKey(insertRaw("{\"email\": \"a@x.com\", \"name\": \"B\"}"));
    }

    @Test
    void shouldRejectDuplicateUniqueValueWithinSameBatch() {
        // Behavior: two documents in one insert batch sharing a unique value fail, since the FDB read
        // cannot see the not-yet-written sibling and the in-memory batch set catches the collision.
        assertDuplicateKey(insertRaw(
                "{\"email\": \"dup@x.com\", \"name\": \"A\"}",
                "{\"email\": \"dup@x.com\", \"name\": \"B\"}"));
    }

    @Test
    void shouldRejectSecondDocumentMissingUniqueField() {
        // Behavior: a missing field is indexed as null and null counts as a value, so only one
        // document may omit the unique field.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"A\"}"));
        assertDuplicateKey(insertRaw("{\"name\": \"B\"}"));
    }

    @Test
    void shouldAllowDistinctUniqueValues() {
        // Behavior: distinct values in a unique index insert without conflict.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"email\": \"a@x.com\", \"name\": \"A\"}"));
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"email\": \"b@x.com\", \"name\": \"B\"}"));
    }

    @Test
    void shouldRejectDuplicateArrayElementOnUniqueIndex() {
        // Behavior: the write path indexes each array element even on a non-multi-key index, so
        // uniqueness must cover elements too. Two documents sharing an array element conflict.
        // Runs on a dedicated bucket with a unique "tag" index.
        String bucket = "uniq-tag-1";
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, SingleFieldIndexDefinition.create(
                "tag-index", "tag", BsonType.STRING, false, IndexStatus.WAITING, null, true));
        assertInstanceOf(ArrayRedisMessage.class, insertRawInto(bucket, "{\"tag\": [\"x\"]}"));
        assertDuplicateKey(insertRawInto(bucket, "{\"tag\": [\"x\"]}"));
    }

    @Test
    void shouldAllowRepeatedElementWithinSameArray() {
        // Behavior: a repeated element inside one document's array collapses to a single value and
        // does not conflict with itself.
        String bucket = "uniq-tag-2";
        createIndexThenWaitForReadiness(TEST_NAMESPACE, bucket, SingleFieldIndexDefinition.create(
                "tag-index", "tag", BsonType.STRING, false, IndexStatus.WAITING, null, true));
        assertInstanceOf(ArrayRedisMessage.class, insertRawInto(bucket, "{\"tag\": [\"x\", \"x\"]}"));
    }

    @Test
    void shouldRejectUpdateToExistingUniqueValue() {
        // Behavior: updating a document's unique field to a value held by another document fails.
        insertRaw("{\"email\": \"a@x.com\", \"name\": \"A\"}");
        insertRaw("{\"email\": \"b@x.com\", \"name\": \"B\"}");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"B\"}", "{\"$set\": {\"email\": \"a@x.com\"}}").encode(buf);
        assertDuplicateKey(runCommand(channel, buf));
    }

    @Test
    void shouldAllowUpdatingNonUniqueFieldOnUniqueIndexedDocument() {
        // Behavior: changing a non-unique field must not trip the document's own unique index entry
        // (self-exclusion): the document does not conflict with itself.
        insertRaw("{\"email\": \"a@x.com\", \"name\": \"A\"}");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"A\"}", "{\"$set\": {\"name\": \"A2\"}}").encode(buf);
        assertInstanceOf(MapRedisMessage.class, runCommand(channel, buf));
    }

    @Test
    void shouldRejectUpsertWithConflictingUniqueValue() {
        // Behavior: an upsert that creates a new document with a unique value already in use fails.
        insertRaw("{\"email\": \"a@x.com\", \"name\": \"A\"}");

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, "{\"name\": \"Z\"}", "{\"$set\": {\"email\": \"a@x.com\"}, \"upsert\": true}").encode(buf);
        assertDuplicateKey(runCommand(channel, buf));
    }
}
