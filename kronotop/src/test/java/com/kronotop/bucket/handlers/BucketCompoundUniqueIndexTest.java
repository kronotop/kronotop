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

import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BucketCompoundUniqueIndexTest extends BaseBucketHandlerTest {

    @BeforeEach
    void setUp() {
        // Behavior: every test runs against a bucket with a unique compound index on (name, age).
        CompoundIndexDefinition nameAgeIndex = CompoundIndexDefinition.create(
                "name-age-idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                ),
                IndexStatus.WAITING, null, true);
        createIndexThenWaitForReadiness(nameAgeIndex);
    }

    private Object update(String query, String updateDocument) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);
        ByteBuf buf = Unpooled.buffer();
        cmd.update(TEST_BUCKET, query, updateDocument).encode(buf);
        return runCommand(channel, buf);
    }

    @Test
    void shouldRejectSecondInsertWithDuplicateCompoundValue() {
        // Behavior: a second document with the same (name, age) combination fails.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"A\", \"age\": 30}"));
        assertDuplicateKey(insertRaw("{\"name\": \"A\", \"age\": 30}"));
    }

    @Test
    void shouldAllowSameFirstFieldDifferentSecond() {
        // Behavior: uniqueness is over the whole key, so the same name with a different age is allowed.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"A\", \"age\": 30}"));
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"A\", \"age\": 31}"));
    }

    @Test
    void shouldAllowSameSecondFieldDifferentFirst() {
        // Behavior: the same age with a different name is a distinct compound key and is allowed.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"A\", \"age\": 30}"));
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"B\", \"age\": 30}"));
    }

    @Test
    void shouldRejectDuplicateCompoundValueWithinSameBatch() {
        // Behavior: two documents in one insert batch sharing a compound key fail; the FDB read cannot
        // see the not-yet-written sibling, so the in-memory batch set catches the collision.
        assertDuplicateKey(insertRaw(
                "{\"name\": \"A\", \"age\": 30}",
                "{\"name\": \"A\", \"age\": 30}"));
    }

    @Test
    void shouldRejectSecondDocumentMissingBothFields() {
        // Behavior: missing fields are indexed as null and null counts as a value, so only one document
        // may omit both compound fields (both map to the (null, null) key).
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"other\": 1}"));
        assertDuplicateKey(insertRaw("{\"other\": 2}"));
    }

    @Test
    void shouldAllowDistinctCompoundValues() {
        // Behavior: distinct compound keys insert without conflict.
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"A\", \"age\": 30}"));
        assertInstanceOf(ArrayRedisMessage.class, insertRaw("{\"name\": \"B\", \"age\": 40}"));
    }

    @Test
    void shouldRejectUpdateToExistingCompoundValue() {
        // Behavior: updating a document's compound fields to a key held by another document fails.
        insertRaw("{\"name\": \"A\", \"age\": 30}");
        insertRaw("{\"name\": \"B\", \"age\": 40}");

        assertDuplicateKey(update("{\"name\": \"B\"}", "{\"$set\": {\"name\": \"A\", \"age\": 30}}"));
    }

    @Test
    void shouldAllowUpdatingNonIndexedFieldOnCompoundIndexedDocument() {
        // Behavior: changing a field outside the compound key must not trip the document's own entry
        // (self-exclusion): the document does not conflict with itself.
        insertRaw("{\"name\": \"A\", \"age\": 30, \"note\": \"x\"}");

        assertInstanceOf(MapRedisMessage.class,
                update("{\"name\": \"A\"}", "{\"$set\": {\"note\": \"y\"}}"));
    }

    @Test
    void shouldRejectUpsertWithConflictingCompoundValue() {
        // Behavior: an upsert that creates a new document with a compound key already in use fails.
        insertRaw("{\"name\": \"A\", \"age\": 30}");

        assertDuplicateKey(update("{\"status\": \"new\"}",
                "{\"$set\": {\"name\": \"A\", \"age\": 30}, \"upsert\": true}"));
    }
}
