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

package com.kronotop.bucket.index.statistics;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.index.*;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexStatsBuilderTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldStoreHintWhenValuePassesProbabilisticSelector() {
        // Behavior: setHintForStats(tr, index, objectId, value) stores an ObjectId in STAT_HINTS
        // subspace when the value passes the probabilistic selector.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-matching-value");
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test_index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        Index index = new Index(definition, subspace);

        // BsonNull always matches the probabilistic selector (hash = 0, 0 & 0x3FFF = 0)
        BsonValue matchingValue = BsonNull.VALUE;
        assertTrue(ProbabilisticSelector.match(matchingValue), "BsonNull should match");

        ObjectId objectId = new ObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray(), matchingValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), objectId.toByteArray());
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Hint key should exist in FoundationDB");
            assertEquals(0, value.length, "Hint value should be empty");
        }
    }

    @Test
    void shouldNotStoreHintWhenValueFailsProbabilisticSelector() {
        // Behavior: setHintForStats(tr, index, objectId, value) does not store anything when
        // the value fails the probabilistic selector.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-non-matching-value");
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test_index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        Index index = new Index(definition, subspace);

        BsonValue nonMatchingValue = new BsonInt32(1);
        assertFalse(ProbabilisticSelector.match(nonMatchingValue), "BsonInt32(1) should not match");

        ObjectId objectId = new ObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray(), nonMatchingValue);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple prefixTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue());
            byte[] prefix = subspace.pack(prefixTuple);

            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            List<KeyValue> results = tr.getRange(begin, end, 1).asList().join();
            assertTrue(results.isEmpty(), "No hint keys should exist when value does not match");
        }
    }

    @Test
    void shouldStoreHintUnconditionallyWithoutValueParameter() {
        // Behavior: setHintForStats(tr, index, objectId) stores an ObjectId in STAT_HINTS
        // subspace unconditionally, bypassing the probabilistic selector.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-unconditional");
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test_index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        Index index = new Index(definition, subspace);

        ObjectId objectId = new ObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), objectId.toByteArray());
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Hint key should exist in FoundationDB");
            assertEquals(0, value.length, "Hint value should be empty");
        }
    }

    @Test
    void shouldUseCorrectKeyStructure() {
        // Behavior: setHintForStats creates a key with structure (STAT_HINTS, ObjectId bytes).
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-key-structure");
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test_index", "field", BsonType.STRING, false, IndexStatus.WAITING);
        Index index = new Index(definition, subspace);

        ObjectId objectId = new ObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStats(tr, index, objectId.toByteArray());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), objectId.toByteArray());
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Key with expected structure should exist");

            Tuple unpackedTuple = subspace.unpack(expectedKey);
            assertEquals(2, unpackedTuple.size(), "Tuple should have 2 elements");
            assertEquals(IndexSubspaceMagic.STAT_HINTS.getValue(), unpackedTuple.getLong(0), "First element should be STAT_HINTS");
            assertArrayEquals(objectId.toByteArray(), unpackedTuple.getBytes(1), "Second element should be ObjectId bytes");
        }
    }

    /**
     * Finds an ObjectId whose bytes pass ProbabilisticSelector.match(byte[]).
     */
    private ObjectId findMatchingObjectId() {
        for (int i = 0; i < 1_000_000; i++) {
            ObjectId candidate = new ObjectId();
            if (ProbabilisticSelector.match(candidate.toByteArray())) {
                return candidate;
            }
        }
        throw new AssertionError("Could not find a matching ObjectId within 1M attempts");
    }

    /**
     * Finds an ObjectId whose bytes do NOT pass ProbabilisticSelector.match(byte[]).
     */
    private ObjectId findNonMatchingObjectId() {
        for (int i = 0; i < 100; i++) {
            ObjectId candidate = new ObjectId();
            if (!ProbabilisticSelector.match(candidate.toByteArray())) {
                return candidate;
            }
        }
        throw new AssertionError("Could not find a non-matching ObjectId within 100 attempts");
    }

    @Test
    void shouldStoreHintWhenObjectIdPassesByteArraySelector() {
        // Behavior: setHintForStatsIfSelected stores an ObjectId in STAT_HINTS when the ObjectId
        // bytes pass the probabilistic selector.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-if-selected-match");
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test_index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        Index index = new Index(definition, subspace);

        ObjectId objectId = findMatchingObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStatsIfSelected(tr, index, objectId.toByteArray());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), objectId.toByteArray());
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Hint key should exist when ObjectId bytes match");
            assertEquals(0, value.length, "Hint value should be empty");
        }
    }

    @Test
    void shouldNotStoreHintWhenObjectIdFailsByteArraySelector() {
        // Behavior: setHintForStatsIfSelected does not store anything when the ObjectId bytes
        // fail the probabilistic selector.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-if-selected-no-match");
        SingleFieldIndexDefinition definition = SingleFieldIndexDefinition.create("test_index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        Index index = new Index(definition, subspace);

        ObjectId objectId = findNonMatchingObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStatsIfSelected(tr, index, objectId.toByteArray());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple prefixTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue());
            byte[] prefix = subspace.pack(prefixTuple);

            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            List<KeyValue> results = tr.getRange(begin, end, 1).asList().join();
            assertTrue(results.isEmpty(), "No hint keys should exist when ObjectId bytes do not match");
        }
    }

    @Test
    void shouldStoreHintForCompoundIndex() {
        // Behavior: setHintForStats works with CompoundIndex (via IndexHolder interface),
        // producing the same key structure as single-field indexes.
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-hint-compound-index");
        CompoundIndexDefinition definition = CompoundIndexDefinition.create(
                "test_compound",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                ),
                IndexStatus.WAITING
        );
        CompoundIndex compoundIndex = new CompoundIndex(definition, subspace);

        ObjectId objectId = new ObjectId();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStats(tr, compoundIndex, objectId.toByteArray());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), objectId.toByteArray());
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Hint key should exist for compound index");
            assertEquals(0, value.length, "Hint value should be empty");
        }
    }
}
