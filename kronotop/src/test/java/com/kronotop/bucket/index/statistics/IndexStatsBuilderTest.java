/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.TestUtil;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class IndexStatsBuilderTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldSetHintForStats_whenValueMatches() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-index-stats-set-matching");
        IndexDefinition definition = IndexDefinition.create("test_index", "field", BsonType.INT32);
        Index index = new Index(definition, subspace);

        // Use BsonNull which always matches (hash = 0, 0 & 0x3FFF = 0)
        BsonValue matchingValue = BsonNull.VALUE;
        assertTrue(ProbabilisticSelector.match(matchingValue), "BsonNull should match");

        CompletableFuture<byte[]> future = null;
        int userVersion = 1;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            future = tr.getVersionstamp();
            IndexStatsBuilder.setHintForStats(tr, userVersion, index, matchingValue);
            tr.commit().join();
        }
        Versionstamp versionstamp = Versionstamp.complete(future.join(), userVersion);

        // Verify the key was set
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), versionstamp);
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Hint key should exist in FoundationDB");
            assertEquals(0, value.length, "Hint value should be empty");
        }
    }

    @Test
    void shouldNotSetHintForStats_whenValueDoesNotMatch() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-index-stats-set-non-matching");
        IndexDefinition definition = IndexDefinition.create("test_index", "field", BsonType.INT32);
        Index index = new Index(definition, subspace);

        // Find a value that doesn't match
        BsonValue nonMatchingValue = new BsonInt32(1);
        assertFalse(ProbabilisticSelector.match(nonMatchingValue), "BsonInt32(1) should not match");

        int userVersion = 1;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.setHintForStats(tr, userVersion, index, nonMatchingValue);
            tr.commit().join();
        }

        // Verify no key was set - scan the entire STAT_HINTS prefix
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple prefixTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue());
            byte[] prefix = subspace.pack(prefixTuple);

            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            // Check that no keys exist with STAT_HINTS prefix
            List<KeyValue> results = tr.getRange(begin, end, 1).asList().join();
            assertTrue(results.isEmpty(), "No hint keys should exist when value does not match");
        }
    }

    @Test
    void shouldInsertHintForStats_whenValueMatches() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-index-stats-insert-matching");
        IndexDefinition definition = IndexDefinition.create("test_index", "field", BsonType.INT32);
        Index index = new Index(definition, subspace);

        // Use BsonNull which always matches
        BsonValue matchingValue = BsonNull.VALUE;
        assertTrue(ProbabilisticSelector.match(matchingValue), "BsonNull should match");

        Versionstamp versionstamp = TestUtil.generateVersionstamp(42);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.insertHintForStats(tr, versionstamp, index, matchingValue);
            tr.commit().join();
        }

        // Verify the key was set
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), versionstamp);
            byte[] expectedKey = subspace.pack(expectedTuple);

            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Hint key should exist in FoundationDB");
            assertEquals(0, value.length, "Hint value should be empty");
        }
    }

    @Test
    void shouldNotInsertHintForStats_whenValueDoesNotMatch() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-index-stats-insert-non-matching");
        IndexDefinition definition = IndexDefinition.create("test_index", "field", BsonType.INT32);
        Index index = new Index(definition, subspace);

        // Find a value that doesn't match
        BsonValue nonMatchingValue = new BsonInt32(1);
        assertFalse(ProbabilisticSelector.match(nonMatchingValue), "BsonInt32(1) should not match");

        Versionstamp versionstamp = TestUtil.generateVersionstamp(42);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.insertHintForStats(tr, versionstamp, index, nonMatchingValue);
            tr.commit().join();
        }

        // Verify no key was set - scan the entire STAT_HINTS prefix
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple prefixTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue());
            byte[] prefix = subspace.pack(prefixTuple);

            // Check that no keys exist with STAT_HINTS prefix
            KeySelector begin = KeySelector.firstGreaterThan(prefix);
            KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));
            List<KeyValue> results = tr.getRange(begin, end, 1).asList().join();
            assertTrue(results.isEmpty(), "No hint keys should exist when value does not match");
        }
    }

    @Test
    void shouldUseCorrectKeyStructure_forSetHintForStats() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-key-structure-set");
        IndexDefinition definition = IndexDefinition.create("test_index", "field", BsonType.STRING);
        Index index = new Index(definition, subspace);

        BsonValue matchingValue = BsonNull.VALUE;
        int userVersion = 123;

        CompletableFuture<byte[]> future = null;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            future = tr.getVersionstamp();
            IndexStatsBuilder.setHintForStats(tr, userVersion, index, matchingValue);
            tr.commit().join();
        }
        Versionstamp completedVersionstamp = Versionstamp.complete(future.join(), userVersion);

        // Verify key structure: STAT_HINTS magic byte + versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), completedVersionstamp);
            byte[] expectedKey = subspace.pack(expectedTuple);

            // Verify the key exists and matches expected structure
            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Key with expected structure should exist");

            // Unpack and verify tuple structure
            Tuple unpackedTuple = subspace.unpack(expectedKey);
            assertEquals(2, unpackedTuple.size(), "Tuple should have 2 elements");
            assertEquals(IndexSubspaceMagic.STAT_HINTS.getValue(), unpackedTuple.getLong(0), "First element should be STAT_HINTS");
            assertEquals(completedVersionstamp, unpackedTuple.getVersionstamp(1), "Second element should be versionstamp");
        }
    }

    @Test
    void shouldUseCorrectKeyStructure_forInsertHintForStats() {
        DirectorySubspace subspace = createOrOpenSubspaceUnderCluster("test-key-structure-insert");
        IndexDefinition definition = IndexDefinition.create("test_index", "field", BsonType.STRING);
        Index index = new Index(definition, subspace);

        BsonValue matchingValue = BsonNull.VALUE;
        Versionstamp versionstamp = TestUtil.generateVersionstamp(456);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexStatsBuilder.insertHintForStats(tr, versionstamp, index, matchingValue);
            tr.commit().join();
        }

        // Verify key structure: STAT_HINTS magic byte + versionstamp
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Tuple expectedTuple = Tuple.from(IndexSubspaceMagic.STAT_HINTS.getValue(), versionstamp);
            byte[] expectedKey = subspace.pack(expectedTuple);

            // Verify the key exists and matches expected structure
            byte[] value = tr.get(expectedKey).join();
            assertNotNull(value, "Key with expected structure should exist");

            // Unpack and verify tuple structure
            Tuple unpackedTuple = subspace.unpack(expectedKey);
            assertEquals(2, unpackedTuple.size(), "Tuple should have 2 elements");
            assertEquals(IndexSubspaceMagic.STAT_HINTS.getValue(), unpackedTuple.getLong(0), "First element should be STAT_HINTS");
            assertEquals(versionstamp, unpackedTuple.getVersionstamp(1), "Second element should be versionstamp");
        }
    }
}