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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.*;
import org.bson.BsonArray;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UpdateExecutorTest extends BasePipelineTest {

    @Test
    void shouldUpdateAllAffectedIndexesWhenFieldModified() {
        // Create two secondary indexes
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexDefinition nameIndex = IndexDefinition.create("name_idx", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get index subspaces
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READ);
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Index nameIdx = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);

        DirectorySubspace primarySubspace = primaryIndex.subspace();
        DirectorySubspace ageSubspace = ageIdx.subspace();
        DirectorySubspace nameSubspace = nameIdx.subspace();

        // Verify initial state
        assertEquals(3, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should have 3 entries");
        assertEquals(3, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 3 entries");
        assertEquals(3, fetchAllIndexedEntries(nameSubspace).size(), "Name index should have 3 entries");
        assertEquals(3, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 3 back pointers");
        assertEquals(3, fetchAllIndexBackPointers(nameSubspace).size(), "Name index should have 3 back pointers");

        // Verify age index contains values 25, 30, 35 before update
        Set<Integer> ageValuesBefore = extractAgeValuesFromIndex(ageSubspace);
        assertTrue(ageValuesBefore.contains(25), "Age index should contain 25 before update");
        assertTrue(ageValuesBefore.contains(30), "Age index should contain 30 before update");
        assertTrue(ageValuesBefore.contains(35), "Age index should contain 35 before update");

        // Update Bob's age from 30 to 40
        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(40)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<Versionstamp> updatedVersionstamps;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updatedVersionstamps = updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, updatedVersionstamps.size(), "Should have updated 1 document");

        // Verify index counts remain the same (update, not delete)
        assertEquals(3, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should still have 3 entries");
        assertEquals(3, fetchAllIndexedEntries(ageSubspace).size(), "Age index should still have 3 entries");
        assertEquals(3, fetchAllIndexedEntries(nameSubspace).size(), "Name index should still have 3 entries");
        assertEquals(3, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should still have 3 back pointers");
        assertEquals(3, fetchAllIndexBackPointers(nameSubspace).size(), "Name index should still have 3 back pointers");

        // Verify age index now contains 25, 35, 40 (30 replaced with 40)
        Set<Integer> ageValuesAfter = extractAgeValuesFromIndex(ageSubspace);
        assertFalse(ageValuesAfter.contains(30), "Age index should NOT contain 30 after update");
        assertTrue(ageValuesAfter.contains(40), "Age index should contain 40 after update");
        assertTrue(ageValuesAfter.contains(25), "Age index should still contain 25");
        assertTrue(ageValuesAfter.contains(35), "Age index should still contain 35");

        // Verify the document can be queried with new value
        List<String> bobQuery = runQueryOnBucket(metadata, "{age: {$eq: 40}}");
        assertEquals(1, bobQuery.size(), "Should find Bob with age 40");
        assertTrue(bobQuery.getFirst().contains("\"name\": \"Bob\""), "Result should be Bob");

        // Verify old value returns no results
        List<String> oldAgeQuery = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertTrue(oldAgeQuery.isEmpty(), "Should not find any document with age 30");
    }

    @Test
    void shouldUpdateMultipleIndexedFieldsSimultaneously() {
        // Create two secondary indexes
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexDefinition nameIndex = IndexDefinition.create("name_idx", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get index subspaces
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Index nameIdx = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);

        DirectorySubspace ageSubspace = ageIdx.subspace();
        DirectorySubspace nameSubspace = nameIdx.subspace();

        // Verify initial values
        Set<Integer> ageValuesBefore = extractAgeValuesFromIndex(ageSubspace);
        Set<String> nameValuesBefore = extractNameValuesFromIndex(nameSubspace);
        assertTrue(ageValuesBefore.contains(30), "Age index should contain 30");
        assertTrue(nameValuesBefore.contains("Bob"), "Name index should contain Bob");

        // Update both age and name for Bob
        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("age", new BsonInt32(50))
                .set("name", new BsonString("Robert"))
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify both indexes updated
        Set<Integer> ageValuesAfter = extractAgeValuesFromIndex(ageSubspace);
        Set<String> nameValuesAfter = extractNameValuesFromIndex(nameSubspace);

        assertFalse(ageValuesAfter.contains(30), "Age index should NOT contain 30");
        assertTrue(ageValuesAfter.contains(50), "Age index should contain 50");
        assertFalse(nameValuesAfter.contains("Bob"), "Name index should NOT contain Bob");
        assertTrue(nameValuesAfter.contains("Robert"), "Name index should contain Robert");

        // Verify queries work with new values
        List<String> robertQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Robert\"}}");
        assertEquals(1, robertQuery.size(), "Should find Robert");
        assertTrue(robertQuery.getFirst().contains("\"age\": 50"), "Robert should have age 50");
    }

    @Test
    void shouldReplaceNullIndexEntryWhenFieldAddedViaUpdate() {
        // Create an INT32 index on age field BEFORE inserting documents
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Insert documents WITHOUT the indexed field
        // Note: Missing fields are indexed as null entries
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetVersionstamps(TEST_BUCKET, documents);

        // Get age index subspace
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();

        // Verify age index has 2 null entries (missing fields are indexed as null)
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 2 null entries initially");
        assertEquals(2, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 2 back pointers initially");

        // Verify documents can be found via null query
        List<String> nullAgeQuery = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(2, nullAgeQuery.size(), "Should find 2 documents with null age");

        // Update Alice to ADD the age field (replaces null entry with actual value)
        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(25)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify age index still has 2 entries (Alice's null replaced with 25, Bob still null)
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should still have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should still have 2 back pointers");

        // Verify Alice can now be found via age=25 query
        List<String> aliceByAge = runQueryOnBucket(metadata, "{age: {$eq: 25}}");
        assertEquals(1, aliceByAge.size(), "Should find Alice by age 25");
        assertTrue(aliceByAge.getFirst().contains("\"name\": \"Alice\""), "Result should be Alice");

        // Verify only Bob remains with null age
        List<String> nullAgeAfterUpdate = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(1, nullAgeAfterUpdate.size(), "Should find 1 document with null age after update");
        assertTrue(nullAgeAfterUpdate.getFirst().contains("\"name\": \"Bob\""), "Bob should still have null age");

        // Update Bob to also add age field
        PipelineNode bobPlan = createExecutionPlan(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("age", new BsonInt32(30)).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(metadata, bobOptions, bobPlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, bobCtx);
            tr.commit().join();
        }

        // Verify age index still has 2 entries (both now have actual values)
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 2 back pointers");

        // Verify indexed values are non-null integers
        Set<Integer> allAgeValues = extractAgeValuesFromIndex(ageSubspace);
        assertEquals(2, allAgeValues.size(), "Should have 2 distinct age values");
        assertTrue(allAgeValues.contains(25), "Age index should contain 25");
        assertTrue(allAgeValues.contains(30), "Age index should contain 30");

        // Verify no documents have null age anymore
        List<String> noNullAge = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertTrue(noNullAge.isEmpty(), "No documents should have null age after both updates");

        // Verify both documents can be queried by their actual age values
        List<String> bobByAge = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertEquals(1, bobByAge.size(), "Should find Bob by age 30");
        assertTrue(bobByAge.getFirst().contains("\"name\": \"Bob\""), "Result should be Bob");
    }

    @Test
    void shouldReplaceNullIndexEntryWhenNestedFieldAddedViaUpdate() {
        final String TEST_BUCKET = "test-bucket-nested-field-update";

        // Create an INT32 index on nested field "a.b.c" BEFORE inserting documents
        IndexDefinition nestedIndex = IndexDefinition.create("nested_idx", "a.b.c", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, nestedIndex);

        // Insert documents WITHOUT the nested indexed field
        // Note: Missing fields are indexed as null entries
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"a\": {\"x\": 1}}")  // has "a" but not "a.b.c"
        );
        insertDocumentsAndGetVersionstamps(TEST_BUCKET, documents);

        // Get nested index subspace
        Index nestedIdx = metadata.indexes().getIndex("a.b.c", IndexSelectionPolicy.READ);
        DirectorySubspace nestedSubspace = nestedIdx.subspace();

        // Verify nested index has 2 null entries (missing nested fields are indexed as null)
        assertEquals(2, fetchAllIndexedEntries(nestedSubspace).size(), "Nested index should have 2 null entries initially");
        assertEquals(2, fetchAllIndexBackPointers(nestedSubspace).size(), "Nested index should have 2 back pointers initially");

        // Verify documents can be found via null query on nested field
        List<String> nullNestedQuery = runQueryOnBucket(metadata, "{\"a.b.c\": {$eq: null}}");
        assertEquals(2, nullNestedQuery.size(), "Should find 2 documents with null a.b.c");

        // Update Alice to ADD the nested field (replaces null entry with actual value)
        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("a.b.c", new BsonInt32(100)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify nested index still has 2 entries (Alice's null replaced with 100, Bob still null)
        assertEquals(2, fetchAllIndexedEntries(nestedSubspace).size(), "Nested index should still have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(nestedSubspace).size(), "Nested index should still have 2 back pointers");

        // Verify Alice can now be found via a.b.c=100 query
        List<String> aliceByNested = runQueryOnBucket(metadata, "{\"a.b.c\": {$eq: 100}}");
        assertEquals(1, aliceByNested.size(), "Should find Alice by a.b.c=100");
        assertTrue(aliceByNested.getFirst().contains("\"name\": \"Alice\""), "Result should be Alice");

        // Verify only Bob remains with null a.b.c
        List<String> nullNestedAfterUpdate = runQueryOnBucket(metadata, "{\"a.b.c\": {$eq: null}}");
        assertEquals(1, nullNestedAfterUpdate.size(), "Should find 1 document with null a.b.c after update");
        assertTrue(nullNestedAfterUpdate.getFirst().contains("\"name\": \"Bob\""), "Bob should still have null a.b.c");

        // Update Bob to also add nested field
        PipelineNode bobPlan = createExecutionPlan(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("a.b.c", new BsonInt32(200)).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(metadata, bobOptions, bobPlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, bobCtx);
            tr.commit().join();
        }

        // Verify nested index still has 2 entries (both now have actual values)
        assertEquals(2, fetchAllIndexedEntries(nestedSubspace).size(), "Nested index should have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(nestedSubspace).size(), "Nested index should have 2 back pointers");

        // Verify indexed values are non-null integers
        Set<Integer> allNestedValues = extractIntValuesFromIndex(nestedSubspace);
        assertEquals(2, allNestedValues.size(), "Should have 2 distinct nested values");
        assertTrue(allNestedValues.contains(100), "Nested index should contain 100");
        assertTrue(allNestedValues.contains(200), "Nested index should contain 200");

        // Verify no documents have null a.b.c anymore
        List<String> noNullNested = runQueryOnBucket(metadata, "{\"a.b.c\": {$eq: null}}");
        assertTrue(noNullNested.isEmpty(), "No documents should have null a.b.c after both updates");

        // Verify both documents can be queried by their actual nested values
        List<String> bobByNested = runQueryOnBucket(metadata, "{\"a.b.c\": {$eq: 200}}");
        assertEquals(1, bobByNested.size(), "Should find Bob by a.b.c=200");
        assertTrue(bobByNested.getFirst().contains("\"name\": \"Bob\""), "Result should be Bob");
    }

    private Set<Integer> extractIntValuesFromIndex(DirectorySubspace indexSubspace) {
        Set<Integer> values = new HashSet<>();
        List<KeyValue> entries = fetchAllIndexedEntries(indexSubspace);
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));

        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            // Secondary index key structure: BqlValue | Versionstamp
            // First element is the indexed value
            Object value = keyTuple.get(0);
            if (value instanceof Long) {
                values.add(((Long) value).intValue());
            }
        }
        return values;
    }

    private Set<Integer> extractAgeValuesFromIndex(DirectorySubspace indexSubspace) {
        Set<Integer> values = new HashSet<>();
        List<KeyValue> entries = fetchAllIndexedEntries(indexSubspace);
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));

        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            // Secondary index key structure: BqlValue | Versionstamp
            // First element is the indexed value
            values.add((int) keyTuple.getLong(0));
        }
        return values;
    }

    private Set<String> extractNameValuesFromIndex(DirectorySubspace indexSubspace) {
        Set<String> values = new HashSet<>();
        List<KeyValue> entries = fetchAllIndexedEntries(indexSubspace);
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));

        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            // Secondary index key structure: BqlValue | Versionstamp
            // First element is the indexed value
            values.add(keyTuple.getString(0));
        }
        return values;
    }

    @Test
    void shouldCreateMultikeyIndexEntriesWhenUpdatingToArrayValue() {
        // Create a multikey index on "tags.name"
        IndexDefinition tagsIndex = IndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, tagsIndex);

        // Insert a document without the tags field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Verify initial state: 1 null entry
        assertEquals(1, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 1 null entry initially");

        // Update Alice to add tags array with multiple values
        BsonArray tagsArray = new BsonArray();
        tagsArray.add(new BsonString("java"));
        tagsArray.add(new BsonString("kotlin"));
        tagsArray.add(new BsonString("scala"));

        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags.name", tagsArray).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify multikey index has 3 entries (java, kotlin, scala)
        List<KeyValue> entries = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(3, entries.size(), "Tags index should have 3 entries after multikey update");

        // Verify all values are indexed
        Set<String> indexedValues = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(indexedValues.contains("java"), "Should have 'java' indexed");
        assertTrue(indexedValues.contains("kotlin"), "Should have 'kotlin' indexed");
        assertTrue(indexedValues.contains("scala"), "Should have 'scala' indexed");
    }

    @Test
    void shouldDeduplicateMultikeyIndexEntriesWhenUpdating() {
        // Create a multikey index on "tags.name"
        IndexDefinition tagsIndex = IndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, tagsIndex);

        // Insert a document without the tags field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Update Alice with tags array containing duplicates
        BsonArray tagsArray = new BsonArray();
        tagsArray.add(new BsonString("java"));
        tagsArray.add(new BsonString("java"));  // duplicate
        tagsArray.add(new BsonString("kotlin"));
        tagsArray.add(new BsonString("java"));  // duplicate

        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags.name", tagsArray).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify only 2 unique entries (deduplication)
        List<KeyValue> entries = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(2, entries.size(), "Tags index should have 2 entries after deduplication");

        Set<String> indexedValues = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(indexedValues.contains("java"), "Should have 'java' indexed");
        assertTrue(indexedValues.contains("kotlin"), "Should have 'kotlin' indexed");
        assertEquals(2, indexedValues.size(), "Should have exactly 2 unique values");
    }

    @Test
    void shouldTrackCardinalityCorrectlyForMultikeyIndexUpdate() {
        // Create a multikey index on "langs.name"
        IndexDefinition langsIndex = IndexDefinition.create("langs_idx", "langs.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, langsIndex);

        // Insert two documents without the langs field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Update Alice with 2 unique langs
        BsonArray aliceLangs = new BsonArray();
        aliceLangs.add(new BsonString("java"));
        aliceLangs.add(new BsonString("kotlin"));

        PipelineNode alicePlan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions aliceUpdate = UpdateOptions.builder().set("langs.name", aliceLangs).build();
        QueryOptions aliceOptions = QueryOptions.builder().update(aliceUpdate).build();
        QueryContext aliceCtx = new QueryContext(metadata, aliceOptions, alicePlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, aliceCtx);
            tr.commit().join();
        }

        // Verify cardinality after the first update: 2 (from Alice) + 1 (null from Bob) = 3
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(langsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(3L, stats.cardinality(), "Cardinality should be 3 after first update");
        }

        // Update Bob with 3 unique langs
        BsonArray bobLangs = new BsonArray();
        bobLangs.add(new BsonString("python"));
        bobLangs.add(new BsonString("rust"));
        bobLangs.add(new BsonString("go"));

        PipelineNode bobPlan = createExecutionPlan(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("langs.name", bobLangs).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(metadata, bobOptions, bobPlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, bobCtx);
            tr.commit().join();
        }

        // Verify cardinality after second update: 2 (Alice) + 3 (Bob) = 5
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(langsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(5L, stats.cardinality(), "Cardinality should be 5 after second update");
        }
    }

    @Test
    void shouldDeduplicateAndTrackCardinalityCorrectlyForMultikeyIndexUpdate() {
        // Create a multikey index on "skills.name"
        IndexDefinition skillsIndex = IndexDefinition.create("skills_idx", "skills.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, skillsIndex);

        // Insert documents without skills field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\"}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        Index skillsIdx = metadata.indexes().getIndex("skills.name", IndexSelectionPolicy.READ);
        DirectorySubspace skillsSubspace = skillsIdx.subspace();

        // Update Alice with 4 elements, 2 unique (java x3, kotlin x1)
        BsonArray aliceSkills = new BsonArray();
        aliceSkills.add(new BsonString("java"));
        aliceSkills.add(new BsonString("java"));
        aliceSkills.add(new BsonString("kotlin"));
        aliceSkills.add(new BsonString("java"));

        PipelineNode alicePlan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions aliceUpdate = UpdateOptions.builder().set("skills.name", aliceSkills).build();
        QueryOptions aliceOptions = QueryOptions.builder().update(aliceUpdate).build();
        QueryContext aliceCtx = new QueryContext(metadata, aliceOptions, alicePlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, aliceCtx);
            tr.commit().join();
        }

        // Verify 2 entries for Alice + 2 null entries for Bob and Charlie = 4 total
        assertEquals(4, fetchAllIndexedEntries(skillsSubspace).size(), "Should have 4 entries after Alice update");

        // Update Bob with 5 elements, 3 unique (python x2, rust x2, go x1)
        BsonArray bobSkills = new BsonArray();
        bobSkills.add(new BsonString("python"));
        bobSkills.add(new BsonString("rust"));
        bobSkills.add(new BsonString("python"));
        bobSkills.add(new BsonString("go"));
        bobSkills.add(new BsonString("rust"));

        PipelineNode bobPlan = createExecutionPlan(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("skills.name", bobSkills).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(metadata, bobOptions, bobPlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, bobCtx);
            tr.commit().join();
        }

        // Verify 2 (Alice) + 3 (Bob) + 1 (Charlie null) = 6 total
        assertEquals(6, fetchAllIndexedEntries(skillsSubspace).size(), "Should have 6 entries after Bob update");

        // Update Charlie with 3 elements, 1 unique (scala x3)
        BsonArray charlieSkills = new BsonArray();
        charlieSkills.add(new BsonString("scala"));
        charlieSkills.add(new BsonString("scala"));
        charlieSkills.add(new BsonString("scala"));

        PipelineNode charliePlan = createExecutionPlan(metadata, "{name: {$eq: \"Charlie\"}}");
        UpdateOptions charlieUpdate = UpdateOptions.builder().set("skills.name", charlieSkills).build();
        QueryOptions charlieOptions = QueryOptions.builder().update(charlieUpdate).build();
        QueryContext charlieCtx = new QueryContext(metadata, charlieOptions, charliePlan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, charlieCtx);
            tr.commit().join();
        }

        // Verify final state: 2 (Alice) + 3 (Bob) + 1 (Charlie) = 6 total entries
        List<KeyValue> finalEntries = fetchAllIndexedEntries(skillsSubspace);
        assertEquals(6, finalEntries.size(), "Should have 6 entries total");

        // Verify all unique values are present
        Set<String> indexedValues = extractStringValuesFromIndex(skillsSubspace);
        assertTrue(indexedValues.contains("java"), "Should have 'java' indexed");
        assertTrue(indexedValues.contains("kotlin"), "Should have 'kotlin' indexed");
        assertTrue(indexedValues.contains("python"), "Should have 'python' indexed");
        assertTrue(indexedValues.contains("rust"), "Should have 'rust' indexed");
        assertTrue(indexedValues.contains("go"), "Should have 'go' indexed");
        assertTrue(indexedValues.contains("scala"), "Should have 'scala' indexed");
        assertEquals(6, indexedValues.size(), "Should have exactly 6 unique values");

        // Verify cardinality
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(skillsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(6L, stats.cardinality(), "Final cardinality should be 6");
        }
    }

    private Set<String> extractStringValuesFromIndex(DirectorySubspace indexSubspace) {
        Set<String> values = new HashSet<>();
        List<KeyValue> entries = fetchAllIndexedEntries(indexSubspace);
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));

        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            Object value = keyTuple.get(0);
            if (value instanceof String) {
                values.add((String) value);
            }
        }
        return values;
    }

    @Test
    void shouldDropMultikeyIndexEntriesWhenArrayItemsRemoved() {
        // Create a multikey index on "tags.name"
        IndexDefinition tagsIndex = IndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, tagsIndex);

        // Insert a document with tags array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"scala\"}]}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Verify initial state: 3 entries (java, kotlin, scala)
        assertEquals(3, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 3 entries initially");
        Set<String> initialValues = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(initialValues.contains("java"), "Should have 'java' initially");
        assertTrue(initialValues.contains("kotlin"), "Should have 'kotlin' initially");
        assertTrue(initialValues.contains("scala"), "Should have 'scala' initially");

        // Update Alice to have only 1 tag (removing kotlin and scala)
        BsonArray reducedTags = new BsonArray();
        reducedTags.add(new BsonString("java"));

        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags.name", reducedTags).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify only 1 entry remains (java)
        List<KeyValue> entriesAfterUpdate = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(1, entriesAfterUpdate.size(), "Tags index should have 1 entry after update");

        Set<String> valuesAfterUpdate = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesAfterUpdate.contains("java"), "Should still have 'java'");
        assertFalse(valuesAfterUpdate.contains("kotlin"), "Should NOT have 'kotlin' after update");
        assertFalse(valuesAfterUpdate.contains("scala"), "Should NOT have 'scala' after update");

        // Verify back pointers are also cleaned up
        assertEquals(1, fetchAllIndexBackPointers(tagsSubspace).size(), "Should have 1 back pointer after update");

        // Verify cardinality is updated correctly: 3 entries dropped, 1 entry added = 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after reducing array to single item");
        }
    }

    @Test
    void shouldDropAllMultikeyIndexEntriesWhenFieldUnset() {
        // Create a multikey index on "tags.name"
        IndexDefinition tagsIndex = IndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, tagsIndex);

        // Insert a document with tags array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"scala\"}]}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Verify initial state: 3 entries (java, kotlin, scala)
        assertEquals(3, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 3 entries initially");
        assertEquals(3, fetchAllIndexBackPointers(tagsSubspace).size(), "Should have 3 back pointers initially");

        Set<String> initialValues = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(initialValues.contains("java"), "Should have 'java' initially");
        assertTrue(initialValues.contains("kotlin"), "Should have 'kotlin' initially");
        assertTrue(initialValues.contains("scala"), "Should have 'scala' initially");

        // Unset the tags.name field entirely
        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().unset("tags.name").build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify all entries are dropped and replaced with a single null entry
        List<KeyValue> entriesAfterUnset = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(1, entriesAfterUnset.size(), "Tags index should have 1 null entry after unset");

        // Verify no string values remain (only null)
        Set<String> valuesAfterUnset = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesAfterUnset.isEmpty(), "Should have no string values after unset");

        // Verify back pointers are cleaned up to a single null entry
        assertEquals(1, fetchAllIndexBackPointers(tagsSubspace).size(), "Should have 1 back pointer after unset");

        // Verify document can be found via null query
        List<String> nullQuery = runQueryOnBucket(metadata, "{\"tags.name\": {$eq: null}}");
        assertEquals(1, nullQuery.size(), "Should find 1 document with null tags.name");
        assertTrue(nullQuery.getFirst().contains("\"name\": \"Alice\""), "Result should be Alice");

        // Verify cardinality is updated correctly: 3 entries dropped, 1 null entry added = 1
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after unset (single null entry)");
        }
    }
}