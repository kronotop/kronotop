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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UpdateExecutorTest extends BasePipelineTest {

    @Test
    void shouldUpdateAllAffectedIndexesWhenFieldModified() {
        // Behavior: Updating an indexed field removes the old index entry and creates a new one.
        // The document becomes queryable by the new value while the old value returns no results.

        // Create two secondary indexes
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get index subspaces
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
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
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(40)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> updatedObjectIds;
        try (Transaction tr = createTransaction()) {
            updatedObjectIds = updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

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

        // Verify the document can be queried with a new value
        List<String> bobQuery = runQueryOnBucket(metadata, "{age: {$eq: 40}}");
        assertEquals(1, bobQuery.size(), "Should find Bob with age 40");
        assertTrue(bobQuery.getFirst().contains("\"name\": \"Bob\""), "Result should be Bob");

        // Verify old value returns no results
        List<String> oldAgeQuery = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertTrue(oldAgeQuery.isEmpty(), "Should not find any document with age 30");
    }

    @Test
    void shouldUpdateMultipleIndexedFieldsSimultaneously() {
        // Behavior: Updating multiple indexed fields in one operation updates all affected indexes.
        // Both old entries are removed and new entries are created atomically.

        // Create two secondary indexes
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

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
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("age", new BsonInt32(50))
                .set("name", new BsonString("Robert"))
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        // Behavior: Documents missing an indexed field have null entries in the index. When the
        // field is added via update, the null entry is replaced with the actual value.

        // Create an INT32 index on age field BEFORE inserting documents
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Insert documents WITHOUT the indexed field
        // Note: Missing fields are indexed as null entries
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

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
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(25)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        PlanWithParams bobPlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("age", new BsonInt32(30)).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(getSession(), metadata, bobOptions, bobPlanWithParams.plan(), bobPlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        // Behavior: Nested field indexes (e.g., "a.b.c") also use null entries for missing paths.
        // Adding the nested field via $set replaces the null entry with the actual value.

        final String TEST_BUCKET = "test-bucket-nested-field-update";

        // Create an INT32 index on nested field "a.b.c" BEFORE inserting documents
        SingleFieldIndexDefinition nestedIndex = SingleFieldIndexDefinition.create("nested_idx", "a.b.c", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, nestedIndex);

        // Insert documents WITHOUT the nested indexed field
        // Note: Missing fields are indexed as null entries
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"a\": {\"x\": 1}}")  // has "a" but not "a.b.c"
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

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
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("a.b.c", new BsonInt32(100)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        PlanWithParams bobPlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("a.b.c", new BsonInt32(200)).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(getSession(), metadata, bobOptions, bobPlanWithParams.plan(), bobPlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
            // First, the element is the indexed value
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
            // First, the element is the indexed value
            values.add(keyTuple.getString(0));
        }
        return values;
    }

    @Test
    void shouldCreateMultikeyIndexEntriesWhenUpdatingToArrayValue() {
        // Behavior: Setting an array value on a multiKey index creates one entry per array element.
        // The null entry is removed and replaced with entries for each array value.

        // Create a multikey index on "tags.name"
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert a document without the tags field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

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

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags.name", tagsArray).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        // Behavior: Duplicate values in an array are deduplicated in the multiKey index.
        // Only unique values get index entries, preventing redundant storage.

        // Create a multikey index on "tags.name"
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert a document without the tags field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Update Alice with tags array containing duplicates
        BsonArray tagsArray = new BsonArray();
        tagsArray.add(new BsonString("java"));
        tagsArray.add(new BsonString("java"));  // duplicate
        tagsArray.add(new BsonString("kotlin"));
        tagsArray.add(new BsonString("java"));  // duplicate

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags.name", tagsArray).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        // Behavior: Index cardinality tracks the total number of index entries including multiKey.
        // Updates adjust cardinality by removing old entries count and adding new entries count.

        // Create a multikey index on "langs.name"
        SingleFieldIndexDefinition langsIndex = SingleFieldIndexDefinition.create("langs_idx", "langs.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, langsIndex);

        // Insert two documents without the langs field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Update Alice with 2 unique langs
        BsonArray aliceLangs = new BsonArray();
        aliceLangs.add(new BsonString("java"));
        aliceLangs.add(new BsonString("kotlin"));

        PlanWithParams alicePlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions aliceUpdate = UpdateOptions.builder().set("langs.name", aliceLangs).build();
        QueryOptions aliceOptions = QueryOptions.builder().update(aliceUpdate).build();
        QueryContext aliceCtx = new QueryContext(getSession(), metadata, aliceOptions, alicePlanWithParams.plan(), alicePlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, aliceCtx);
            tr.commit().join();
        }

        // Verify cardinality after the first update: 2 (from Alice) + 1 (null from Bob) = 3
        try (Transaction tr = createTransaction()) {
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

        PlanWithParams bobPlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("langs.name", bobLangs).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(getSession(), metadata, bobOptions, bobPlanWithParams.plan(), bobPlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, bobCtx);
            tr.commit().join();
        }

        // Verify cardinality after second update: 2 (Alice) + 3 (Bob) = 5
        try (Transaction tr = createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(langsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(5L, stats.cardinality(), "Cardinality should be 5 after second update");
        }
    }

    @Test
    void shouldDeduplicateAndTrackCardinalityCorrectlyForMultikeyIndexUpdate() {
        // Behavior: MultiKey index updates with duplicates deduplicate entries and track cardinality
        // correctly. Each document contributes unique values count to the total cardinality.

        // Create a multikey index on "skills.name"
        SingleFieldIndexDefinition skillsIndex = SingleFieldIndexDefinition.create("skills_idx", "skills.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, skillsIndex);

        // Insert documents without skills field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        Index skillsIdx = metadata.indexes().getIndex("skills.name", IndexSelectionPolicy.READ);
        DirectorySubspace skillsSubspace = skillsIdx.subspace();

        // Update Alice with 4 elements, 2 unique (java x3, kotlin x1)
        BsonArray aliceSkills = new BsonArray();
        aliceSkills.add(new BsonString("java"));
        aliceSkills.add(new BsonString("java"));
        aliceSkills.add(new BsonString("kotlin"));
        aliceSkills.add(new BsonString("java"));

        PlanWithParams alicePlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions aliceUpdate = UpdateOptions.builder().set("skills.name", aliceSkills).build();
        QueryOptions aliceOptions = QueryOptions.builder().update(aliceUpdate).build();
        QueryContext aliceCtx = new QueryContext(getSession(), metadata, aliceOptions, alicePlanWithParams.plan(), alicePlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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

        PlanWithParams bobPlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions bobUpdate = UpdateOptions.builder().set("skills.name", bobSkills).build();
        QueryOptions bobOptions = QueryOptions.builder().update(bobUpdate).build();
        QueryContext bobCtx = new QueryContext(getSession(), metadata, bobOptions, bobPlanWithParams.plan(), bobPlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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

        PlanWithParams charliePlanWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Charlie\"}}");
        UpdateOptions charlieUpdate = UpdateOptions.builder().set("skills.name", charlieSkills).build();
        QueryOptions charlieOptions = QueryOptions.builder().update(charlieUpdate).build();
        QueryContext charlieCtx = new QueryContext(getSession(), metadata, charlieOptions, charliePlanWithParams.plan(), charliePlanWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        try (Transaction tr = createTransaction()) {
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

    // ==================== UPSERT TESTS ====================

    @Test
    void shouldUpsertWithPrimaryAndSecondaryIndexes() {
        // Behavior: Upsert creates document and maintains both primary and secondary indexes.
        // The document is queryable by both _id and indexed field values.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // No documents exist - upsert should create one
        String query = "{name: {$eq: \"Alice\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("Alice"))
                .set("age", new BsonInt32(25))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, results.size());
        assertTrue(results.getFirst().contains("\"name\": \"Alice\""));
        assertTrue(results.getFirst().contains("\"age\": 25"));

        // Verify primary index entry created
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        assertEquals(1, fetchAllIndexedEntries(primaryIndex.subspace()).size(), "Primary index should have 1 entry");

        // Verify secondary index entry created
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Set<Integer> ageValues = extractAgeValuesFromIndex(ageIdx.subspace());
        assertTrue(ageValues.contains(25), "Age index should contain 25");

        // Verify upsertResult is set
        assertNotNull(ctx.upsertResult());
    }

    @Test
    void shouldUpsertWithMultikeyIndex() {
        // Behavior: Upsert with array value on multikey-indexed field creates
        // multiple index entries (one per unique array element).

        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        BsonArray tags = new BsonArray();
        tags.add(new BsonString("java"));
        tags.add(new BsonString("kotlin"));
        tags.add(new BsonString("scala"));

        String query = "{name: {$eq: \"Developer\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("Developer"))
                .set("tags", tags)
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Developer\"}}");
        assertEquals(1, results.size());

        // Verify 3 index entries created (java, kotlin, scala)
        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        List<KeyValue> entries = fetchAllIndexedEntries(tagsIdx.subspace());
        assertEquals(3, entries.size(), "Tags index should have 3 entries for multikey upsert");

        Set<String> indexedValues = extractStringValuesFromIndex(tagsIdx.subspace());
        assertTrue(indexedValues.contains("java"), "Should have 'java' indexed");
        assertTrue(indexedValues.contains("kotlin"), "Should have 'kotlin' indexed");
        assertTrue(indexedValues.contains("scala"), "Should have 'scala' indexed");
    }

    @Test
    void shouldCreateNullIndexEntriesForMissingFieldsOnUpsert() {
        // Behavior: When upsert creates a document that's missing an indexed field,
        // a null index entry is created so the document is queryable via null queries.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Upsert document WITHOUT the indexed "age" field
        String query = "{name: {$eq: \"NoAge\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("NoAge"))
                // Note: NOT setting "age"
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify null index entry created
        List<String> nullResults = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(1, nullResults.size());
        assertTrue(nullResults.getFirst().contains("\"name\": \"NoAge\""));

        // Verify document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"NoAge\"}}");
        assertEquals(1, results.size());
    }

    @Test
    void shouldCreateNullIndexEntryForExplicitlyNullFieldOnUpsert() {
        // Behavior: When upsert creates a document with an indexed field explicitly set to null,
        // a null index entry is created so the document is queryable via null queries.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Upsert document WITH indexed "age" field explicitly set to null
        String query = "{name: {$eq: \"ExplicitNull\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("ExplicitNull"))
                .set("age", BsonNull.VALUE)
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify index entry and back pointer created
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();
        assertEquals(1, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 1 entry");
        assertEquals(1, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 1 back pointer");

        // Verify the index entry is a null entry
        List<KeyValue> entries = fetchAllIndexedEntries(ageSubspace);
        assertNotNull(entries);
        assertFalse(entries.isEmpty());
        byte[] prefix = ageSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entries.getFirst().getKey(), prefix.length, entries.getFirst().getKey().length - prefix.length);
        assertNull(keyTuple.get(0), "Index entry should have null as the indexed value");

        // Verify document can be found via null query
        List<String> nullResults = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(1, nullResults.size());
        assertTrue(nullResults.getFirst().contains("\"name\": \"ExplicitNull\""));

        // Verify document was created with explicit null
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"ExplicitNull\"}}");
        assertEquals(1, results.size());
        assertTrue(results.getFirst().contains("\"age\": null"), "Document should have age field explicitly set to null");
    }

    @Test
    void shouldIgnoreUnsetOnUpsert() {
        // Behavior: $unset operations are ignored during upsert since there's
        // no existing document to remove fields from.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        String query = "{name: {$eq: \"Test\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("Test"))
                .set("x", new BsonInt32(1))
                .unset("nonexistent") // Should be ignored
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify document created with $set fields
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Test\"}}");
        assertEquals(1, results.size());
        assertTrue(results.getFirst().contains("\"x\": 1"));
    }

    @Test
    void shouldNotExtractEqualityFromOrQuery() {
        // Behavior: $or queries don't contribute equality conditions to upsert.
        // Only explicit $eq or implicit equality operators are extracted.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        // Query with $or - should NOT extract "a" or "b"
        String query = "{$or: [{a: 1}, {b: 2}]}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("created", new BsonString("true"))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Document created with ONLY $set fields (no a or b)
        List<String> results = runQueryOnBucket(metadata, "{created: {$eq: \"true\"}}");
        assertEquals(1, results.size());
        assertFalse(results.getFirst().contains("\"a\""), "Field 'a' should not be in document from $or");
        assertFalse(results.getFirst().contains("\"b\""), "Field 'b' should not be in document from $or");
    }

    @Test
    void shouldUpsertWithMultipleSecondaryIndexes() {
        // Behavior: Upsert with multiple secondary indexes creates entries in all indexes.
        // Each index gets its own entry based on the indexed field value.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition cityIndex = SingleFieldIndexDefinition.create("city_idx", "city", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex, cityIndex);

        String query = "{status: {$eq: \"active\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("status", new BsonString("active"))
                .set("name", new BsonString("Alice"))
                .set("age", new BsonInt32(30))
                .set("city", new BsonString("Berlin"))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify all indexes have entries
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Index nameIdx = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);
        Index cityIdx = metadata.indexes().getIndex("city", IndexSelectionPolicy.READ);

        assertEquals(1, fetchAllIndexedEntries(ageIdx.subspace()).size(), "Age index should have 1 entry");
        assertEquals(1, fetchAllIndexedEntries(nameIdx.subspace()).size(), "Name index should have 1 entry");
        assertEquals(1, fetchAllIndexedEntries(cityIdx.subspace()).size(), "City index should have 1 entry");

        // Verify indexed values
        Set<Integer> ageValues = extractAgeValuesFromIndex(ageIdx.subspace());
        assertTrue(ageValues.contains(30), "Age index should contain 30");

        Set<String> nameValues = extractNameValuesFromIndex(nameIdx.subspace());
        assertTrue(nameValues.contains("Alice"), "Name index should contain Alice");

        Set<String> cityValues = extractStringValuesFromIndex(cityIdx.subspace());
        assertTrue(cityValues.contains("Berlin"), "City index should contain Berlin");

        // Verify document is queryable via all indexes
        assertEquals(1, runQueryOnBucket(metadata, "{age: {$eq: 30}}").size());
        assertEquals(1, runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}").size());
        assertEquals(1, runQueryOnBucket(metadata, "{city: {$eq: \"Berlin\"}}").size());
    }

    @Test
    void shouldUpsertWithMultikeyIndexAndExplicitNullArray() {
        // Behavior: Upsert with a multikey-indexed array field explicitly set to null
        // creates a single null index entry.

        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        String query = "{name: {$eq: \"NullTags\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("NullTags"))
                .set("tags", BsonNull.VALUE)
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify single null index entry
        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();
        List<KeyValue> entries = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(1, entries.size(), "Tags index should have 1 entry for null");

        // Verify it's a null entry
        byte[] prefix = tagsSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entries.getFirst().getKey(), prefix.length, entries.getFirst().getKey().length - prefix.length);
        assertNull(keyTuple.get(0), "Index entry should have null as the indexed value");

        // Verify document queryable via null
        List<String> nullResults = runQueryOnBucket(metadata, "{tags: {$eq: null}}");
        assertEquals(1, nullResults.size());
        assertTrue(nullResults.getFirst().contains("\"name\": \"NullTags\""));
    }

    @Test
    void shouldUpsertWithConsecutiveUpsertTransitioningNullToValue() {
        // Behavior: First upsert creates a document with missing indexed field (null entry).
        // Second upsert to the same document adds the field, replacing null entry with actual value.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // First upsert: create the document WITHOUT an age field
        String query = "{name: {$eq: \"Transitioning\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update1 = UpdateOptions.builder()
                .set("name", new BsonString("Transitioning"))
                .upsert(true)
                .build();
        QueryOptions options1 = QueryOptions.builder().update(update1).build();
        QueryContext ctx1 = new QueryContext(getSession(), metadata, options1, planWithParams.plan(), planWithParams.parameters());
        ctx1.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx1);
            tr.commit().join();
        }

        // Verify null index entry exists
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();
        assertEquals(1, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 1 null entry");

        List<String> nullResults = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(1, nullResults.size(), "Should find document with null age");

        // Second upsert: match existing document and ADD age field
        PlanWithParams planWithParams2 = createPlanWithParams(metadata, query);
        UpdateOptions update2 = UpdateOptions.builder()
                .set("age", new BsonInt32(25))
                .upsert(true)
                .build();
        QueryOptions options2 = QueryOptions.builder().update(update2).build();
        QueryContext ctx2 = new QueryContext(getSession(), metadata, options2, planWithParams2.plan(), planWithParams2.parameters());
        ctx2.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx2);
            tr.commit().join();
        }

        // Verify null entry replaced with actual value
        assertEquals(1, fetchAllIndexedEntries(ageSubspace).size(), "Age index should still have 1 entry");
        Set<Integer> ageValues = extractAgeValuesFromIndex(ageSubspace);
        assertTrue(ageValues.contains(25), "Age index should contain 25");

        // Verify null query returns nothing
        List<String> nullAfterUpdate = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertTrue(nullAfterUpdate.isEmpty(), "No documents should have null age after update");

        // Verify document queryable by actual value
        List<String> ageResults = runQueryOnBucket(metadata, "{age: {$eq: 25}}");
        assertEquals(1, ageResults.size());
        assertTrue(ageResults.getFirst().contains("\"name\": \"Transitioning\""));
    }

    @Test
    void shouldUpsertWithDeeplyNestedIndexAndPartialPath() {
        // Behavior: Upsert with index on deeply nested path (a.b.c.d) when document
        // only has a partial path (a.x) creates null index entry for missing nested field.

        SingleFieldIndexDefinition deepIndex = SingleFieldIndexDefinition.create("deep_idx", "a.b.c.d", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, deepIndex);

        // Upsert with partial path - has "a" but not "a.b.c.d"
        String query = "{name: {$eq: \"PartialPath\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("PartialPath"))
                .set("a.x", new BsonInt32(100))  // Different nested path
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify null index entry for missing a deep path
        Index deepIdx = metadata.indexes().getIndex("a.b.c.d", IndexSelectionPolicy.READ);
        DirectorySubspace deepSubspace = deepIdx.subspace();
        List<KeyValue> entries = fetchAllIndexedEntries(deepSubspace);
        assertEquals(1, entries.size(), "Deep index should have 1 entry");

        byte[] prefix = deepSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entries.getFirst().getKey(), prefix.length, entries.getFirst().getKey().length - prefix.length);
        assertNull(keyTuple.get(0), "Deep index entry should be null (path doesn't exist)");

        // Verify document queryable via null on deep path
        List<String> nullResults = runQueryOnBucket(metadata, "{\"a.b.c.d\": {$eq: null}}");
        assertEquals(1, nullResults.size());
        assertTrue(nullResults.getFirst().contains("\"name\": \"PartialPath\""));

        // Verify the document has the partial path that was set
        List<String> docResults = runQueryOnBucket(metadata, "{\"a.x\": {$eq: 100}}");
        assertEquals(1, docResults.size());
    }

    @Test
    void shouldNotExtractIdFieldDuringUpsert() {
        // Behavior: The _id field is reserved and auto-generated. When upserting,
        // _id equality conditions from the query should NOT be extracted into the document.
        // This prevents creating documents where a user-provided _id field differs from
        // the actual primary key (ObjectId).

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        // Create a fake ObjectId for the query
        ObjectId fakeObjectId = new ObjectId();
        String encodedFakeId = fakeObjectId.toHexString();

        // Upsert with _id in the query AND another field - _id should be ignored
        String query = String.format("{_id: {$eq: \"%s\"}, status: {$eq: \"active\"}}", encodedFakeId);
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("IdSkipTest"))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify upsert was triggered
        assertNotNull(ctx.upsertResult(), "Upsert should have been triggered");

        // Query all documents
        List<String> allDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(1, allDocs.size(), "One document should exist");

        String docJson = allDocs.getFirst();

        // Verify _id field was NOT extracted from the query
        assertFalse(docJson.contains("\"_id\""), "Document should NOT have _id as a field (it's reserved)");

        // Verify other equality condition WAS extracted
        assertTrue(docJson.contains("\"status\": \"active\""), "Document should have status from query equality");

        // Verify $set field was applied
        assertTrue(docJson.contains("\"name\": \"IdSkipTest\""), "Document should have name from $set");
    }

    @Test
    void shouldDropMultikeyIndexEntriesWhenArrayItemsRemoved() {
        // Behavior: Reducing array size via update removes excess index entries and back pointers.
        // Cardinality is adjusted to reflect the new smaller array.

        // Create a multikey index on "tags.name"
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert a document with tags array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"scala\"}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

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
        reducedTags.add(new BsonDocument().append("name", new BsonString("java")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags", reducedTags).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        try (Transaction tr = createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after reducing array to single item");
        }
    }

    @Test
    void shouldCreateNullIndexEntryWhenUnsetOnSecondaryIndexedField() {
        // Behavior: $unset on a regular (non-array) secondary indexed field removes the
        // existing value entry and creates a null index entry. The document becomes
        // queryable via {field: {$eq: null}}.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex);

        // Insert a document with the age field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Verify the initial state: index has entry for age=30
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();
        assertEquals(1, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 1 entry initially");

        Set<Integer> initialValues = extractAgeValuesFromIndex(ageSubspace);
        assertTrue(initialValues.contains(30), "Age index should contain 30");

        // Verify the document is queryable by age
        List<String> byAge = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertEquals(1, byAge.size(), "Should find document by age=30");

        // $unset the age field
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .unset("age")
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify index entry replaced with null
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(ageSubspace);
        assertEquals(1, entriesAfter.size(), "Age index should still have 1 entry (null)");

        // Verify it's a null entry (not an integer value)
        byte[] prefix = ageSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entriesAfter.getFirst().getKey(), prefix.length, entriesAfter.getFirst().getKey().length - prefix.length);
        assertNull(keyTuple.get(0), "Index entry should be null after $unset");

        // Verify the old value (30) is no longer in the index
        boolean foundOldValue = false;
        for (KeyValue kv : entriesAfter) {
            Tuple tuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            if (tuple.get(0) != null) {
                foundOldValue = true;
                break;
            }
        }
        assertFalse(foundOldValue, "Old value should not be in the index after $unset");

        // Verify the document no longer queryable by age=30
        List<String> byAgeAfter = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertTrue(byAgeAfter.isEmpty(), "Should NOT find document by age=30 after $unset");

        // Verify the document is now queryable by age=null
        List<String> byNull = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(1, byNull.size(), "Should find document by age=null after $unset");
        assertTrue(byNull.getFirst().contains("\"name\": \"Alice\""), "Should be Alice's document");
        assertFalse(byNull.getFirst().contains("\"age\""), "Document should not have age field");
    }

    @Test
    void shouldDropAllMultikeyIndexEntriesWhenFieldUnset() {
        // Behavior: Using $unset on a multiKey field removes all array entries and replaces them
        // with a single null entry. The document becomes queryable via null query.

        // Create a multikey index on "tags.name"
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert a document with tags array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}, {\"name\": \"scala\"}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

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
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().unset("tags.$[].name").build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
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
        try (Transaction tr = createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            IndexStatistics stats = indexStatistics.get(tagsIndex.id());
            assertNotNull(stats, "Index statistics should exist");
            assertEquals(1L, stats.cardinality(), "Cardinality should be 1 after unset (single null entry)");
        }
    }

    @Test
    void shouldMaintainMultikeyIndexWhenUpdatingArrayElementsWithArrayFilters() {
        // Behavior: When updating array elements using array_filters (positional $[identifier]),
        // the multi-key index should be correctly maintained. Old index entries for modified values
        // should be removed, and new entries for the updated array should be created.

        // Create a multikey index on "scores"
        SingleFieldIndexDefinition scoresIndex = SingleFieldIndexDefinition.create("scores_idx", "scores", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, scoresIndex);

        // Insert a document with scores array [55, 60, 65, 70, 75]
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"scores\": [55, 60, 65, 70, 75]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get scores index subspace
        Index scoresIdx = metadata.indexes().getIndex("scores", IndexSelectionPolicy.READ);
        DirectorySubspace scoresSubspace = scoresIdx.subspace();

        // Verify initial state: 5 entries (55, 60, 65, 70, 75)
        List<KeyValue> entriesBefore = fetchAllIndexedEntries(scoresSubspace);
        assertEquals(5, entriesBefore.size(), "Scores index should have 5 entries initially");

        Set<Integer> valuesBefore = extractIntValuesFromIndex(scoresSubspace);
        assertTrue(valuesBefore.contains(55), "Index should contain 55 before update");
        assertTrue(valuesBefore.contains(60), "Index should contain 60 before update");
        assertTrue(valuesBefore.contains(65), "Index should contain 65 before update");
        assertTrue(valuesBefore.contains(70), "Index should contain 70 before update");
        assertTrue(valuesBefore.contains(75), "Index should contain 75 before update");

        // Update: change elements <= 60 to 0 using array_filters
        // This should change [55, 60, 65, 70, 75] to [0, 0, 65, 70, 75]
        ArrayFilter lowFilter = new ArrayFilter("low", Operator.LTE, 60);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("scores.$[low]", new BsonInt32(0))
                .arrayFilter(lowFilter)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly
        List<String> queryResult = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, queryResult.size(), "Should find Alice");
        String aliceDoc = queryResult.getFirst();
        assertTrue(aliceDoc.contains("\"scores\": [0, 0, 65, 70, 75]"), "Document should have updated scores array");

        // Verify index entries after update:
        // Expected: 4 entries (0 deduplicated, 65, 70, 75)
        // Old entries for 55 and 60 should be removed
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(scoresSubspace);
        assertEquals(4, entriesAfter.size(), "Scores index should have 4 entries after update (0, 65, 70, 75)");

        Set<Integer> valuesAfter = extractIntValuesFromIndex(scoresSubspace);
        assertTrue(valuesAfter.contains(0), "Index should contain 0 after update");
        assertTrue(valuesAfter.contains(65), "Index should still contain 65 after update");
        assertTrue(valuesAfter.contains(70), "Index should still contain 70 after update");
        assertTrue(valuesAfter.contains(75), "Index should still contain 75 after update");
        assertFalse(valuesAfter.contains(55), "Index should NOT contain 55 after update");
        assertFalse(valuesAfter.contains(60), "Index should NOT contain 60 after update");

        // Verify queries via index work correctly
        // Query for scores = 65 should return Alice
        List<String> query65 = runQueryOnBucket(metadata, "{scores: {$eq: 65}}");
        assertEquals(1, query65.size(), "Query for scores=65 should find Alice via index");

        // Query for scores = 55 should return nothing (old value removed)
        List<String> query55 = runQueryOnBucket(metadata, "{scores: {$eq: 55}}");
        assertEquals(0, query55.size(), "Query for scores=55 should find nothing (value was changed)");
    }

    @Test
    void shouldMaintainMultikeyIndexWhenUnsetWithPositionalOperator() {
        // Behavior: $unset with positional operator ($[identifier]) removes matching elements from
        // the array and updates the multi-key index accordingly.

        // Create a multikey index on "scores"
        SingleFieldIndexDefinition scoresIndex = SingleFieldIndexDefinition.create("scores_idx", "scores", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, scoresIndex);

        // Insert a document with scores array [55, 60, 65, 70, 75]
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"scores\": [55, 60, 65, 70, 75]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get scores index subspace
        Index scoresIdx = metadata.indexes().getIndex("scores", IndexSelectionPolicy.READ);
        DirectorySubspace scoresSubspace = scoresIdx.subspace();

        // Verify initial state: 5 entries (55, 60, 65, 70, 75)
        assertEquals(5, fetchAllIndexedEntries(scoresSubspace).size(), "Scores index should have 5 entries initially");

        // Unset (remove) elements <= 60 using array_filters
        // This should change [55, 60, 65, 70, 75] to [65, 70, 75]
        ArrayFilter lowFilter = new ArrayFilter("low", Operator.LTE, 60);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .unset("scores.$[low]")
                .arrayFilter(lowFilter)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly
        List<String> queryResult = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, queryResult.size(), "Should find Alice");
        String aliceDoc = queryResult.getFirst();
        assertTrue(aliceDoc.contains("\"scores\": [65, 70, 75]"), "Document should have scores with low values removed");

        // Verify index entries after update: 3 entries (65, 70, 75)
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(scoresSubspace);
        assertEquals(3, entriesAfter.size(), "Scores index should have 3 entries after unset");

        Set<Integer> valuesAfter = extractIntValuesFromIndex(scoresSubspace);
        assertTrue(valuesAfter.contains(65), "Index should contain 65");
        assertTrue(valuesAfter.contains(70), "Index should contain 70");
        assertTrue(valuesAfter.contains(75), "Index should contain 75");
        assertFalse(valuesAfter.contains(55), "Index should NOT contain 55");
        assertFalse(valuesAfter.contains(60), "Index should NOT contain 60");
    }

    @Test
    void shouldMaintainMultikeyIndexWithAllPositionalOperator() {
        // Behavior: Using $[] (all positional operator) to update all array elements
        // correctly maintains the multi-key index by replacing all entries with the new value.

        // Create a multikey index on "scores"
        SingleFieldIndexDefinition scoresIndex = SingleFieldIndexDefinition.create("scores_idx", "scores", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, scoresIndex);

        // Insert a document with scores array [55, 60, 65, 70, 75]
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"scores\": [55, 60, 65, 70, 75]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get scores index subspace
        Index scoresIdx = metadata.indexes().getIndex("scores", IndexSelectionPolicy.READ);
        DirectorySubspace scoresSubspace = scoresIdx.subspace();

        // Verify initial state: 5 entries (55, 60, 65, 70, 75)
        assertEquals(5, fetchAllIndexedEntries(scoresSubspace).size(), "Scores index should have 5 entries initially");

        // Update all elements to 100 using $[]
        // This should change [55, 60, 65, 70, 75] to [100, 100, 100, 100, 100]
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("scores.$[]", new BsonInt32(100))
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly
        List<String> queryResult = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, queryResult.size(), "Should find Alice");
        String aliceDoc = queryResult.getFirst();
        assertTrue(aliceDoc.contains("\"scores\": [100, 100, 100, 100, 100]"), "All scores should be 100");

        // Verify index entries after update: 1 entry (100, deduplicated)
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(scoresSubspace);
        assertEquals(1, entriesAfter.size(), "Scores index should have 1 entry after update (deduplicated)");

        Set<Integer> valuesAfter = extractIntValuesFromIndex(scoresSubspace);
        assertTrue(valuesAfter.contains(100), "Index should contain 100");
        assertFalse(valuesAfter.contains(55), "Index should NOT contain 55");
        assertFalse(valuesAfter.contains(60), "Index should NOT contain 60");
        assertFalse(valuesAfter.contains(65), "Index should NOT contain 65");
        assertFalse(valuesAfter.contains(70), "Index should NOT contain 70");
        assertFalse(valuesAfter.contains(75), "Index should NOT contain 75");

        // Verify querying by new value works
        List<String> query100 = runQueryOnBucket(metadata, "{scores: {$eq: 100}}");
        assertEquals(1, query100.size(), "Query for scores=100 should find Alice via index");
    }

    @Test
    void shouldMaintainNestedMultikeyIndexWithPositionalOperator() {
        // Behavior: Updating nested field within array elements using positional operators
        // correctly maintains the multi-key index on the nested field path.

        // Create a multikey index on "items.price"
        SingleFieldIndexDefinition pricesIndex = SingleFieldIndexDefinition.create("prices_idx", "items.price", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, pricesIndex);

        // Insert a document with items array containing objects with the price field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Order1\", \"items\": [{\"name\": \"A\", \"price\": 10}, {\"name\": \"B\", \"price\": 20}, {\"name\": \"C\", \"price\": 30}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get prices index subspace
        Index pricesIdx = metadata.indexes().getIndex("items.price", IndexSelectionPolicy.READ);
        DirectorySubspace pricesSubspace = pricesIdx.subspace();

        // Verify initial state: 3 entries (10, 20, 30)
        List<KeyValue> entriesBefore = fetchAllIndexedEntries(pricesSubspace);
        assertEquals(3, entriesBefore.size(), "Prices index should have 3 entries initially");

        Set<Integer> valuesBefore = extractIntValuesFromIndex(pricesSubspace);
        assertTrue(valuesBefore.contains(10), "Index should contain 10 before update");
        assertTrue(valuesBefore.contains(20), "Index should contain 20 before update");
        assertTrue(valuesBefore.contains(30), "Index should contain 30 before update");

        // Update: set prices to 50 for items where price >= 20 using array_filters
        // This should change prices from [10, 20, 30] to [10, 50, 50]
        // ArrayFilter("expensive.price", ...) - the builder extracts "expensive" as map key
        // to match $[expensive], while "expensive.price" is used to extract the comparison value
        ArrayFilter expensiveFilter = new ArrayFilter("expensive.price", Operator.GTE, 20);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Order1\"}}");

        UpdateOptions update = UpdateOptions.builder()
                .set("items.$[expensive].price", new BsonInt32(50))
                .arrayFilter(expensiveFilter)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly
        List<String> queryResult = runQueryOnBucket(metadata, "{name: {$eq: \"Order1\"}}");
        assertEquals(1, queryResult.size(), "Should find Order1");
        String orderDoc = queryResult.getFirst();
        assertTrue(orderDoc.contains("\"price\": 10"), "First item should still have price 10");
        assertTrue(orderDoc.contains("\"price\": 50"), "Updated items should have price 50");

        // Verify index entries after update: 2 entries (10, 50)
        // 20 and 30 are removed, 50 is added (deduplicated since both updated items now have 50)
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(pricesSubspace);
        assertEquals(2, entriesAfter.size(), "Prices index should have 2 entries after update");

        Set<Integer> valuesAfter = extractIntValuesFromIndex(pricesSubspace);
        assertTrue(valuesAfter.contains(10), "Index should contain 10");
        assertTrue(valuesAfter.contains(50), "Index should contain 50");
        assertFalse(valuesAfter.contains(20), "Index should NOT contain 20");
        assertFalse(valuesAfter.contains(30), "Index should NOT contain 30");

        // Verify querying by new value works
        List<String> query50 = runQueryOnBucket(metadata, "{\"items.price\": {$eq: 50}}");
        assertEquals(1, query50.size(), "Query for items.price=50 should find Order1 via index");

        // Verify old values are not queryable via index
        List<String> query20 = runQueryOnBucket(metadata, "{\"items.price\": {$eq: 20}}");
        assertEquals(0, query20.size(), "Query for items.price=20 should find nothing");
    }

    @Test
    void shouldMaintainIndexesWhenUpdatingWithElemMatchFilter() {
        // Behavior: Secondary indexes on array fields should be correctly maintained when updating
        // via $elemMatch filter + array_filters. Old index entries should be removed and new entries
        // created for the updated values.

        // Create a multikey index on "items.status"
        SingleFieldIndexDefinition statusIndex = SingleFieldIndexDefinition.create("status_idx", "items.status", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, statusIndex);

        // Insert documents with items arrays containing status fields
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Order1\", \"items\": [{\"name\": \"A\", \"status\": \"pending\"}, {\"name\": \"B\", \"status\": \"done\"}, {\"name\": \"C\", \"status\": \"pending\"}]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Order2\", \"items\": [{\"name\": \"D\", \"status\": \"done\"}, {\"name\": \"E\", \"status\": \"done\"}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get status index subspace
        Index statusIdx = metadata.indexes().getIndex("items.status", IndexSelectionPolicy.READ);
        DirectorySubspace statusSubspace = statusIdx.subspace();

        // Verify initial state: 2 entries (pending, done) across 2 documents
        // Order1: pending (x2), done (x1) -> deduplicated to: pending, done
        // Order2: done (x2) -> deduplicated to: done
        // Total unique entries per document: Order1 has 2, Order2 has 1 = we expect 3 total index entries
        List<KeyValue> entriesBefore = fetchAllIndexedEntries(statusSubspace);
        assertEquals(3, entriesBefore.size(), "Status index should have 3 entries initially (Order1: pending+done, Order2: done)");

        // Verify documents can be found via "pending" query (only Order1 has pending items)
        List<String> pendingBefore = runQueryOnBucket(metadata, "{\"items.status\": {$eq: \"pending\"}}");
        assertEquals(1, pendingBefore.size(), "Should find 1 document with pending status before update");
        assertTrue(pendingBefore.getFirst().contains("\"name\": \"Order1\""), "Order1 should have pending items");

        // Update using $elemMatch filter and array_filters to change pending -> shipped
        // Filter: documents where at least one item has status = "pending"
        // Update: change all items with status = "pending" to "shipped"
        ArrayFilter pendingFilter = new ArrayFilter("elem.status", Operator.EQ, "pending");
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{\"items\": {$elemMatch: {status: {$eq: \"pending\"}}}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("items.$[elem].status", new BsonString("shipped"))
                .arrayFilter(pendingFilter)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> updatedObjectIds;
        try (Transaction tr = createTransaction()) {
            updatedObjectIds = updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Should have updated 1 document (Order1)
        assertEquals(1, updatedObjectIds.size(), "Should have updated 1 document");

        // Verify the document was updated correctly
        List<String> order1Result = runQueryOnBucket(metadata, "{name: {$eq: \"Order1\"}}");
        assertEquals(1, order1Result.size(), "Should find Order1");
        String order1Doc = order1Result.getFirst();
        assertTrue(order1Doc.contains("\"status\": \"shipped\""), "Order1 should have shipped status now");
        assertTrue(order1Doc.contains("\"status\": \"done\""), "Order1 should still have done status for item B");
        assertFalse(order1Doc.contains("\"status\": \"pending\""), "Order1 should not have pending status anymore");

        // Verify Order2 was not updated
        List<String> order2Result = runQueryOnBucket(metadata, "{name: {$eq: \"Order2\"}}");
        assertEquals(1, order2Result.size(), "Should find Order2");
        String order2Doc = order2Result.getFirst();
        assertTrue(order2Doc.contains("\"status\": \"done\""), "Order2 should still have done status");
        assertFalse(order2Doc.contains("\"status\": \"shipped\""), "Order2 should not have shipped status");

        // Verify index entries after update:
        // Order1: shipped (x2), done (x1) -> deduplicated to: shipped, done (2 entries)
        // Order2: done (x2) -> deduplicated to: done (1 entry)
        // Total: 3 entries (shipped, done, done) but the order may vary
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(statusSubspace);
        assertEquals(3, entriesAfter.size(), "Status index should have 3 entries after update");

        // Verify "pending" is no longer in the index
        Set<String> valuesAfter = extractStringValuesFromIndex(statusSubspace);
        assertFalse(valuesAfter.contains("pending"), "Index should NOT contain 'pending' after update");
        assertTrue(valuesAfter.contains("shipped"), "Index should contain 'shipped' after update");
        assertTrue(valuesAfter.contains("done"), "Index should still contain 'done'");

        // Verify queries work via the updated index
        List<String> pendingAfter = runQueryOnBucket(metadata, "{\"items.status\": {$eq: \"pending\"}}");
        assertEquals(0, pendingAfter.size(), "Should find 0 documents with pending status after update");

        List<String> shippedAfter = runQueryOnBucket(metadata, "{\"items.status\": {$eq: \"shipped\"}}");
        assertEquals(1, shippedAfter.size(), "Should find 1 document with shipped status after update");
        assertTrue(shippedAfter.getFirst().contains("\"name\": \"Order1\""), "Order1 should have shipped items now");

        // Verify "done" query returns both documents
        List<String> doneAfter = runQueryOnBucket(metadata, "{\"items.status\": {$eq: \"done\"}}");
        assertEquals(2, doneAfter.size(), "Should find 2 documents with done status");
    }

    @Test
    void shouldMaintainMultikeyIndexWhenUpdatingScalarArrayWithElemMatchAndPositionalOperator() {
        // Behavior: When using $elemMatch on a scalar array with a multi-key index and the $
        // positional operator, the index should be correctly maintained - the old value's index
        // entry should be removed and the new value's entry should be added.

        // Create a multikey index on "tags" (scalar string array)
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert document with scalar string array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Project1\", \"tags\": [\"java\", \"python\", \"rust\"]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Verify initial state: 3 index entries (java, python, rust)
        List<KeyValue> entriesBefore = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(3, entriesBefore.size(), "Tags index should have 3 entries initially");

        Set<String> valuesBefore = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesBefore.contains("java"), "Index should contain 'java'");
        assertTrue(valuesBefore.contains("python"), "Index should contain 'python'");
        assertTrue(valuesBefore.contains("rust"), "Index should contain 'rust'");

        // Update using $elemMatch + $ positional: change "python" to "go"
        String query = "{\"tags\": {$elemMatch: {$eq: \"python\"}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("tags.$", new BsonString("go"))
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Project1\"}}");
        assertEquals(1, results.size());
        String docJson = results.getFirst();
        assertTrue(docJson.contains("\"go\""), "Document should contain 'go'");
        assertFalse(docJson.contains("\"python\""), "Document should NOT contain 'python'");

        // Verify index entries after update: still 3 entries but python replaced with go
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(3, entriesAfter.size(), "Tags index should still have 3 entries");

        Set<String> valuesAfter = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesAfter.contains("java"), "Index should still contain 'java'");
        assertTrue(valuesAfter.contains("go"), "Index should contain 'go' (new value)");
        assertTrue(valuesAfter.contains("rust"), "Index should still contain 'rust'");
        assertFalse(valuesAfter.contains("python"), "Index should NOT contain 'python' (removed)");

        // Verify queries work via updated index
        List<String> goResults = runQueryOnBucket(metadata, "{tags: {$eq: \"go\"}}");
        assertEquals(1, goResults.size(), "Should find document via 'go' query");

        List<String> pythonResults = runQueryOnBucket(metadata, "{tags: {$eq: \"python\"}}");
        assertEquals(0, pythonResults.size(), "Should NOT find document via 'python' query");
    }

    @Test
    void shouldMaintainMultikeyIndexWhenUnsetWithElemMatchAndPositionalOperator() {
        // Behavior: When using $elemMatch on a scalar array with a multi-key index and the $
        // positional operator with $unset, the index should be correctly maintained - the matched
        // element's index entry should be removed and replaced with a null entry (since unset
        // leaves a null in the array position).

        // Create a multikey index on "tags" (scalar string array)
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert a document with a scalar string array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Project1\", \"tags\": [\"java\", \"python\", \"rust\"]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Verify initial state: 3 index entries (java, python, rust)
        List<KeyValue> entriesBefore = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(3, entriesBefore.size(), "Tags index should have 3 entries initially");

        Set<String> valuesBefore = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesBefore.contains("java"), "Index should contain 'java'");
        assertTrue(valuesBefore.contains("python"), "Index should contain 'python'");
        assertTrue(valuesBefore.contains("rust"), "Index should contain 'rust'");

        // Unset using $elemMatch + $ positional: remove "python" from the array
        String query = "{\"tags\": {$elemMatch: {$eq: \"python\"}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .unset("tags.$")
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Project1\"}}");
        assertEquals(1, results.size());
        String docJson = results.getFirst();
        assertTrue(docJson.contains("\"java\""), "Document should contain 'java'");
        assertTrue(docJson.contains("\"rust\""), "Document should contain 'rust'");
        assertFalse(docJson.contains("\"python\""), "Document should NOT contain 'python'");

        // Verify index entries after unset: 3 entries (java, rust, null)
        // The unset leaves a null in the array position, which is now correctly indexed
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(3, entriesAfter.size(), "Tags index should have 3 entries after unset (java, rust, null)");

        Set<String> valuesAfter = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesAfter.contains("java"), "Index should still contain 'java'");
        assertTrue(valuesAfter.contains("rust"), "Index should still contain 'rust'");
        assertFalse(valuesAfter.contains("python"), "Index should NOT contain 'python' (removed)");

        // Verify there's exactly one null entry
        int nullCount = 0;
        byte[] prefix = tagsSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : entriesAfter) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            if (keyTuple.get(0) == null) {
                nullCount++;
            }
        }
        assertEquals(1, nullCount, "Should have exactly 1 null entry");

        // Verify queries work via updated index
        List<String> javaResults = runQueryOnBucket(metadata, "{tags: {$eq: \"java\"}}");
        assertEquals(1, javaResults.size(), "Should find document via 'java' query");

        List<String> pythonResults = runQueryOnBucket(metadata, "{tags: {$eq: \"python\"}}");
        assertEquals(0, pythonResults.size(), "Should NOT find document via 'python' query");

        // Verify document is queryable via null
        List<String> nullResults = runQueryOnBucket(metadata, "{tags: {$eq: null}}");
        assertEquals(1, nullResults.size(), "Should find document via null query");
    }

    // ==================== UPSERT WITH $elemMatch TESTS ====================

    @Test
    void shouldIgnoreElemMatchOnUpsert() {
        // Behavior: $elemMatch in query filter is ignored during upsert - array field is not created.
        // This is correct behavior since $elemMatch is a query operator for matching array elements,
        // not a document construction operator.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        // Upsert with $elemMatch filter - the items array should NOT be created
        String query = "{\"items\": {$elemMatch: {status: {$eq: \"pending\"}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("ElemMatchTest"))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify upsert was triggered
        assertNotNull(ctx.upsertResult(), "Upsert should have been triggered");

        // Verify document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"ElemMatchTest\"}}");
        assertEquals(1, results.size(), "Should find the upserted document");

        String docJson = results.getFirst();

        // Verify the items field was NOT created (since $elemMatch is ignored on upsert)
        assertFalse(docJson.contains("\"items\""), "Document should NOT have items field from $elemMatch");

        // Verify $set field was applied
        assertTrue(docJson.contains("\"name\": \"ElemMatchTest\""), "Document should have name from $set");
    }

    @Test
    void shouldIgnoreElemMatchWithNestedConditionsOnUpsert() {
        // Behavior: $elemMatch with multiple nested conditions is ignored during upsert.
        // None of the conditions inside $elemMatch contribute to the upserted document.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        // Upsert with $elemMatch containing multiple conditions
        String query = "{\"items\": {$elemMatch: {status: {$eq: \"pending\"}, qty: {$eq: 10}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("NestedElemMatchTest"))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify upsert was triggered
        assertNotNull(ctx.upsertResult(), "Upsert should have been triggered");

        // Verify document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"NestedElemMatchTest\"}}");
        assertEquals(1, results.size(), "Should find the upserted document");

        String docJson = results.getFirst();

        // Verify none of the $elemMatch nested fields were created
        assertFalse(docJson.contains("\"items\""), "Document should NOT have items field from $elemMatch");
        assertFalse(docJson.contains("\"status\""), "Document should NOT have status field from nested $elemMatch condition");
        assertFalse(docJson.contains("\"qty\""), "Document should NOT have qty field from nested $elemMatch condition");

        // Verify $set field was applied
        assertTrue(docJson.contains("\"name\": \"NestedElemMatchTest\""), "Document should have name from $set");
    }

    @Test
    void shouldAllowSetToOverrideElemMatchArrayFieldOnUpsert() {
        // Behavior: While $elemMatch is ignored, $set can explicitly provide the array field.
        // The $set operation takes precedence and creates the array with the specified value.

        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET);

        // Build the items array to set
        BsonArray items = new BsonArray();
        BsonDocument item = new BsonDocument();
        item.put("status", new BsonString("active"));
        item.put("qty", new BsonInt32(5));
        items.add(item);

        // Upsert with $elemMatch filter AND $set explicitly providing items
        String query = "{\"items\": {$elemMatch: {status: {$eq: \"pending\"}}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("SetOverrideTest"))
                .set("items", items)
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify upsert was triggered
        assertNotNull(ctx.upsertResult(), "Upsert should have been triggered");

        // Verify document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"SetOverrideTest\"}}");
        assertEquals(1, results.size(), "Should find the upserted document");

        String docJson = results.getFirst();

        // Verify the items field WAS created from $set (not from $elemMatch)
        assertTrue(docJson.contains("\"items\""), "Document should have items field from $set");
        assertTrue(docJson.contains("\"status\": \"active\""), "Item should have status 'active' from $set");
        assertTrue(docJson.contains("\"qty\": 5"), "Item should have qty 5 from $set");

        // Verify $set name field was applied
        assertTrue(docJson.contains("\"name\": \"SetOverrideTest\""), "Document should have name from $set");
    }

    @Test
    void shouldIndexNullElementsInMultikeyArrayDuringUpdate() {
        // Behavior: Updating a document with an array containing null elements should create
        // ONE deduplicated null index entry along with non-null values.

        // Create a multikey index on "tags"
        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Insert a document with a simple array
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [\"java\"]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get tags index subspace
        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();

        // Verify initial state: 1 entry (java)
        assertEquals(1, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 1 entry initially");

        // Update: set tags array with null elements mixed with non-null values
        BsonArray tagsWithNulls = new BsonArray();
        tagsWithNulls.add(new BsonString("kotlin"));
        tagsWithNulls.add(BsonNull.VALUE);
        tagsWithNulls.add(new BsonString("scala"));
        tagsWithNulls.add(BsonNull.VALUE);  // Duplicate null - should be deduplicated

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("tags", tagsWithNulls)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify index entries after update: 3 entries (kotlin, scala, null)
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(3, entriesAfter.size(), "Tags index should have 3 entries (kotlin, scala, null)");

        // Verify the non-null values are indexed
        Set<String> stringValues = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(stringValues.contains("kotlin"), "Index should contain 'kotlin'");
        assertTrue(stringValues.contains("scala"), "Index should contain 'scala'");
        assertFalse(stringValues.contains("java"), "Index should NOT contain 'java' (old value)");

        // Verify there's exactly one null entry (deduplicated)
        int nullCount = 0;
        byte[] prefix = tagsSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : entriesAfter) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            if (keyTuple.get(0) == null) {
                nullCount++;
            }
        }
        assertEquals(1, nullCount, "Should have exactly 1 null entry (deduplicated)");

        // Verify document is queryable via null
        List<String> nullResults = runQueryOnBucket(metadata, "{tags: {$eq: null}}");
        assertEquals(1, nullResults.size(), "Should find document with null in tags array");
        assertTrue(nullResults.getFirst().contains("\"name\": \"Alice\""), "Should be Alice's document");
    }

    @Test
    void shouldIndexNullElementsInMultikeyArrayDuringUpsert() {
        // Behavior: Upsert creating a document with an array containing null elements should create
        // ONE deduplicated null index entry along with non-null values.

        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        // Create an array with null elements mixed with non-null values
        BsonArray tagsWithNulls = new BsonArray();
        tagsWithNulls.add(new BsonString("python"));
        tagsWithNulls.add(BsonNull.VALUE);
        tagsWithNulls.add(new BsonString("rust"));
        tagsWithNulls.add(BsonNull.VALUE);  // Duplicate null - should be deduplicated
        tagsWithNulls.add(new BsonString("go"));

        String query = "{name: {$eq: \"Developer\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("Developer"))
                .set("tags", tagsWithNulls)
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was created
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Developer\"}}");
        assertEquals(1, results.size(), "Should find the upserted document");

        // Verify 4 index entries created (python, rust, go, null)
        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();
        List<KeyValue> entries = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(4, entries.size(), "Tags index should have 4 entries (python, rust, go, null)");

        // Verify the non-null values are indexed
        Set<String> indexedValues = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(indexedValues.contains("python"), "Should have 'python' indexed");
        assertTrue(indexedValues.contains("rust"), "Should have 'rust' indexed");
        assertTrue(indexedValues.contains("go"), "Should have 'go' indexed");

        // Verify there's exactly one null entry (deduplicated)
        int nullCount = 0;
        byte[] prefix = tagsSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        for (KeyValue kv : entries) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey(), prefix.length, kv.getKey().length - prefix.length);
            if (keyTuple.get(0) == null) {
                nullCount++;
            }
        }
        assertEquals(1, nullCount, "Should have exactly 1 null entry (deduplicated)");

        // Verify document is queryable via null
        List<String> nullResults = runQueryOnBucket(metadata, "{tags: {$eq: null}}");
        assertEquals(1, nullResults.size(), "Should find document with null in tags array");
        assertTrue(nullResults.getFirst().contains("\"name\": \"Developer\""), "Should be Developer's document");
    }

    // ==================== COMPOUND INDEX UPDATE TESTS ====================

    @Test
    void shouldUpdateAffectedCompoundIndexWhenOneFieldModified() {
        // Behavior: Updating one field of a compound index drops the old compound entry and creates
        // a new one with the updated value combined with the unchanged field.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 35}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        assertEquals(3, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 3 entries before update");
        assertEquals(3, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 3 back pointers before update");

        // Update Bob's age from 30 to 40
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(40)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(3, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should still have 3 entries");
        assertEquals(3, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should still have 3 back pointers");

        List<String> bobQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Bob\"}}");
        assertEquals(1, bobQuery.size(), "Should find Bob");
        assertTrue(bobQuery.getFirst().contains("\"age\": 40"), "Bob should have age 40");

        List<String> oldAgeQuery = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertTrue(oldAgeQuery.isEmpty(), "Should not find any document with age 30");
    }

    @Test
    void shouldUpdateEntryMetadataOnUnaffectedCompoundIndex() {
        // Behavior: Updating a field NOT in the compound index leaves entries/back pointers unchanged;
        // only entry metadata is updated.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"city\": \"Berlin\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"city\": \"Paris\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries before update");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers before update");

        // Update Alice's city (not in compound index)
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("city", new BsonString("Munich")).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should still have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should still have 2 back pointers");

        List<String> aliceQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, aliceQuery.size(), "Should find Alice");
        assertTrue(aliceQuery.getFirst().contains("\"city\": \"Munich\""), "Alice should have city Munich");
    }

    @Test
    void shouldUpdateCompoundIndexWhenBothFieldsModified() {
        // Behavior: Updating all fields of a compound index drops the old entry and creates a new
        // entry with all new values.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries before update");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers before update");

        // Update Bob's name and age
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("Robert"))
                .set("age", new BsonInt32(50))
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should still have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should still have 2 back pointers");

        List<String> robertQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Robert\"}}");
        assertEquals(1, robertQuery.size(), "Should find Robert");
        assertTrue(robertQuery.getFirst().contains("\"age\": 50"), "Robert should have age 50");

        List<String> bobQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Bob\"}}");
        assertTrue(bobQuery.isEmpty(), "Should not find Bob anymore");
    }

    @Test
    void shouldReplaceNullCompoundEntryWhenMissingFieldAddedViaUpdate() {
        // Behavior: Documents missing a compound index field have null at that position. Adding
        // the field via update replaces the null entry with actual values.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        // Insert documents missing age field
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries (with null age)");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers");

        // Add age to Alice
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(25)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should still have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should still have 2 back pointers");

        List<String> aliceByAge = runQueryOnBucket(metadata, "{age: {$eq: 25}}");
        assertEquals(1, aliceByAge.size(), "Should find Alice by age 25");
        assertTrue(aliceByAge.getFirst().contains("\"name\": \"Alice\""), "Result should be Alice");

        List<String> nullAgeQuery = runQueryOnBucket(metadata, "{age: {$eq: null}}");
        assertEquals(1, nullAgeQuery.size(), "Should find only Bob with null age");
        assertTrue(nullAgeQuery.getFirst().contains("\"name\": \"Bob\""), "Bob should still have null age");
    }

    @Test
    void shouldUpdateCompoundIndexWithMultiKeyFieldUpdate() {
        // Behavior: Compound index with multiKey field: updating the array drops old expanded entries
        // and creates new entries for each unique element.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_tags_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("tags", BsonType.STRING, true)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [\"x\", \"y\", \"z\"]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_tags_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        // 3 compound entries (one per tag element)
        assertEquals(3, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 3 entries before update");

        // Update tags to ["a", "b"]
        BsonArray newTags = new BsonArray();
        newTags.add(new BsonString("a"));
        newTags.add(new BsonString("b"));

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags", newTags).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Now 2 compound entries (one per new tag element)
        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries after update");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers after update");
    }

    @Test
    void shouldMaintainBothSingleFieldAndCompoundIndexesDuringUpdate() {
        // Behavior: When both single-field and compound indexes exist and the update affects a
        // shared field, all indexes are updated consistently.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{ageIndex, nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();
        Index nameIdx = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);
        DirectorySubspace nameSubspace = nameIdx.subspace();
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        // Update Bob's age from 30 to 40
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Bob\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonInt32(40)).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Single-field age index: 2 entries (25, 40 — no 30)
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 2 entries");
        Set<Integer> ageValues = extractAgeValuesFromIndex(ageSubspace);
        assertTrue(ageValues.contains(25), "Age index should contain 25");
        assertTrue(ageValues.contains(40), "Age index should contain 40");
        assertFalse(ageValues.contains(30), "Age index should NOT contain 30");

        // Single-field name index: 2 entries unchanged
        assertEquals(2, fetchAllIndexedEntries(nameSubspace).size(), "Name index should have 2 entries");

        // Compound index: 2 entries, 2 back pointers
        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers");

        List<String> bobQuery = runQueryOnBucket(metadata, "{age: {$eq: 40}}");
        assertEquals(1, bobQuery.size(), "Should find Bob with age 40");
        assertTrue(bobQuery.getFirst().contains("\"name\": \"Bob\""), "Result should be Bob");

        List<String> oldAgeQuery = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertTrue(oldAgeQuery.isEmpty(), "Should not find any document with age 30");
    }

    @Test
    void shouldCreateCompoundIndexEntriesOnUpsert() {
        // Behavior: Upsert creates compound index entries for the new document.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Upsert: no documents exist
        String query = "{name: {$eq: \"Alice\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("name", new BsonString("Alice"))
                .set("age", new BsonInt32(25))
                .upsert(true)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Reload metadata to pick up compound index subspaces after insert
        metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        assertEquals(1, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 1 entry");
        assertEquals(1, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 1 back pointer");

        List<String> nameQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, nameQuery.size(), "Should find Alice by name");
        assertTrue(nameQuery.getFirst().contains("\"age\": 25"), "Alice should have age 25");

        List<String> ageQuery = runQueryOnBucket(metadata, "{age: {$eq: 25}}");
        assertEquals(1, ageQuery.size(), "Should find Alice by age");

        assertNotNull(ctx.upsertResult(), "Upsert result should not be null");
    }

    @Test
    void shouldDropCompoundEntryAndReindexWithNullWhenFieldUnset() {
        // Behavior: $unset on a compound index field drops old entry and creates new entry with
        // null for the unset field.

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        assertEquals(1, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 1 entry before unset");
        assertEquals(1, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 1 back pointer before unset");

        // $unset the age field
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().unset("age").build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Compound entry re-indexed with null for age
        assertEquals(1, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should still have 1 entry (re-indexed with null)");
        assertEquals(1, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should still have 1 back pointer");

        // Document retrievable via name
        List<String> aliceQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, aliceQuery.size(), "Should find Alice by name");
        assertFalse(aliceQuery.getFirst().contains("\"age\""), "Alice should no longer have age field");

        // Old age value no longer queryable
        List<String> ageQuery = runQueryOnBucket(metadata, "{age: {$eq: 30}}");
        assertTrue(ageQuery.isEmpty(), "Should not find any document with age 30");
    }

    // ==================== CROSS-NUMERIC-TYPE POSITIONAL OPERATOR TESTS ====================

    @Test
    void shouldUpdateWithPositionalOperatorWhenQueryUsesInt32AndDocumentHasInt64() {
        // Behavior: The $ positional operator correctly finds the matching array element when the
        // document stores Int64 values but the query filter uses Int32 (cross-numeric-type match).

        SingleFieldIndexDefinition scoresIndex = SingleFieldIndexDefinition.create("scores_idx", "scores", BsonType.INT64, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, scoresIndex);

        // Insert document with Int64 values in the array
        BsonDocument doc = new BsonDocument()
                .append("name", new BsonString("Student1"))
                .append("scores", new BsonArray(List.of(
                        new BsonInt64(85),
                        new BsonInt64(92),
                        new BsonInt64(78)
                )));
        List<byte[]> documents = List.of(BSONUtil.toBytes(doc));
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Update using $elemMatch + $ positional: change the first score >= 90 (Int32 in query) to 99
        String query = "{\"scores\": {$elemMatch: {$gte: 90}}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, query);
        UpdateOptions update = UpdateOptions.builder()
                .set("scores.$", new BsonInt64(99))
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());
        ctx.setQueryBytes(query.getBytes());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify the document was updated correctly: 92 should be replaced with 99
        List<String> results = runQueryOnBucket(metadata, "{name: {$eq: \"Student1\"}}");
        assertEquals(1, results.size());
        String docJson = results.getFirst();
        assertTrue(docJson.contains("99"), "Document should contain 99 (the updated value)");
        assertFalse(docJson.contains("92"), "Document should NOT contain 92 (the replaced value)");
    }

    @Test
    void shouldRefreshChildSelectorIndexWhenParentPathSet() {
        // Behavior: $set on a parent path refreshes a child-selector index. Removed values are
        // dropped from the index and new values are indexed, so indexed queries see the
        // updated document content.

        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();
        assertEquals(2, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 2 entries initially");

        // Replace the whole tags array
        BsonArray newTags = new BsonArray();
        newTags.add(new BsonDocument().append("name", new BsonString("go")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags", newTags).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Index reflects the new array content
        assertEquals(1, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 1 entry after update");
        Set<String> valuesAfter = extractStringValuesFromIndex(tagsSubspace);
        assertTrue(valuesAfter.contains("go"), "Index should contain 'go' after update");
        assertFalse(valuesAfter.contains("java"), "Index should NOT contain stale 'java'");
        assertFalse(valuesAfter.contains("kotlin"), "Index should NOT contain stale 'kotlin'");

        // Indexed queries see the updated content
        List<String> byNewValue = runQueryOnBucket(metadata, "{\"tags.name\": {$eq: \"go\"}}");
        assertEquals(1, byNewValue.size(), "Should find the document by the new tag value");
        List<String> byOldValue = runQueryOnBucket(metadata, "{\"tags.name\": {$eq: \"kotlin\"}}");
        assertTrue(byOldValue.isEmpty(), "Should NOT find the document by a removed tag value");
    }

    @Test
    void shouldCreateNullChildSelectorEntryWhenParentPathUnset() {
        // Behavior: $unset of a parent path leaves a child-selector index with a single null
        // entry, matching the state a fresh insert of the updated document would produce.

        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags.name", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        Index tagsIdx = metadata.indexes().getIndex("tags.name", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();
        assertEquals(2, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 2 entries initially");

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().unset("tags").build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // A single null entry replaces the multikey entries
        List<KeyValue> entriesAfter = fetchAllIndexedEntries(tagsSubspace);
        assertEquals(1, entriesAfter.size(), "Tags index should have 1 entry after unset");
        byte[] prefix = tagsSubspace.pack(Tuple.from(IndexSubspaceMagic.ENTRIES.getValue()));
        Tuple keyTuple = Tuple.fromBytes(entriesAfter.getFirst().getKey(), prefix.length, entriesAfter.getFirst().getKey().length - prefix.length);
        assertNull(keyTuple.get(0), "Index entry should be null after unsetting the parent path");

        // The document is queryable by null and not by the removed values
        List<String> byNull = runQueryOnBucket(metadata, "{\"tags.name\": {$eq: null}}");
        assertEquals(1, byNull.size(), "Should find the document by null after unset");
        List<String> byOldValue = runQueryOnBucket(metadata, "{\"tags.name\": {$eq: \"java\"}}");
        assertTrue(byOldValue.isEmpty(), "Should NOT find the document by a removed tag value");
    }

    @Test
    void shouldLeaveSiblingIndexUnaffectedOnSubfieldSet() {
        // Behavior: Setting one nested field does not touch an index on a sibling nested field.
        // The sibling index keeps its entries and cardinality; only entry metadata is refreshed.

        SingleFieldIndexDefinition colorIndex = SingleFieldIndexDefinition.create("color_idx", "meta.color", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("meta_name_idx", "meta.name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, colorIndex, nameIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"meta\": {\"color\": \"red\", \"name\": \"x\"}}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        Index colorIdx = metadata.indexes().getIndex("meta.color", IndexSelectionPolicy.READ);
        DirectorySubspace colorSubspace = colorIdx.subspace();
        assertEquals(1, fetchAllIndexedEntries(colorSubspace).size(), "Color index should have 1 entry initially");

        try (Transaction tr = createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            assertEquals(1L, indexStatistics.get(colorIndex.id()).cardinality(), "Color index cardinality should be 1 initially");
        }

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{\"meta.color\": {$eq: \"red\"}}");
        UpdateOptions update = UpdateOptions.builder().set("meta.name", new BsonString("y")).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Sibling index entries and cardinality untouched
        Set<String> colorValues = extractStringValuesFromIndex(colorSubspace);
        assertEquals(Set.of("red"), colorValues, "Color index should still contain only 'red'");
        try (Transaction tr = createTransaction()) {
            var indexStatistics = BucketMetadataUtil.readIndexStatistics(tr, metadata);
            assertEquals(1L, indexStatistics.get(colorIndex.id()).cardinality(), "Color index cardinality should remain 1");
        }

        // The document is still queryable through the sibling index
        List<String> byColor = runQueryOnBucket(metadata, "{\"meta.color\": {$eq: \"red\"}}");
        assertEquals(1, byColor.size(), "Should find the document by meta.color after sibling update");
        assertTrue(byColor.getFirst().contains("\"name\": \"y\""), "Document should carry the updated sibling value");
    }

    @Test
    void shouldRefreshParentSelectorIndexWhenArrayElementSet() {
        // Behavior: Setting an array element by numeric index refreshes a multikey index whose
        // selector is the array field itself.

        SingleFieldIndexDefinition tagsIndex = SingleFieldIndexDefinition.create("tags_idx", "tags", BsonType.STRING, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, tagsIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [\"java\", \"kotlin\"]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        Index tagsIdx = metadata.indexes().getIndex("tags", IndexSelectionPolicy.READ);
        DirectorySubspace tagsSubspace = tagsIdx.subspace();
        assertEquals(2, fetchAllIndexedEntries(tagsSubspace).size(), "Tags index should have 2 entries initially");

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags.0", new BsonString("go")).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        Set<String> valuesAfter = extractStringValuesFromIndex(tagsSubspace);
        assertEquals(Set.of("go", "kotlin"), valuesAfter, "Index should reflect the updated array content");

        List<String> byNewValue = runQueryOnBucket(metadata, "{tags: {$eq: \"go\"}}");
        assertEquals(1, byNewValue.size(), "Should find the document by the new element value");
        List<String> byOldValue = runQueryOnBucket(metadata, "{tags: {$eq: \"java\"}}");
        assertTrue(byOldValue.isEmpty(), "Should NOT find the document by the replaced element value");
    }

    @Test
    void shouldRefreshIndexWhenPositionalArrayFilterSet() {
        // Behavior: A $set with a filtered positional operator refreshes the index on the
        // normalized selector. Positional segments in the op path are normalized before
        // affected-index detection.

        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score_idx", "grades.score", BsonType.INT32, true, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, scoreIndex);

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"grades\": [{\"score\": 50}, {\"score\": 70}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        Index scoreIdx = metadata.indexes().getIndex("grades.score", IndexSelectionPolicy.READ);
        DirectorySubspace scoreSubspace = scoreIdx.subspace();
        assertEquals(2, fetchAllIndexedEntries(scoreSubspace).size(), "Score index should have 2 entries initially");

        // Raise scores below 60 to 90; the filter targets the element's score field
        ArrayFilter lowFilter = new ArrayFilter("low.score", Operator.LT, 60);
        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder()
                .set("grades.$[low].score", new BsonInt32(90))
                .arrayFilter(lowFilter)
                .build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        Set<Integer> valuesAfter = extractIntValuesFromIndex(scoreSubspace);
        assertEquals(Set.of(90, 70), valuesAfter, "Index should reflect the updated scores");

        List<String> byNewValue = runQueryOnBucket(metadata, "{\"grades.score\": {$eq: 90}}");
        assertEquals(1, byNewValue.size(), "Should find the document by the updated score");
        List<String> byOldValue = runQueryOnBucket(metadata, "{\"grades.score\": {$eq: 50}}");
        assertTrue(byOldValue.isEmpty(), "Should NOT find the document by the replaced score");
    }

    @Test
    void shouldRefreshCompoundIndexWhenParentPathSet() {
        // Behavior: $set on a parent path refreshes a compound index whose field selector is a
        // child of that path. Old entries are dropped and new ones reflect the updated content.

        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("tagname_age_idx", List.of(
                new CompoundIndexField("tags.name", BsonType.STRING, true),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25, \"tags\": [{\"name\": \"java\"}, {\"name\": \"kotlin\"}]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("tagname_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();
        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries initially");

        // Replace the whole tags array
        BsonArray newTags = new BsonArray();
        newTags.add(new BsonDocument().append("name", new BsonString("go")));

        PlanWithParams planWithParams = createPlanWithParams(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("tags", newTags).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        try (Transaction tr = createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 1 entry after update");
        assertEquals(1, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 1 back pointer after update");

        List<String> byNewValue = runQueryOnBucket(metadata, "{$and: [{\"tags.name\": {$eq: \"go\"}}, {age: {$eq: 25}}]}");
        assertEquals(1, byNewValue.size(), "Should find the document by the new tag value through the compound index");
        List<String> byOldValue = runQueryOnBucket(metadata, "{$and: [{\"tags.name\": {$eq: \"java\"}}, {age: {$eq: 25}}]}");
        assertTrue(byOldValue.isEmpty(), "Should NOT find the document by a removed tag value");
    }
}