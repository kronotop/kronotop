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
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;
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
    void shouldDropIndexEntryWhenUpdatedWithMismatchedType() {
        // Create an INT32 index on age field
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, ageIndex);

        // Insert document with valid INT32 age
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get age index subspace
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();

        // Verify age index has 2 entries before update
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 2 entries");
        assertEquals(2, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 2 back pointers");

        Set<Integer> ageValuesBefore = extractAgeValuesFromIndex(ageSubspace);
        assertTrue(ageValuesBefore.contains(25), "Age index should contain 25");
        assertTrue(ageValuesBefore.contains(30), "Age index should contain 30");

        // Update Alice's age with a STRING value (type mismatch for INT32 index)
        PipelineNode plan = createExecutionPlan(metadata, "{name: {$eq: \"Alice\"}}");
        UpdateOptions update = UpdateOptions.builder().set("age", new BsonString("twenty-five")).build();
        QueryOptions options = QueryOptions.builder().update(update).build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            updateExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        // Verify age index now has only 1 entry (Alice's entry dropped due to type mismatch)
        assertEquals(1, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 1 entry after type mismatch update");
        assertEquals(1, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 1 back pointer after type mismatch update");

        // Verify only Bob's age (30) remains in the index
        Set<Integer> ageValuesAfter = extractAgeValuesFromIndex(ageSubspace);
        assertFalse(ageValuesAfter.contains(25), "Age index should NOT contain 25 (dropped due to type mismatch)");
        assertTrue(ageValuesAfter.contains(30), "Age index should still contain 30");

        // Verify Alice's document still exists with the new string value
        List<String> aliceQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, aliceQuery.size(), "Alice should still exist");
        assertTrue(aliceQuery.getFirst().contains("\"age\": \"twenty-five\""), "Alice should have string age value");

        // Verify Alice cannot be found via age index query (since her entry was dropped)
        List<String> ageQuery = runQueryOnBucket(metadata, "{age: {$eq: 25}}");
        assertTrue(ageQuery.isEmpty(), "Should not find any document with age 25 via index");
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
}