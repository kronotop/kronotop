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
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.*;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeleteExecutorTest extends BasePipelineTest {

    @Test
    void shouldCleanAllIndexesWhenDocumentDeleted() {
        // Behavior: Deleting documents removes entries from ALL indexes (primary and secondary).
        // Both index entries and back pointers are cleaned for each deleted document.

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
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Index nameIdx = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);

        DirectorySubspace primarySubspace = primaryIndex.subspace();
        DirectorySubspace ageSubspace = ageIdx.subspace();
        DirectorySubspace nameSubspace = nameIdx.subspace();

        // Verify all indexes have entries before delete
        List<KeyValue> primaryEntriesBefore = fetchAllIndexedEntries(primarySubspace);
        List<KeyValue> ageEntriesBefore = fetchAllIndexedEntries(ageSubspace);
        List<KeyValue> nameEntriesBefore = fetchAllIndexedEntries(nameSubspace);

        assertEquals(2, primaryEntriesBefore.size(), "Primary index should have 2 entries before delete");
        assertEquals(2, ageEntriesBefore.size(), "Age index should have 2 entries before delete");
        assertEquals(2, nameEntriesBefore.size(), "Name index should have 2 entries before delete");

        // Verify back pointers exist
        List<KeyValue> ageBackPointersBefore = fetchAllIndexBackPointers(ageSubspace);
        List<KeyValue> nameBackPointersBefore = fetchAllIndexBackPointers(nameSubspace);

        assertEquals(2, ageBackPointersBefore.size(), "Age index should have 2 back pointers before delete");
        assertEquals(2, nameBackPointersBefore.size(), "Name index should have 2 back pointers before delete");

        // Execute delete query - delete all documents
        String deleteQuery = "{}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(2, deletedObjectIds.size(), "Should have deleted 2 documents");

        // Verify all indexes are cleaned after delete
        List<KeyValue> primaryEntriesAfter = fetchAllIndexedEntries(primarySubspace);
        List<KeyValue> ageEntriesAfter = fetchAllIndexedEntries(ageSubspace);
        List<KeyValue> nameEntriesAfter = fetchAllIndexedEntries(nameSubspace);

        assertTrue(primaryEntriesAfter.isEmpty(), "Primary index should be empty after delete");
        assertTrue(ageEntriesAfter.isEmpty(), "Age index should be empty after delete");
        assertTrue(nameEntriesAfter.isEmpty(), "Name index should be empty after delete");

        // Verify back pointers are cleaned
        List<KeyValue> ageBackPointersAfter = fetchAllIndexBackPointers(ageSubspace);
        List<KeyValue> nameBackPointersAfter = fetchAllIndexBackPointers(nameSubspace);

        assertTrue(ageBackPointersAfter.isEmpty(), "Age index back pointers should be empty after delete");
        assertTrue(nameBackPointersAfter.isEmpty(), "Name index back pointers should be empty after delete");
    }

    @Test
    void shouldCleanOnlyMatchingDocumentIndexesWhenDeletedWithPredicate() {
        // Behavior: Delete with a predicate removes only matching documents and their index entries.
        // Non-matching documents and their index entries remain intact across all indexes.

        // Create two secondary indexes
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, nameIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get index subspaces
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Index nameIdx = metadata.indexes().getIndex("name", IndexSelectionPolicy.READ);

        DirectorySubspace primarySubspace = primaryIndex.subspace();
        DirectorySubspace ageSubspace = ageIdx.subspace();
        DirectorySubspace nameSubspace = nameIdx.subspace();

        // Verify all indexes have entries before delete
        assertEquals(3, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should have 3 entries before delete");
        assertEquals(3, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 3 entries before delete");
        assertEquals(3, fetchAllIndexedEntries(nameSubspace).size(), "Name index should have 3 entries before delete");
        assertEquals(3, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 3 back pointers before delete");
        assertEquals(3, fetchAllIndexBackPointers(nameSubspace).size(), "Name index should have 3 back pointers before delete");

        // Execute delete query - delete only Bob (age = 30)
        String deleteQuery = "{age: {$eq: 30}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, deletedObjectIds.size(), "Should have deleted 1 document");

        // Verify indexes have the correct count after delete (Alice and Charlie remain)
        assertEquals(2, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexedEntries(nameSubspace).size(), "Name index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 2 back pointers after delete");
        assertEquals(2, fetchAllIndexBackPointers(nameSubspace).size(), "Name index should have 2 back pointers after delete");

        // Verify remaining documents can be queried
        List<String> remainingDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(2, remainingDocs.size(), "Should have 2 remaining documents");

        // Verify Bob is deleted
        List<String> bobQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Bob\"}}");
        assertTrue(bobQuery.isEmpty(), "Bob should be deleted");

        // Verify Alice and Charlie still exist
        List<String> aliceQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}");
        assertEquals(1, aliceQuery.size(), "Alice should still exist");

        List<String> charlieQuery = runQueryOnBucket(metadata, "{name: {$eq: \"Charlie\"}}");
        assertEquals(1, charlieQuery.size(), "Charlie should still exist");
    }

    @Test
    void shouldCleanSecondaryIndexWhenDeletingWithRangePredicateOnDifferentIndex() {
        // Behavior: Deleting via a range predicate on one indexed field (age > 25) also cleans
        // entries from other indexes (category). All indexes stay consistent after deletion.

        // Create two secondary indexes on different fields
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition categoryIndex = SingleFieldIndexDefinition.create("category_idx", "category", BsonType.STRING, false, IndexStatus.WAITING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET, ageIndex, categoryIndex);

        // Insert documents with various ages and categories
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 20, \"category\": \"junior\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30, \"category\": \"senior\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25, \"category\": \"mid\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Diana\", \"age\": 35, \"category\": \"senior\"}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Eve\", \"age\": 28, \"category\": \"mid\"}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Get index subspaces
        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        Index categoryIdx = metadata.indexes().getIndex("category", IndexSelectionPolicy.READ);

        DirectorySubspace primarySubspace = primaryIndex.subspace();
        DirectorySubspace ageSubspace = ageIdx.subspace();
        DirectorySubspace categorySubspace = categoryIdx.subspace();

        // Verify all indexes have entries before delete
        assertEquals(5, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should have 5 entries before delete");
        assertEquals(5, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 5 entries before delete");
        assertEquals(5, fetchAllIndexedEntries(categorySubspace).size(), "Category index should have 5 entries before delete");
        assertEquals(5, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 5 back pointers before delete");
        assertEquals(5, fetchAllIndexBackPointers(categorySubspace).size(), "Category index should have 5 back pointers before delete");

        // Execute the delete query using range predicate on age index
        // This should delete Bob (30), Diana (35), and Eve (28) - all with age > 25
        String deleteQuery = "{age: {$gt: 25}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(3, deletedObjectIds.size(), "Should have deleted 3 documents (Bob, Diana, Eve)");

        // Verify indexes have the correct count after delete (Alice and Charlie remain)
        assertEquals(2, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexedEntries(categorySubspace).size(), "Category index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 2 back pointers after delete");
        assertEquals(2, fetchAllIndexBackPointers(categorySubspace).size(), "Category index should have 2 back pointers after delete");

        // Verify remaining documents can be queried
        List<String> remainingDocs = runQueryOnBucket(metadata, "{}");
        assertEquals(2, remainingDocs.size(), "Should have 2 remaining documents");

        // Verify deleted documents are gone
        assertTrue(runQueryOnBucket(metadata, "{name: {$eq: \"Bob\"}}").isEmpty(), "Bob should be deleted");
        assertTrue(runQueryOnBucket(metadata, "{name: {$eq: \"Diana\"}}").isEmpty(), "Diana should be deleted");
        assertTrue(runQueryOnBucket(metadata, "{name: {$eq: \"Eve\"}}").isEmpty(), "Eve should be deleted");

        // Verify remaining documents still exist
        assertEquals(1, runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}").size(), "Alice should still exist");
        assertEquals(1, runQueryOnBucket(metadata, "{name: {$eq: \"Charlie\"}}").size(), "Charlie should still exist");

        // Verify category index is properly cleaned by querying through it
        List<String> seniorQuery = runQueryOnBucket(metadata, "{category: {$eq: \"senior\"}}");
        assertTrue(seniorQuery.isEmpty(), "No senior category documents should remain (Bob and Diana deleted)");

        List<String> midQuery = runQueryOnBucket(metadata, "{category: {$eq: \"mid\"}}");
        assertEquals(1, midQuery.size(), "Only Charlie with mid category should remain (Eve deleted)");

        List<String> juniorQuery = runQueryOnBucket(metadata, "{category: {$eq: \"junior\"}}");
        assertEquals(1, juniorQuery.size(), "Alice with junior category should still exist");
    }

    @Test
    void shouldCleanCompoundIndexEntriesWhenAllDocumentsDeleted() {
        // Behavior: Deleting all documents removes all compound index entries and back pointers.

        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        // Reload metadata after insert to pick up compound index subspaces
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        // Verify compound index has entries before delete
        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries before delete");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers before delete");

        // Delete all documents
        String deleteQuery = "{}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(2, deletedObjectIds.size(), "Should have deleted 2 documents");

        // Verify the compound index is cleaned after delete
        assertTrue(fetchAllIndexedEntries(compoundSubspace).isEmpty(), "Compound index entries should be empty after delete");
        assertTrue(fetchAllIndexBackPointers(compoundSubspace).isEmpty(), "Compound index back pointers should be empty after delete");
    }

    @Test
    void shouldCleanOnlyMatchingCompoundIndexEntriesWhenDeletedWithPredicate() {
        // Behavior: Delete with a predicate removes compound index entries only for matching documents.
        // Non-matching documents retain their compound index entries and back pointers.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{ageIndex}, new CompoundIndexDefinition[]{compoundDef});

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        // Verify compound index has 3 entries before delete
        assertEquals(3, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 3 entries before delete");
        assertEquals(3, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 3 back pointers before delete");

        // Delete Bob (age = 30)
        String deleteQuery = "{age: {$eq: 30}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, deletedObjectIds.size(), "Should have deleted 1 document");

        // Verify compound index retains Alice and Charlie
        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers after delete");

        // Verify Alice and Charlie still queryable
        assertEquals(1, runQueryOnBucket(metadata, "{name: {$eq: \"Alice\"}}").size(), "Alice should still exist");
        assertEquals(1, runQueryOnBucket(metadata, "{name: {$eq: \"Charlie\"}}").size(), "Charlie should still exist");
    }

    @Test
    void shouldCleanBothSingleFieldAndCompoundIndexesOnDelete() {
        // Behavior: When both single-field and compound indexes exist, deleting documents cleans
        // entries from all index types consistently.

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_age_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{ageIndex},
                new CompoundIndexDefinition[]{compoundDef}
        );

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        Index ageIdx = metadata.indexes().getIndex("age", IndexSelectionPolicy.READ);
        DirectorySubspace ageSubspace = ageIdx.subspace();

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_age_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READ);
        DirectorySubspace primarySubspace = primaryIndex.subspace();

        // Delete Bob (age = 30)
        String deleteQuery = "{age: {$eq: 30}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, deletedObjectIds.size(), "Should have deleted 1 document");

        // Assert single-field age index: 1 entry, 1 back pointer
        assertEquals(1, fetchAllIndexedEntries(ageSubspace).size(), "Age index should have 1 entry after delete");
        assertEquals(1, fetchAllIndexBackPointers(ageSubspace).size(), "Age index should have 1 back pointer after delete");

        // Assert compound index: 1 entry, 1 back pointer
        assertEquals(1, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 1 entry after delete");
        assertEquals(1, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 1 back pointer after delete");

        // Assert primary index: 1 entry
        assertEquals(1, fetchAllIndexedEntries(primarySubspace).size(), "Primary index should have 1 entry after delete");
    }

    @Test
    void shouldCleanMultiKeyCompoundIndexEntriesOnDelete() {
        // Behavior: When a compound index has a multi-key field, deleting a document removes
        // all expanded entries. Each array element produces a separate index entry.

        CompoundIndexDefinition compoundDef = CompoundIndexDefinition.create("name_tags_idx", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("tags", BsonType.STRING, true)
        ), IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        createIndexesAndLoadBucketMetadata(
                TEST_BUCKET, new SingleFieldIndexDefinition[]{nameIndex},
                new CompoundIndexDefinition[]{compoundDef}
        );

        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"tags\": [\"x\", \"y\", \"z\"]}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"tags\": [\"a\", \"b\"]}")
        );
        insertDocumentsAndGetObjectIds(TEST_BUCKET, documents);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName("name_tags_idx", IndexSelectionPolicy.READ);
        DirectorySubspace compoundSubspace = compoundIndex.subspace();

        // Verify 5 total compound entries before delete (Alice: 3, Bob: 2)
        assertEquals(5, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 5 entries before delete");

        // Delete Alice
        String deleteQuery = "{name: {$eq: \"Alice\"}}";
        PlanWithParams planWithParams = createPlanWithParams(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(getSession(), metadata, options, planWithParams.plan(), planWithParams.parameters());

        List<ObjectId> deletedObjectIds;
        try (Transaction tr = createTransaction()) {
            deletedObjectIds = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, deletedObjectIds.size(), "Should have deleted 1 document");

        // Assert 2 compound entries remain (Bob's)
        assertEquals(2, fetchAllIndexedEntries(compoundSubspace).size(), "Compound index should have 2 entries after delete");
        assertEquals(2, fetchAllIndexBackPointers(compoundSubspace).size(), "Compound index should have 2 back pointers after delete");
    }
}