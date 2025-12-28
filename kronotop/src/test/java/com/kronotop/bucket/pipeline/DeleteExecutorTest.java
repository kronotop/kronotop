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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DeleteExecutorTest extends BasePipelineTest {

    @Test
    void shouldCleanAllIndexesWhenDocumentDeleted() {
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
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READ);
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
        PipelineNode plan = createExecutionPlan(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<Versionstamp> deletedVersionstamps;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            deletedVersionstamps = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(2, deletedVersionstamps.size(), "Should have deleted 2 documents");

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
        // Create two secondary indexes
        IndexDefinition ageIndex = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexDefinition nameIndex = IndexDefinition.create("name_idx", "name", BsonType.STRING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(BUCKET_NAME, ageIndex, nameIndex);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Alice\", \"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Bob\", \"age\": 30}"),
                BSONUtil.jsonToDocumentThenBytes("{\"name\": \"Charlie\", \"age\": 25}")
        );
        insertDocumentsAndGetVersionstamps(BUCKET_NAME, documents);

        // Get index subspaces
        Index primaryIndex = metadata.indexes().getIndex(DefaultIndexDefinition.ID.selector(), IndexSelectionPolicy.READ);
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
        PipelineNode plan = createExecutionPlan(metadata, deleteQuery);
        QueryOptions options = QueryOptions.builder().build();
        QueryContext ctx = new QueryContext(metadata, options, plan);

        List<Versionstamp> deletedVersionstamps;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            deletedVersionstamps = deleteExecutor.execute(tr, ctx);
            tr.commit().join();
        }

        assertEquals(1, deletedVersionstamps.size(), "Should have deleted 1 document");

        // Verify indexes have correct count after delete (Alice and Charlie remain)
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
}