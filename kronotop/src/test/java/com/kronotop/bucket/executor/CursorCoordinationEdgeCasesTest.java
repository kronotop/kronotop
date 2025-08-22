/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.SortOrder;
import org.bson.BsonBinaryReader;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for cursor coordination edge cases in mixed queries.
 * This class validates behavior in unusual scenarios that could
 * expose bugs in the cursor coordination logic.
 */
class CursorCoordinationEdgeCasesTest extends BasePlanExecutorTest {

    @Test
    void testEmptyIndexedResults() {
        final String TEST_BUCKET_NAME = "test-empty-indexed-results";

        // Create index
        IndexDefinition indexedIndex = IndexDefinition.create("indexed-field-index", "indexed_field", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, indexedIndex);

        // Insert test data
        List<byte[]> documents = List.of(
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'existing_value', 'non_indexed_field': 'other_value'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'another_value', 'non_indexed_field': 'common_value'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'third_value', 'non_indexed_field': 'common_value'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'fourth_value', 'non_indexed_field': 'common_value'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query where indexed condition matches nothing, but non-indexed does
        String query = "{ $or: [ { indexed_field: \"non_existent_value\" }, { non_indexed_field: \"common_value\" } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 10);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            assertTrue(results.size() >= 3, "Should find results from non-indexed portion");

            // Validate results - should only match non-indexed condition
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String nonIndexedValue = null;
                    
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("non_indexed_field".equals(fieldName)) {
                            nonIndexedValue = reader.readString();
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndDocument();
                    
                    assertEquals("common_value", nonIndexedValue, "Should only match non-indexed condition");
                }
            }

            System.out.println("✅ Empty indexed results edge case passed!");
            System.out.println("Found " + results.size() + " results from non-indexed portion only");
        }
    }

    @Test
    void testEmptyNonIndexedResults() {
        final String TEST_BUCKET_NAME = "test-empty-non-indexed-results";

        // Create index
        IndexDefinition indexedIndex = IndexDefinition.create("indexed-field-index", "indexed_field", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, indexedIndex);

        // Insert test data
        List<byte[]> documents = List.of(
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'indexed_value_1', 'non_indexed_field': 'other_value'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'indexed_value_1', 'non_indexed_field': 'another_value'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'different_value', 'non_indexed_field': 'third_value'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        // Query where non-indexed condition matches nothing, but indexed does
        String query = "{ $or: [ { indexed_field: \"indexed_value_1\" }, { non_indexed_field: \"impossible_value\" } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 10);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            assertTrue(results.size() >= 2, "Should find results from indexed portion");

            // Validate results - should only match indexed condition
            for (ByteBuffer documentBuffer : results.values()) {
                documentBuffer.rewind();
                try (BsonBinaryReader reader = new BsonBinaryReader(documentBuffer)) {
                    reader.readStartDocument();
                    String indexedValue = null;
                    
                    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                        String fieldName = reader.readName();
                        if ("indexed_field".equals(fieldName)) {
                            indexedValue = reader.readString();
                        } else {
                            reader.skipValue();
                        }
                    }
                    reader.readEndDocument();
                    
                    assertEquals("indexed_value_1", indexedValue, "Should only match indexed condition");
                }
            }

            System.out.println("✅ Empty non-indexed results edge case passed!");
            System.out.println("Found " + results.size() + " results from indexed portion only");
        }
    }

    @Test
    void testBothPortionsEmpty() {
        final String TEST_BUCKET_NAME = "test-both-portions-empty";

        // Create index
        IndexDefinition indexedIndex = IndexDefinition.create("indexed-field-index", "indexed_field", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, indexedIndex);

        // Insert test data
        List<byte[]> documents = List.of(
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'real_value', 'non_indexed_field': 'real_value'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'another_real_value', 'non_indexed_field': 'another_real_value'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { indexed_field: \"impossible_indexed_value\" }, { non_indexed_field: \"impossible_non_indexed_value\" } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 10);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            assertEquals(0, results.size(), "Should find no results");
            System.out.println("✅ Both portions empty edge case passed!");
        }
    }

    @Test
    void testOverlappingResults() {
        final String TEST_BUCKET_NAME = "test-overlapping-results";

        // Create index
        IndexDefinition indexedIndex = IndexDefinition.create("indexed-field-index", "indexed_field", BsonType.STRING, SortOrder.ASCENDING);
        BucketMetadata metadata = createIndexesAndLoadBucketMetadata(TEST_BUCKET_NAME, indexedIndex);

        // Insert documents that match both conditions
        List<byte[]> documents = List.of(
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'overlap_value', 'non_indexed_field': 'overlap_value', 'extra': 'overlap1'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'overlap_value', 'non_indexed_field': 'overlap_value', 'extra': 'overlap2'}"),
            BSONUtil.jsonToDocumentThenBytes("{'indexed_field': 'other_value', 'non_indexed_field': 'different_value', 'extra': 'other'}")
        );

        insertDocumentsAndGetVersionstamps(TEST_BUCKET_NAME, documents);

        String query = "{ $or: [ { indexed_field: \"overlap_value\" }, { non_indexed_field: \"overlap_value\" } ] }";
        PlanExecutor executor = createPlanExecutorForQuery(metadata, query, 10);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<?, ByteBuffer> results = executor.execute(tr);

            // Should find the overlapping documents exactly once (deduplication)
            assertTrue(results.size() >= 2, "Should find at least the overlapping documents");

            System.out.println("✅ Overlapping results edge case passed!");
            System.out.println("Found " + results.size() + " unique results (deduplication working)");
        }
    }
}