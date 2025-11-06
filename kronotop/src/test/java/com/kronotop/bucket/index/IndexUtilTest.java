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

package com.kronotop.bucket.index;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataHeader;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.server.RESPError;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class IndexUtilTest extends BaseStandaloneInstanceTest {
    final IndexDefinition definition = IndexDefinition.create(
            "numeric-index",
            "numeric-selector",
            BsonType.INT32
    );

    @Test
    void shouldCreateIndexAndIncreaseVersionByOne() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        DirectorySubspace indexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                DirectorySubspace subspace = IndexUtil.create(tx, TEST_NAMESPACE, TEST_BUCKET, definition);
                tr.commit().join();
                return subspace;
            }
        });
        assertNotNull(indexSubspace);

        long bucketMetadataVersion = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return BucketMetadataUtil.readVersion(tr, metadata.subspace());
            }
        });
        // The first one is the default index, _id
        assertTrue(bucketMetadataVersion > 0);
    }

    @Test
    void shouldOpenDefaultIDIndexSubspace() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        DirectorySubspace indexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return IndexUtil.open(tr, metadata.subspace(), DefaultIndexDefinition.ID.name());
            }
        });
        assertNotNull(indexSubspace);
    }

    @Test
    void shouldThrowErrorWhenOpeningNonExistingIndexSubspace() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        KronotopException ex = assertThrows(KronotopException.class, () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.open(tr, metadata.subspace(), "not-exist-index-name");
            }
        });
        assertEquals("No such index: 'not-exist-index-name'", ex.getMessage());
        assertEquals(RESPError.NOSUCHINDEX, ex.getPrefix());
    }

    @Test
    void shouldListAllIndexes() {
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        List<String> expectedIndexes = List.of(DefaultIndexDefinition.ID.name(), definition.name());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> indexes = IndexUtil.list(tr, metadata.subspace());
            assertThat(expectedIndexes).hasSameElementsAs(indexes);
        }
    }

    @Test
    void shouldLoadIndexDefinitionWithDirectorySubspace() {
        createIndexThenWaitForReadiness(definition);

        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            IndexDefinition loadedIndexDefinition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            IndexDefinition finalDef = definition.updateStatus(IndexStatus.READY);
            assertThat(loadedIndexDefinition).usingRecursiveComparison().isEqualTo(finalDef);
        }
    }

    @Test
    void shouldModifyIndexCardinality() {
        createIndexThenWaitForReadiness(definition);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        DirectorySubspace indexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            assertNotNull(indexSubspace);
            tr.commit().join();
        }

        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.mutateCardinality(tr, metadata.subspace(), definition.id(), 1);
                tr.commit().join();
            }
        });

        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.mutateCardinality(tr, metadata.subspace(), definition.id(), -1);
                tr.commit().join();
            }
        });
    }

    @Test
    void shouldClear() {
        createIndexThenWaitForReadiness(definition);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        DirectorySubspace indexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            assertNotNull(indexSubspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                IndexUtil.clear(tr, metadata.subspace(), definition.name());
                tr.commit().join();
            });
        }

        KronotopException exception = assertThrows(KronotopException.class,
                () -> IndexUtil.open(context.getFoundationDB().createTransaction(), metadata.subspace(), definition.name()));
        assertEquals("No such index: '" + definition.name() + "'", exception.getMessage());
        assertEquals(RESPError.NOSUCHINDEX, exception.getPrefix());

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadataHeader header = BucketMetadataUtil.readBucketMetadataHeader(tr, metadata.subspace());
            assertFalse(header.indexStatistics().containsKey(definition.id()));
        }
    }

    @Test
    void shouldNotClearNotExistingIndex() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopException exception = assertThrows(KronotopException.class, () -> {
                IndexUtil.clear(tr, metadata.subspace(), "not-exist-index-name");
            });
            assertEquals("No such index: 'not-exist-index-name'", exception.getMessage());
        }
    }

    @Test
    void shouldThrowExceptionWhenCreatingSameIndexTwice() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Create index first time
        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.create(tr, metadata.subspace(), definition);
                tr.commit().join();
            }
        });

        // Attempt to create the same index again should throw exception
        KronotopException exception = assertThrows(KronotopException.class, () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.create(tr, metadata.subspace(), definition);
            }
        });
        assertEquals("'" + definition.name() + "' has already exist", exception.getMessage());
    }

    @Test
    void shouldCreateSameIndexDefinitionOnDifferentBuckets() {
        final String FIRST_TEST_BUCKET = "first-bucket";
        final String SECOND_TEST_BUCKET = "second-bucket";

        BucketMetadata firstBucketMetadata = getBucketMetadata(FIRST_TEST_BUCKET);
        BucketMetadata secondBucketMetadata = getBucketMetadata(SECOND_TEST_BUCKET);

        // Create same index on first bucket
        DirectorySubspace firstIndexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = IndexUtil.create(tr, firstBucketMetadata.subspace(), definition);
                tr.commit().join();
                return subspace;
            }
        });
        assertNotNull(firstIndexSubspace);

        // Create same index on second bucket - should succeed
        DirectorySubspace secondIndexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = IndexUtil.create(tr, secondBucketMetadata.subspace(), definition);
                tr.commit().join();
                return subspace;
            }
        });
        assertNotNull(secondIndexSubspace);

        // Verify both indexes exist and have the same definition
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexDefinition firstIndexDefinition = IndexUtil.loadIndexDefinition(tr, firstIndexSubspace);
            IndexDefinition secondIndexDefinition = IndexUtil.loadIndexDefinition(tr, secondIndexSubspace);

            assertThat(firstIndexDefinition).usingRecursiveComparison().isEqualTo(definition);
            assertThat(secondIndexDefinition).usingRecursiveComparison().isEqualTo(definition);
            assertThat(firstIndexDefinition).usingRecursiveComparison().isEqualTo(secondIndexDefinition);
        }

        // Verify indexes appear in their respective bucket index lists
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> firstBucketIndexes = IndexUtil.list(tr, firstBucketMetadata.subspace());
            List<String> secondBucketIndexes = IndexUtil.list(tr, secondBucketMetadata.subspace());

            assertTrue(firstBucketIndexes.contains(definition.name()));
            assertTrue(secondBucketIndexes.contains(definition.name()));
        }
    }

    @Test
    void shouldCreateIndexDefinitionsWithUniqueIdsForSameParameters() {
        String indexName = "test-index";
        String selector = "test.field";
        BsonType bsonType = BsonType.STRING;
        int numberOfIndexes = 10;

        List<IndexDefinition> createdIndexes = new ArrayList<>();
        Set<Long> uniqueIds = new HashSet<>();

        // Create multiple IndexDefinition instances with identical parameters
        for (int i = 0; i < numberOfIndexes; i++) {
            IndexDefinition indexDefinition = IndexDefinition.create(indexName, selector, bsonType);
            createdIndexes.add(indexDefinition);
            uniqueIds.add(indexDefinition.id());
        }

        // Verify all indexes have the same name, selector, and bsonType
        for (IndexDefinition indexDefinition : createdIndexes) {
            assertEquals(indexName, indexDefinition.name());
            assertEquals(selector, indexDefinition.selector());
            assertEquals(bsonType, indexDefinition.bsonType());
        }

        // Verify all IDs are unique
        assertEquals(numberOfIndexes, uniqueIds.size(),
                "All IndexDefinition instances should have unique IDs despite having identical parameters");

        // Verify no two indexes have the same ID
        for (int i = 0; i < createdIndexes.size(); i++) {
            for (int j = i + 1; j < createdIndexes.size(); j++) {
                assertNotEquals(createdIndexes.get(i).id(), createdIndexes.get(j).id(),
                        "IndexDefinition at index " + i + " and " + j + " should have different IDs");
            }
        }
    }

    @Test
    void shouldDropIndexAndUpdateStatusToDropped() {
        createIndexThenWaitForReadiness(definition);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Verify index is READY before drop
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            IndexDefinition currentDef = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            assertEquals(IndexStatus.READY, currentDef.status());
        }

        // Drop the index
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.drop(tx, metadata, definition.name());
            tr.commit().join();
        }

        // Verify index status is now DROPPED
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            IndexDefinition droppedDef = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            assertEquals(IndexStatus.DROPPED, droppedDef.status());
        }
    }

    @Test
    void shouldNotDropNonExistingIndex() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        KronotopException exception = assertThrows(KronotopException.class, () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.drop(tx, metadata, "non-existing-index");
            }
        });
        assertEquals("No such index: 'non-existing-index'", exception.getMessage());
    }

    @Test
    void shouldAllowIdempotentDropOperation() {
        createIndexThenWaitForReadiness(definition);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        // Drop the index first time
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            IndexUtil.drop(tx, metadata, definition.name());
            tr.commit().join();
        }

        // Verify status is DROPPED
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            IndexDefinition droppedDef = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            assertEquals(IndexStatus.DROPPED, droppedDef.status());
        }

        // Drop again should succeed (idempotent operation)
        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.drop(tx, metadata, definition.name());
                tr.commit().join();
            }
        });

        // Verify status is still DROPPED
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), definition.name());
            IndexDefinition droppedDef = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            assertEquals(IndexStatus.DROPPED, droppedDef.status());
        }
    }

    @Test
    void shouldAnalyzeIndex() {
        createIndexThenWaitForReadiness(definition);
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.analyze(tx, metadata, definition.name());
                tr.commit().join();
            }
        });
    }

    @Test
    void shouldNotAnalyzeNonExistingIndex() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        KronotopException exception = assertThrows(KronotopException.class, () -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.analyze(tx, metadata, "non-existing-index");
            }
        });
        assertEquals("No such index: 'non-existing-index'", exception.getMessage());
    }

    @Test
    void shouldAnalyzeDefaultIdIndex() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);

        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                IndexUtil.analyze(tx, metadata, DefaultIndexDefinition.ID.name());
                tr.commit().join();
            }
        });
    }
}