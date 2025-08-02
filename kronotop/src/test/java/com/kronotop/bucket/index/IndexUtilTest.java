// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataHeader;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.server.RESPError;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class IndexUtilTest extends BaseStandaloneInstanceTest {
    final String bucketName = "test-bucket";
    final IndexDefinition definition = IndexDefinition.create(
            "numeric-index",
            "numeric-field",
            BsonType.INT32,
            SortOrder.ASCENDING
    );

    @Test
    void shouldCreateIndexAndIncreaseVersionByOne() {
        BucketMetadata metadata = getBucketMetadata(bucketName);
        DirectorySubspace indexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = IndexUtil.create(tr, metadata.subspace(), definition);
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
        BucketMetadata metadata = getBucketMetadata(bucketName);
        DirectorySubspace indexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                return IndexUtil.open(tr, metadata.subspace(), DefaultIndexDefinition.ID.name());
            }
        });
        assertNotNull(indexSubspace);
    }

    @Test
    void shouldThrowErrorWhenOpeningNonExistingIndexSubspace() {
        BucketMetadata metadata = getBucketMetadata(bucketName);
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
        BucketMetadata metadata = getBucketMetadata(bucketName);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
            assertNotNull(indexSubspace);
            tr.commit().join();
        }

        List<String> expectedIndexes = List.of(DefaultIndexDefinition.ID.name(), definition.name());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> indexes = IndexUtil.list(tr, metadata.subspace());
            assertEquals(expectedIndexes, indexes);
        }
    }

    @Test
    void shouldLoadIndexDefinitionWithDirectorySubspace() {
        BucketMetadata metadata = getBucketMetadata(bucketName);
        DirectorySubspace indexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
            assertNotNull(indexSubspace);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            IndexDefinition loadedIndexDefinition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
            assertThat(loadedIndexDefinition).usingRecursiveComparison().isEqualTo(definition);
        }
    }

    @Test
    void shouldModifyIndexCardinality() {
        BucketMetadata metadata = getBucketMetadata(bucketName);
        DirectorySubspace indexSubspace;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            indexSubspace = IndexUtil.create(tr, metadata.subspace(), definition);
            assertNotNull(indexSubspace);
            tr.commit().join();
        }

        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.increaseCardinality(tr, metadata.subspace(), definition.id());
                tr.commit().join();
            }
        });

        assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                IndexUtil.decreaseCardinality(tr, metadata.subspace(), definition.id());
                tr.commit().join();
            }
        });
    }

    @Test
    void shouldDrop() {
        BucketMetadata metadata = getBucketMetadata(bucketName);
        DirectorySubspace indexSubspace = assertDoesNotThrow(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = IndexUtil.create(tr, metadata.subspace(), definition);
                tr.commit().join();
                return subspace;
            }
        });
        assertNotNull(indexSubspace);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> {
                IndexUtil.drop(tr, metadata.subspace(), definition.name());
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
    void shouldNotDropNotExistingIndex() {
        BucketMetadata metadata = getBucketMetadata(bucketName);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopException exception = assertThrows(KronotopException.class, () -> {
                IndexUtil.drop(tr, metadata.subspace(), "not-exist-index-name");
            });
            assertEquals("No such index: 'not-exist-index-name'", exception.getMessage());
        }
    }
}