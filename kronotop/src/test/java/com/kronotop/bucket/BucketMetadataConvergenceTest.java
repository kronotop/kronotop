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

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.handlers.BaseBucketHandlerTest;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BucketMetadataConvergenceTest extends BaseBucketHandlerTest {

    @Test
    void shouldAwaitSuccessfullyWhenBucketExists() {
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            tr.commit().join();
        }

        assertDoesNotThrow(() -> BucketMetadataConvergence.await(context, TEST_NAMESPACE, TEST_BUCKET));
    }

    @Test
    void shouldAwaitSuccessfullyAfterMetadataUpdate() {
        BucketMetadata initialMetadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, initialMetadata);
            tr.commit().join();
        }

        long initialVersion = initialMetadata.version();

        // Update metadata by adding an index
        IndexDefinition indexDefinition = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(indexDefinition);

        // Get updated metadata
        BucketMetadata updatedMetadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);
        assertTrue(updatedMetadata.version() > initialVersion);

        // Await should succeed with the new version
        assertDoesNotThrow(() -> BucketMetadataConvergence.await(context, TEST_NAMESPACE, TEST_BUCKET));
    }

    @Test
    void shouldThrowExceptionWhenBucketDoesNotExist() {
        assertThrows(BucketMetadataConvergenceException.class,
                () -> BucketMetadataConvergence.await(context, TEST_NAMESPACE, "non-existent-bucket"));
    }
}
