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
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BucketMetadataVersionBarrierTest extends BaseBucketHandlerTest {
    @Test
    void shouldAwaitForTargetVersion() {
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            TransactionalContext tx = new TransactionalContext(context, tr);
            BucketMetadataUtil.publishBucketMetadataUpdatedEvent(tx, metadata);
            tr.commit().join();
        }

        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
        barrier.await(metadata.version(), 10, Duration.ofMillis(500));
    }

    @Test
    void shouldThrowBarrierNotSatisfiedExceptionForUnreachableVersion() {
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);

        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
        long unreachableVersion = Long.MAX_VALUE;
        int maxAttempts = 3;

        BarrierNotSatisfiedException exception = assertThrows(BarrierNotSatisfiedException.class, () -> {
            barrier.await(unreachableVersion, maxAttempts, Duration.ofMillis(100));
        });

        assertTrue(exception.getMessage().contains("Barrier not satisfied"));
        assertTrue(exception.getMessage().contains(String.valueOf(unreachableVersion)));
        assertTrue(exception.getMessage().contains("attempts"));
    }

    @Test
    void shouldThrowBarrierNotSatisfiedExceptionWithLowAttempts() {
        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, getSession(), TEST_BUCKET);

        BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
        long futureVersion = metadata.version() + 1000;

        BarrierNotSatisfiedException exception = assertThrows(BarrierNotSatisfiedException.class, () -> {
            barrier.await(futureVersion, 1, Duration.ofMillis(1));
        });

        assertNotNull(exception.getMessage());
        assertTrue(exception.getMessage().contains("Barrier not satisfied"));
    }
}