// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.CachedTimeService;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class BucketMetadataCacheTest extends BaseStandaloneInstanceTest {
    final String testBucketName = "test-bucket";
    final String testNamespaceName = "test-namespace";

    @Test
    void shouldBasicOperationsWork() {
        BucketMetadata metadata = getBucketMetadata(testBucketName);
        BucketMetadataCache cache = new BucketMetadataCache(context);

        assertDoesNotThrow(() -> cache.set(testNamespaceName, testBucketName, metadata));

        BucketMetadata cachedMetadata = cache.get(testNamespaceName, testBucketName);
        assertThat(cachedMetadata).usingRecursiveComparison().isEqualTo(metadata);
    }

    @Test
    void shouldEvictionWorkerWork() {
        BucketMetadata metadata = getBucketMetadata(testBucketName);
        BucketMetadataCache cache = new BucketMetadataCache(context);

        assertDoesNotThrow(() -> cache.set(testNamespaceName, testBucketName, metadata));

        CachedTimeService cachedTimeService = context.getService(CachedTimeService.NAME);
        // 1-millisecond TTL
        Runnable runnable = cache.createEvictionWorker(cachedTimeService, 1);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            runnable.run();
            return cache.get(testNamespaceName, testBucketName) == null;
        });
    }
}