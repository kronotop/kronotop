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