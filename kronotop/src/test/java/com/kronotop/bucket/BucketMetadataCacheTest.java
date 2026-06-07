/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class BucketMetadataCacheTest extends BaseStandaloneInstanceTest {

    @BeforeEach
    public void setUp() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldBasicOperationsWork() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketMetadataCache cache = new BucketMetadataCache(context);

        assertDoesNotThrow(() -> cache.set(TEST_NAMESPACE, TEST_BUCKET, metadata));

        BucketMetadata cachedMetadata = cache.get(TEST_NAMESPACE, TEST_BUCKET);
        assertThat(cachedMetadata).usingRecursiveComparison().isEqualTo(metadata);
    }

    @Test
    void shouldInvalidateRemoveBucketFromCache() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketMetadataCache cache = new BucketMetadataCache(context);

        cache.set(TEST_NAMESPACE, TEST_BUCKET, metadata);
        assertThat(cache.get(TEST_NAMESPACE, TEST_BUCKET)).isNotNull();

        cache.invalidate(TEST_NAMESPACE, TEST_BUCKET);
        assertThat(cache.get(TEST_NAMESPACE, TEST_BUCKET)).isNull();
    }

    @Test
    void shouldInvalidateHandleNonExistentNamespace() {
        BucketMetadataCache cache = new BucketMetadataCache(context);
        assertDoesNotThrow(() -> cache.invalidate("non-existent-namespace", TEST_BUCKET));
    }

    @Test
    void shouldInvalidateByPrefixRemoveMatchingNamespaces() {
        BucketMetadataCache cache = new BucketMetadataCache(context);

        // Add items to the cache for various namespaces
        cache.set("a", TEST_BUCKET, getBucketMetadata(TEST_BUCKET));
        cache.set("a.b", TEST_BUCKET, getBucketMetadata(TEST_BUCKET));
        cache.set("a.b.c", TEST_BUCKET, getBucketMetadata(TEST_BUCKET));
        cache.set("a.b.c.d", TEST_BUCKET, getBucketMetadata(TEST_BUCKET));
        cache.set("a.b2", TEST_BUCKET, getBucketMetadata(TEST_BUCKET));

        // Invalidate cache for "a.b" prefix
        cache.invalidate("a.b");

        // "a" and "a.b2" should remain in the cache
        assertThat(cache.get("a", TEST_BUCKET)).isNotNull();
        assertThat(cache.get("a.b2", TEST_BUCKET)).isNotNull();

        // "a.b", "a.b.c", "a.b.c.d" should be removed
        assertThat(cache.get("a.b", TEST_BUCKET)).isNull();
        assertThat(cache.get("a.b.c", TEST_BUCKET)).isNull();
        assertThat(cache.get("a.b.c.d", TEST_BUCKET)).isNull();
    }

    @Test
    void shouldEvictionWorkerWork() {
        BucketMetadata metadata = getBucketMetadata(TEST_BUCKET);
        BucketMetadataCache cache = new BucketMetadataCache(context);

        assertDoesNotThrow(() -> cache.set(TEST_NAMESPACE, TEST_BUCKET, metadata));

        // 1-millisecond TTL
        Runnable runnable = cache.createEvictionWorker(context::now, 1);
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            runnable.run();
            return cache.get(TEST_NAMESPACE, TEST_BUCKET) == null;
        });
    }
}