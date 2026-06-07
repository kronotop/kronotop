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

package com.kronotop.bucket.vector;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.DistanceFunction;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.bucket.index.VectorIndexDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class VectorGraphIndexRegistryTest {

    private VectorGraphIndexRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new VectorGraphIndexRegistry();
    }

    private BucketMetadata newMetadata(String namespace, String bucket) {
        return new BucketMetadata(UUID.randomUUID(), namespace, bucket, 1L, false,
                null, null, null, null, null, null, null, null, null);
    }

    private BucketMetadata newMetadata(UUID uuid, String namespace, String bucket) {
        return new BucketMetadata(uuid, namespace, bucket, 1L, false,
                null, null, null, null, null, null, null, null, null);
    }

    private VectorIndex newVectorIndex(long id) {
        return new VectorIndex(
                new VectorIndexDefinition(id, "idx-" + id, "embedding", 3,
                        DistanceFunction.COSINE, IndexStatus.READY),
                null
        );
    }

    private VectorGraphIndexGroup registerGroup(String namespace, String bucket, long indexId) {
        BucketMetadata metadata = newMetadata(namespace, bucket);
        VectorIndex index = newVectorIndex(indexId);
        return registry.computeIfAbsent(metadata, index,
                () -> new VectorGraphIndexGroup(null, metadata, index));
    }

    private void registerGroup(UUID uuid, String namespace, String bucket, long indexId) {
        BucketMetadata metadata = newMetadata(uuid, namespace, bucket);
        VectorIndex index = newVectorIndex(indexId);
        registry.computeIfAbsent(metadata, index,
                () -> new VectorGraphIndexGroup(null, metadata, index));
    }

    // --- get() ---

    @Test
    void shouldReturnNullWhenNamespaceDoesNotExist() {
        // Behavior: get returns null when the namespace has no entries in the registry.
        assertNull(registry.get("nonexistent", "bucket", 1L));
    }

    @Test
    void shouldReturnNullWhenBucketDoesNotExist() {
        // Behavior: get returns null when the namespace exists but the bucket does not.
        registerGroup("ns", "bucketA", 1L);
        assertNull(registry.get("ns", "bucketB", 1L));
    }

    @Test
    void shouldReturnNullWhenIndexIdDoesNotExist() {
        // Behavior: get returns null when the namespace and bucket exist but the indexId does not.
        registerGroup("ns", "bucket", 1L);
        assertNull(registry.get("ns", "bucket", 999L));
    }

    @Test
    void shouldReturnGroupAfterRegistration() {
        // Behavior: get returns the same group instance that was registered via computeIfAbsent.
        VectorGraphIndexGroup group = registerGroup("ns", "bucket", 1L);
        assertSame(group, registry.get("ns", "bucket", 1L));
    }

    // --- computeIfAbsent() ---

    @Test
    void shouldCreateAndStoreNewGroup() {
        // Behavior: computeIfAbsent invokes the supplier and stores the result when no group exists for the key.
        VectorGraphIndexGroup group = registerGroup("ns", "bucket", 1L);
        assertNotNull(group);
        assertSame(group, registry.get("ns", "bucket", 1L));
    }

    @Test
    void shouldNotInvokeSupplierWhenGroupExists() {
        // Behavior: computeIfAbsent returns the existing group and does not invoke the supplier when a group already exists.
        VectorGraphIndexGroup first = registerGroup("ns", "bucket", 1L);

        AtomicInteger supplierCallCount = new AtomicInteger(0);
        BucketMetadata metadata = newMetadata("ns", "bucket");
        VectorIndex index = newVectorIndex(1L);
        VectorGraphIndexGroup second = registry.computeIfAbsent(metadata, index, () -> {
            supplierCallCount.incrementAndGet();
            return new VectorGraphIndexGroup(null, metadata, index);
        });

        assertEquals(0, supplierCallCount.get());
        assertSame(first, second);
    }

    @Test
    void shouldStoreGroupsIndependentlyAcrossNamespaces() {
        // Behavior: computeIfAbsent stores groups independently for different namespaces.
        VectorGraphIndexGroup group1 = registerGroup("ns1", "bucket", 1L);
        VectorGraphIndexGroup group2 = registerGroup("ns2", "bucket", 1L);
        assertNotSame(group1, group2);
        assertSame(group1, registry.get("ns1", "bucket", 1L));
        assertSame(group2, registry.get("ns2", "bucket", 1L));
    }

    @Test
    void shouldStoreGroupsIndependentlyAcrossBuckets() {
        // Behavior: computeIfAbsent stores groups independently for different buckets under the same namespace.
        VectorGraphIndexGroup group1 = registerGroup("ns", "bucketA", 1L);
        VectorGraphIndexGroup group2 = registerGroup("ns", "bucketB", 1L);
        assertNotSame(group1, group2);
        assertSame(group1, registry.get("ns", "bucketA", 1L));
        assertSame(group2, registry.get("ns", "bucketB", 1L));
    }

    @Test
    void shouldStoreGroupsIndependentlyAcrossIndexIds() {
        // Behavior: computeIfAbsent stores groups independently for different indexIds under the same namespace and bucket.
        VectorGraphIndexGroup group1 = registerGroup("ns", "bucket", 1L);
        VectorGraphIndexGroup group2 = registerGroup("ns", "bucket", 2L);
        assertNotSame(group1, group2);
        assertSame(group1, registry.get("ns", "bucket", 1L));
        assertSame(group2, registry.get("ns", "bucket", 2L));
    }

    // --- remove(namespace, bucket, indexId) ---

    @Test
    void shouldRemoveSingleGroupAndReturnTrue() {
        // Behavior: remove returns true and the group is no longer retrievable via get after removal.
        registerGroup("ns", "bucket", 1L);
        assertTrue(registry.remove("ns", "bucket", 1L));
        assertNull(registry.get("ns", "bucket", 1L));
    }

    @Test
    void shouldReturnFalseWhenRemovingFromNonexistentNamespace() {
        // Behavior: remove returns false when the namespace does not exist in the registry.
        assertFalse(registry.remove("nonexistent", "bucket", 1L));
    }

    @Test
    void shouldReturnFalseWhenRemovingFromNonexistentBucket() {
        // Behavior: remove returns false when the namespace exists but the bucket does not.
        registerGroup("ns", "bucketA", 1L);
        assertFalse(registry.remove("ns", "bucketB", 1L));
    }

    @Test
    void shouldReturnFalseWhenRemovingNonexistentIndexId() {
        // Behavior: remove returns false when the namespace and bucket exist but the indexId does not.
        registerGroup("ns", "bucket", 1L);
        assertFalse(registry.remove("ns", "bucket", 999L));
    }

    @Test
    void shouldNotAffectOtherEntriesWhenRemovingSingleGroup() {
        // Behavior: remove of one group does not affect other groups under the same namespace and bucket.
        registerGroup("ns", "bucket", 1L);
        VectorGraphIndexGroup survivor = registerGroup("ns", "bucket", 2L);
        registry.remove("ns", "bucket", 1L);
        assertSame(survivor, registry.get("ns", "bucket", 2L));
    }

    // --- remove(namespace, bucket) ---

    @Test
    void shouldRemoveAllIndexesUnderBucket() {
        // Behavior: remove(namespace, bucket) removes all indexIds under the specified bucket.
        registerGroup("ns", "bucket", 1L);
        registerGroup("ns", "bucket", 2L);
        registry.remove("ns", "bucket");
        assertNull(registry.get("ns", "bucket", 1L));
        assertNull(registry.get("ns", "bucket", 2L));
    }

    @Test
    void shouldBeNoOpWhenNamespaceMissing() {
        // Behavior: remove(namespace, bucket) is a no-op when the namespace does not exist.
        assertDoesNotThrow(() -> registry.remove("nonexistent", "bucket"));
    }

    @Test
    void shouldNotAffectOtherBucketsWhenRemovingBucket() {
        // Behavior: remove(namespace, bucket) does not affect groups registered under other buckets.
        registerGroup("ns", "bucketA", 1L);
        VectorGraphIndexGroup survivor = registerGroup("ns", "bucketB", 1L);
        registry.remove("ns", "bucketA");
        assertSame(survivor, registry.get("ns", "bucketB", 1L));
    }

    // --- remove(prefix) ---

    @Test
    void shouldRemoveAllNamespacesMatchingPrefix() {
        // Behavior: remove(prefix) removes all entries under namespaces that start with the given prefix.
        registerGroup("cluster1.ns-a", "b1", 1L);
        registerGroup("cluster1.ns-b", "b2", 1L);
        registry.remove("cluster1");
        assertNull(registry.get("cluster1.ns-a", "b1", 1L));
        assertNull(registry.get("cluster1.ns-b", "b2", 1L));
    }

    @Test
    void shouldReturnBucketUUIDsOfRemovedGroups() {
        // Behavior: remove(prefix) returns the set of bucket UUIDs from all removed groups.
        UUID uuid1 = UUID.randomUUID();
        UUID uuid2 = UUID.randomUUID();
        registerGroup(uuid1, "cluster1.ns-a", "b1", 1L);
        registerGroup(uuid2, "cluster1.ns-b", "b2", 1L);

        Set<UUID> removed = registry.remove("cluster1");
        assertEquals(Set.of(uuid1, uuid2), removed);
    }

    @Test
    void shouldNotAffectNonMatchingNamespaces() {
        // Behavior: remove(prefix) does not remove entries under namespaces that do not match the prefix.
        registerGroup("cluster1.ns-a", "b1", 1L);
        VectorGraphIndexGroup survivor = registerGroup("cluster2.ns-c", "b3", 1L);
        registry.remove("cluster1");
        assertSame(survivor, registry.get("cluster2.ns-c", "b3", 1L));
    }

    @Test
    void shouldReturnEmptySetWhenNoPrefixMatches() {
        // Behavior: remove(prefix) returns an empty set when no namespace matches the prefix.
        VectorGraphIndexGroup group = registerGroup("cluster1.ns-a", "b1", 1L);
        Set<UUID> removed = registry.remove("cluster9");
        assertTrue(removed.isEmpty());
        assertSame(group, registry.get("cluster1.ns-a", "b1", 1L));
    }

    // --- closeAll() ---

    @Test
    void shouldClearEntireRegistry() {
        // Behavior: closeAll clears the registry so that get returns null for all previously registered groups.
        registerGroup("ns1", "bucket1", 1L);
        registerGroup("ns2", "bucket2", 2L);
        registry.closeAll();
        assertNull(registry.get("ns1", "bucket1", 1L));
        assertNull(registry.get("ns2", "bucket2", 2L));
    }
}
