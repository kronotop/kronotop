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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.DataModelKind;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.internal.UUIDUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PrefixUtilTest extends BaseStandaloneInstanceTest {
    private TestBundle bundle;

    @BeforeEach
    public void prepareTestBundle() {
        createBucket("test");
        BucketMetadata metadata = getBucketMetadata("test");
        byte[] prefixPointer = BucketMetadataUtil.prefixBindingKey(metadata.pointerSubspace());
        bundle = new TestBundle(metadata, prefixPointer);
    }

    @Test
    void shouldRegisterPrefixSuccessfully() {
        try (Transaction tr = context.getTransactionManager().create()) {
            PrefixEntry entry = new PrefixEntry(bundle.prefixPointer, DataModelKind.BUCKET, UUIDUtil.toBytes(bundle.metadata.uuid()));
            assertDoesNotThrow(() ->
                    PrefixUtil.register(context, tr, bundle.metadata.prefix(), entry)
            );
            tr.commit().join();
        }
    }

    @Test
    void shouldDetectStalePrefixWhenPointerIsRemoved() {
        // Behavior: a registered prefix is not stale while its binding key exists and becomes stale once the key is cleared
        try (Transaction tr = context.getTransactionManager().create()) {
            PrefixEntry entry = new PrefixEntry(bundle.prefixPointer, DataModelKind.BUCKET, UUIDUtil.toBytes(bundle.metadata.uuid()));
            PrefixUtil.register(context, tr, bundle.metadata.prefix(), entry);
            tr.commit().join();
        }

        // Not stale, because prefixPointer still exists;
        try (Transaction tr = context.getTransactionManager().create()) {
            assertFalse(PrefixUtil.inspectPrefix(context, tr, bundle.metadata.prefix()).isStale());
        }

        // Make it stale, the prefix pointer somehow removed
        try (Transaction tr = context.getTransactionManager().create()) {
            tr.clear(bundle.prefixPointer);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            assertTrue(PrefixUtil.inspectPrefix(context, tr, bundle.metadata.prefix()).isStale());
        }
    }

    @Test
    void shouldThrowNoSuchPrefixAfterUnregister() {
        // Behavior: inspecting a prefix after unregister throws NoSuchPrefixException
        try (Transaction tr = context.getTransactionManager().create()) {
            PrefixEntry entry = new PrefixEntry(bundle.prefixPointer, DataModelKind.BUCKET, UUIDUtil.toBytes(bundle.metadata.uuid()));
            PrefixUtil.register(context, tr, bundle.metadata.prefix(), entry);
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            assertDoesNotThrow(() -> PrefixUtil.unregister(context, tr, bundle.prefixPointer, bundle.metadata.prefix()));
            tr.commit().join();
        }

        try (Transaction tr = context.getTransactionManager().create()) {
            assertThrows(NoSuchPrefixException.class, () -> PrefixUtil.inspectPrefix(context, tr, bundle.metadata.prefix()));
        }
    }

    record TestBundle(BucketMetadata metadata, byte[] prefixPointer) {
    }
}