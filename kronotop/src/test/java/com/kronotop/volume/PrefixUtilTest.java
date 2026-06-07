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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
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
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> PrefixUtil.register(context, tr, bundle.prefixPointer, bundle.metadata.prefix()));
            tr.commit().join();
        }
    }

    @Test
    void shouldDetectStalePrefixWhenPointerIsRemoved() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixUtil.register(context, tr, bundle.prefixPointer, bundle.metadata.prefix());
            tr.commit().join();
        }

        // Not stale, because prefixPointer still exists;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(PrefixUtil.isStale(context, tr, bundle.metadata.prefix()));
        }

        // Make it stale, the prefix pointer somehow removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(bundle.prefixPointer);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(PrefixUtil.isStale(context, tr, bundle.metadata.prefix()));
        }
    }

    @Test
    void shouldUnregisterPrefixAndNotBeStale() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixUtil.register(context, tr, bundle.prefixPointer, bundle.metadata.prefix());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> PrefixUtil.unregister(context, tr, bundle.prefixPointer, bundle.metadata.prefix()));
        }

        // Not stale, because it's unregistered
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(PrefixUtil.isStale(context, tr, bundle.metadata.prefix()));
        }
    }

    record TestBundle(BucketMetadata metadata, byte[] prefixPointer) {
    }
}