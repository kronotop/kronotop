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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.NamespaceUtils;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.foundationdb.namespace.Namespace;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PrefixUtilsTest extends BaseStandaloneInstanceTest {

    private TestBundle prepareTestBundle() {
        Namespace namespace = NamespaceUtils.createOrOpen(context, "prefix-utils-test");
        BucketSubspace subspace = new BucketSubspace(namespace);
        byte[] prefixPointer = subspace.getBucketKey("test.bucket");
        Prefix prefix = new Prefix("test-prefix");
        return new TestBundle(subspace, prefix, prefixPointer);
    }

    @Test
    void test_register() {
        TestBundle bundle = prepareTestBundle();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> PrefixUtils.register(context, tr, bundle.prefixPointer, bundle.prefix));
            tr.commit().join();
        }
    }

    @Test
    void test_isStale() {
        TestBundle bundle = prepareTestBundle();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixUtils.register(context, tr, bundle.prefixPointer, bundle.prefix);
            tr.commit().join();
        }

        // Not stale, because prefixPointer still exists;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(PrefixUtils.isStale(context, tr, bundle.prefix));
        }

        // Make it stale, the prefix pointer somehow removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(bundle.prefixPointer);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(PrefixUtils.isStale(context, tr, bundle.prefix));
        }
    }

    @Test
    void test_unregister() {
        TestBundle bundle = prepareTestBundle();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixUtils.register(context, tr, bundle.prefixPointer, bundle.prefix);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> PrefixUtils.unregister(context, tr, bundle.prefixPointer, bundle.prefix));
        }

        // Not stale, because it's unregistered
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(PrefixUtils.isStale(context, tr, bundle.prefix));
        }
    }

    record TestBundle(BucketSubspace subspace, Prefix prefix, byte[] prefixPointer) {
    }
}