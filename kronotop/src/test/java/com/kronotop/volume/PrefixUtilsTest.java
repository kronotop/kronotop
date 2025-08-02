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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Session;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PrefixUtilsTest extends BaseStandaloneInstanceTest {

    private TestBundle prepareTestBundle() {
        MockChannelHandlerContext ctx = new MockChannelHandlerContext(instance.getChannel());
        Session.registerSession(context, ctx);
        Session session = Session.extractSessionFromChannel(instance.getChannel());

        BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, "test");
        byte[] prefixPointer = BucketMetadataUtil.prefixKey(metadata.subspace());
        return new TestBundle(metadata, prefixPointer);
    }

    @Test
    void test_register() {
        TestBundle bundle = prepareTestBundle();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> PrefixUtil.register(context, tr, bundle.prefixPointer, bundle.metadata.volumePrefix()));
            tr.commit().join();
        }
    }

    @Test
    void test_isStale() {
        TestBundle bundle = prepareTestBundle();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixUtil.register(context, tr, bundle.prefixPointer, bundle.metadata.volumePrefix());
            tr.commit().join();
        }

        // Not stale, because prefixPointer still exists;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(PrefixUtil.isStale(context, tr, bundle.metadata.volumePrefix()));
        }

        // Make it stale, the prefix pointer somehow removed
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.clear(bundle.prefixPointer);
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertTrue(PrefixUtil.isStale(context, tr, bundle.metadata.volumePrefix()));
        }
    }

    @Test
    void test_unregister() {
        TestBundle bundle = prepareTestBundle();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            PrefixUtil.register(context, tr, bundle.prefixPointer, bundle.metadata.volumePrefix());
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> PrefixUtil.unregister(context, tr, bundle.prefixPointer, bundle.metadata.volumePrefix()));
        }

        // Not stale, because it's unregistered
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertFalse(PrefixUtil.isStale(context, tr, bundle.metadata.volumePrefix()));
        }
    }

    record TestBundle(BucketMetadata metadata, byte[] prefixPointer) {
    }
}