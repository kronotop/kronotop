// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.bucket.BucketPrefix;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.DefaultIndex;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexBuilderTest extends BaseClusterTest {
    final byte[] data = "test-data".getBytes();

    @Test
    void test_packIndex_default_index_id() {
        KronotopTestInstance instance = getInstances().getFirst();
        Namespace namespace = NamespaceUtils.createOrOpen(instance.getContext(), "index-builder-test");
        BucketSubspace subspace = new BucketSubspace(namespace);

        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix prefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, "test-bucket");
            assertDoesNotThrow(() -> IndexBuilder.packIndex(tr, subspace, 1, prefix, 1, DefaultIndex.ID, data));
            tr.commit().join();
        }
    }

    @Test
    void pack_index_then_unpack() {
        KronotopTestInstance instance = getInstances().getFirst();
        Namespace namespace = NamespaceUtils.createOrOpen(instance.getContext(), "index-builder-test");
        BucketSubspace subspace = new BucketSubspace(namespace);

        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix prefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, "test-bucket");
            assertDoesNotThrow(() -> IndexBuilder.packIndex(tr, subspace, 1, prefix, 1, DefaultIndex.ID, data));
            tr.commit().join();
        }

        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix prefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, "test-bucket");
            Subspace indexSubspace = subspace.getBucketIndexSubspace(1, prefix);
            List<KeyValue> items = tr.getRange(new Range(indexSubspace.pack(), ByteArrayUtil.strinc(indexSubspace.pack()))).asList().join();
            for (KeyValue item : items) {
                UnpackedIndex unpackedIndex = IndexBuilder.unpackIndex(indexSubspace, item.getKey());
                assertNotNull(unpackedIndex.versionstamp());
                assertEquals(DefaultIndex.ID, unpackedIndex.index());
            }
        }
    }
}