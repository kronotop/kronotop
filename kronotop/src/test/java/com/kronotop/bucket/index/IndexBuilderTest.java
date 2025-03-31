// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.index;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.bucket.BucketPrefix;
import com.kronotop.bucket.BucketSubspace;
import com.kronotop.bucket.DefaultIndex;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class IndexBuilderTest extends BaseClusterTest {
    final byte[] data = "test-data".getBytes();

    @Test
    void test_setIndex_default_index_id() {
        KronotopTestInstance instance = getInstances().getFirst();
        Namespace namespace = NamespaceUtils.createOrOpen(instance.getContext(), "index-builder-test");
        BucketSubspace subspace = new BucketSubspace(namespace);

        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix prefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, "test-bucket");
            assertDoesNotThrow(() -> IndexBuilder.setIndex(tr, subspace, 1, prefix, 1, DefaultIndex.ID, data));
            tr.commit().join();
        }
    }
}