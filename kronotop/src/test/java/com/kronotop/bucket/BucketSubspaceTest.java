// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.kronotop.BaseClusterTest;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BucketSubspaceTest extends BaseClusterTest {

    @Test
    void test_bucket_prefix_methods() {
        KronotopTestInstance instance = getInstances().getFirst();
        Context context = instance.getContext();

        String name = "one.two.three";
        Namespace namespace = NamespaceUtils.createOrOpen(context, name);
        BucketSubspace subspace = new BucketSubspace(namespace);

        String bucket = "bucket-name";
        Prefix prefix = new Prefix("prefix-name");

        assertDoesNotThrow(() -> subspace.setBucketPrefix(bucket, prefix));
        assertEquals(prefix, subspace.getBucketPrefix(bucket));
        assertNotNull(subspace.getBucketIndexSubspace(1, prefix));
        assertNotNull(subspace.getBucketKey(bucket));
    }

    @Test
    void test_cleanupBucket() {
        KronotopTestInstance instance = getInstances().getFirst();
        Context context = instance.getContext();

        String name = "one.two.three";
        Namespace namespace = NamespaceUtils.createOrOpen(context, name);
        BucketSubspace subspace = new BucketSubspace(namespace);

        String bucket = "bucket-name";
        Prefix prefix = new Prefix("prefix-name");

        assertDoesNotThrow(() -> subspace.setBucketPrefix(bucket, prefix));
        assertDoesNotThrow(() -> subspace.cleanupBucket(bucket));
    }
}