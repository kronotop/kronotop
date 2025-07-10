// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BucketPrefixTest extends BaseClusterTest {

    private BucketSubspace createOrOpenBucketSubspace(String name) {
        KronotopTestInstance instance = getInstances().getFirst();
        Namespace namespace = NamespaceUtils.createOrOpen(instance.getContext(), name);
        return new BucketSubspace(namespace);
    }

    @Test
    public void test_getOrSetBucketPrefix() {
        String name = "third.two.one";
        KronotopTestInstance instance = getInstances().getFirst();
        BucketSubspace subspace = createOrOpenBucketSubspace(name);

        String firstBucketName = "first-bucket-name";
        Prefix createdPrefix;

        // First, create the prefix.
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            createdPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, firstBucketName);
            tr.commit().join();
        }

        // Retrieve the prefix again
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix retrievedPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, firstBucketName);
            assertEquals(createdPrefix, retrievedPrefix);
        }
    }

    @Test
    public void test_getOrSetBucketPrefix_from_different_namespaces() {
        KronotopTestInstance instance = getInstances().getFirst();

        String firstName = "third.two.one";
        BucketSubspace firstSubspace = createOrOpenBucketSubspace(firstName);

        String secondName = "third.two";
        BucketSubspace secondSubspace = createOrOpenBucketSubspace(secondName);

        String bucketName = "first-bucket-name";

        Prefix firstPrefix;
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            firstPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, firstSubspace, bucketName);
            tr.commit().join();
        }

        Prefix secondPrefix;
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            secondPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, secondSubspace, bucketName);
            tr.commit().join();
        }

        assertNotEquals(firstPrefix, secondPrefix);
    }

    @Test
    public void test_listBucketPrefixes() {
        String name = "third.two.one";
        KronotopTestInstance instance = getInstances().getFirst();
        BucketSubspace subspace = createOrOpenBucketSubspace(name);

        Map<String, Prefix> expectedResult = new HashMap<>();
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            for (int i = 0; i < 10; i++) {
                String bucketName = String.format("bucket-%d", i);
                Prefix prefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, subspace, bucketName);
                expectedResult.put(bucketName, prefix);
            }
            tr.commit().join();
        }

        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Map<String, Prefix> result = BucketPrefix.listBucketPrefixes(tr, subspace);
            assertThat(expectedResult).usingRecursiveComparison().isEqualTo(result);
        }
    }
}