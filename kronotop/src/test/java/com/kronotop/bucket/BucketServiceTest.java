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
import com.kronotop.NamespaceUtils;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BucketServiceTest extends BaseClusterTest {

    private BucketService getBucketService() {
        KronotopTestInstance instance = getInstances().getFirst();
        return instance.getContext().getService(BucketService.NAME);
    }

    private Namespace createOrOpenNamespace(String name) {
        KronotopTestInstance instance = getInstances().getFirst();
        return NamespaceUtils.createOrOpen(instance.getContext(), name);
    }

    @Test
    public void test_getOrSetBucketPrefix() {
        String name = "third.two.one";
        KronotopTestInstance instance = getInstances().getFirst();
        BucketService service = getBucketService();
        Namespace namespace = createOrOpenNamespace(name);

        String firstBucketName = "first-bucket-name";
        Prefix createdPrefix;

        // First, create the prefix.
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            createdPrefix = service.getOrSetBucketPrefix(tr, namespace, firstBucketName);
            tr.commit().join();
        }

        // Retrieve the prefix again
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix retrievedPrefix = service.getOrSetBucketPrefix(tr, namespace, firstBucketName);
            assertEquals(createdPrefix, retrievedPrefix);
        }
    }

    @Test
    public void test_getOrSetBucketPrefix_from_different_namespaces() {
        KronotopTestInstance instance = getInstances().getFirst();
        BucketService service = getBucketService();

        String firstName = "third.two.one";
        Namespace firstNamespace = createOrOpenNamespace(firstName);

        String secondName = "third.two";
        Namespace secondNamespace = createOrOpenNamespace(secondName);

        String bucketName = "first-bucket-name";

        Prefix firstPrefix;
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            firstPrefix = service.getOrSetBucketPrefix(tr, firstNamespace, bucketName);
            tr.commit().join();
        }

        Prefix secondPrefix;
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            secondPrefix = service.getOrSetBucketPrefix(tr, secondNamespace, bucketName);
            tr.commit().join();
        }

        assertNotEquals(firstPrefix, secondPrefix);
    }
}