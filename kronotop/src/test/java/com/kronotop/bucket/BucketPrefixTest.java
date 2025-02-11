package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.NamespaceUtils;
import com.kronotop.directory.Volumes;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.volume.Prefix;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class BucketPrefixTest extends BaseClusterTest {

    private Namespace createOrOpenNamespace(String name) {
        KronotopTestInstance instance = getInstances().getFirst();
        return NamespaceUtils.createOrOpen(instance.getContext(), name);
    }

    @Test
    public void test_getOrSetBucketPrefix() {
        String name = "third.two.one";
        KronotopTestInstance instance = getInstances().getFirst();
        Namespace namespace = createOrOpenNamespace(name);

        String firstBucketName = "first-bucket-name";
        Prefix createdPrefix;

        // First, create the prefix.
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            createdPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, namespace, firstBucketName);
            tr.commit().join();
        }

        // Retrieve the prefix again
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            Prefix retrievedPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, namespace, firstBucketName);
            assertEquals(createdPrefix, retrievedPrefix);
        }
    }

    @Test
    public void test_getOrSetBucketPrefix_from_different_namespaces() {
        KronotopTestInstance instance = getInstances().getFirst();

        String firstName = "third.two.one";
        Namespace firstNamespace = createOrOpenNamespace(firstName);

        String secondName = "third.two";
        Namespace secondNamespace = createOrOpenNamespace(secondName);

        String bucketName = "first-bucket-name";

        Prefix firstPrefix;
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            firstPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, firstNamespace, bucketName);
            tr.commit().join();
        }

        Prefix secondPrefix;
        try (Transaction tr = instance.getContext().getFoundationDB().createTransaction()) {
            secondPrefix = BucketPrefix.getOrSetBucketPrefix(instance.getContext(), tr, secondNamespace, bucketName);
            tr.commit().join();
        }

        assertNotEquals(firstPrefix, secondPrefix);
    }

}