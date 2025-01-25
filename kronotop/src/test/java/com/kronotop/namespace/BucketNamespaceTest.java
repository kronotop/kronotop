package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.kronotop.BaseMetadataStoreTest;
import org.junit.jupiter.api.Test;

import java.util.List;

class BucketNamespaceTest extends BaseMetadataStoreTest {
    @Test
    public void test() {
        Namespace namespace = new Namespace(context);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] id = namespace.createOrOpen(tr, List.of("production", "users", "kronotop"));
            System.out.println(new String(id));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] id = namespace.createOrOpen(tr, List.of("production", "users", "kronotop"));
            System.out.println(new String(id));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] id = namespace.createOrOpen(tr, "production.users.buraksezer");
            System.out.println(new String(id));
            tr.commit().join();
        }
    }
}