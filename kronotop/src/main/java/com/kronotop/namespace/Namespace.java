// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;

import java.util.List;
import java.util.UUID;

public class Namespace {
    private final DirectorySubspace buckets;

    public Namespace(Context context) {
        KronotopDirectoryNode node = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                namespaces().
                datastructures().
                buckets();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            this.buckets = DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join();
            tr.commit().join();
        }
    }

    public byte[] createOrOpen(Transaction tr, String name) {
        return createOrOpen(tr, List.of(name.split("\\.")));
    }

    public byte[] createOrOpen(Transaction tr, List<String> path) {
        Subspace head = null;
        for (String item : path) {
            if (head == null) {
                head = buckets.subspace(Tuple.from(item));
            } else {
                head = head.subspace(Tuple.from(item));
            }
        }
        assert head != null;
        byte[] key = head.pack();
        byte[] value = tr.get(key).join();
        if (value != null) {
            // open
            return value;
        }

        // create
        value = UUID.randomUUID().toString().getBytes();
        tr.set(head.pack(), value);
        return value;
    }



}
