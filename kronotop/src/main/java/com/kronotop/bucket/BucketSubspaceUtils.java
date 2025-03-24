// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;

import java.util.Map;

public class BucketSubspaceUtils {

    /**
     * Opens a {@code BucketSubspace} for the current namespace associated with the provided session.
     * If a {@code BucketSubspace} for the namespace is already open, it will return the existing instance.
     * Otherwise, it will initialize a new {@code BucketSubspace} and store it for further use.
     *
     * @param context the application context containing environment-specific information
     * @param session the session object storing attributes including the current namespace
     * @param tr      the transaction object for performing operations within the namespace
     * @return the {@code BucketSubspace} corresponding to the current namespace
     * @throws IllegalArgumentException if the current namespace is not specified in the session
     */
    public static BucketSubspace open(Context context, Session session, Transaction tr) {
        String name = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
        if (name == null) {
            throw new IllegalArgumentException("namespace not specified");
        }

        Map<String, BucketSubspace> bucketSubspaces = session.attr(SessionAttributes.OPEN_BUCKET_SUBSPACES).get();
        BucketSubspace bucketSubspace = bucketSubspaces.get(name);
        if (bucketSubspace != null) {
            return bucketSubspace;
        }

        Namespace namespace = NamespaceUtils.open(tr, context.getClusterName(), name);
        bucketSubspace = new BucketSubspace(namespace);
        bucketSubspaces.put(name, bucketSubspace);
        return bucketSubspace;
    }
}
