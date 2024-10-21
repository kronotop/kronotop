/*
 * Copyright (c) 2023-2024 Kronotop
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

package com.kronotop.cluster;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Coordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);
    private static final String CLUSTER_COORDINATOR_KEY = "cluster-coordinator";
    private final Context context;
    private final MemberRegistry registry;
    private final DirectorySubspace clusterMetadataSubspace;

    public Coordinator(Context context, MemberRegistry registry) {
        this.context = context;
        this.registry = registry;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopDirectoryNode directory = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata();
            this.clusterMetadataSubspace = DirectoryLayer.getDefault().createOrOpen(tr, directory.toList()).join();
        }
    }

    Member getCoordinator(ReadTransaction tr) {
        byte[] data = tr.get(clusterMetadataSubspace.pack(Tuple.from(CLUSTER_COORDINATOR_KEY))).join();
        if (data == null) {
            return null;

        }
        String memberId = new String(data);
        return registry.findMember(tr, memberId);
    }

    Member getCoordinator() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return getCoordinator(tr);
        }
    }

    void initiateCoordinatorElection() {
        // 1- 2. node bunu calistirir
        // 2- eger baska bi node, ayni node bile olsa processID'si tutmuyorsa,
        // yeni election baslatamaz.
        // 3- stuck olma durumunda insan mudahalesi gerekir.
        // 4- tum node'lar kendi bildikleri coordinator icin oy verirler
        // 5-

    }
}
