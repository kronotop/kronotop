/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

class Node {
    DirectorySubspace subspace;
    ConcurrentHashMap<String, Node> nodes;

    Node(DirectorySubspace subspace) {
        this.subspace = subspace;
        this.nodes = new ConcurrentHashMap<>();
    }

    public DirectorySubspace getSubspace() {
        return subspace;
    }

    public Node getLeaf(Database db, String name) {
        return nodes.compute(name, (k, value) -> {
            if (value == null) {
                try (Transaction tr = db.createTransaction()) {
                    DirectorySubspace directorySubspace = subspace.createOrOpen(tr, Collections.singletonList(name)).join();
                    tr.commit().join();
                    return new Node(directorySubspace);
                }
            }
            return value;
        });
    }
}