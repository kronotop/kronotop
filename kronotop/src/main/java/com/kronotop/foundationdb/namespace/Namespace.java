/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.DataStructureKind;

import java.util.Optional;

/**
 * Represents a Namespace in the system, providing access to specific subspaces
 * within a FoundationDB directory structure. A Namespace is a logical grouping
 * of related subspaces that can be used for organizing data storage.
 */
public class Namespace {
    public static String INTERNAL_LEAF = "__internal__";
    private final String name;
    private Subspace bucketSubspace;

    private DirectorySubspace zmap;
    private DirectorySubspace bucket;

    public Namespace(String name, DirectorySubspace root) {
        // TODO: TBD
        this.name = name;
        //this.bucketSubspace = root.subspace(Tuple.from(SubspaceMagic.BUCKET.getValue()));
    }

    public Optional<DirectorySubspace> get(DataStructureKind kind) {
        return switch (kind) {
            case ZMAP -> Optional.ofNullable(zmap);
            case BUCKET -> Optional.ofNullable(bucket);
        };
    }

    public void set(DataStructureKind kind, DirectorySubspace subspace) {
        switch (kind) {
            case ZMAP -> zmap = subspace;
            case BUCKET -> bucket = subspace;
        }
    }

    /**
     * Returns the name of the Namespace.
     *
     * @return The name of the Namespace.
     */
    public String getName() {
        return name;
    }

    /**
     * Retrieves the bucket subspace associated with this namespace.
     *
     * @return The Subspace object representing the bucket subspace.
     */
    public Subspace getBucket() {
        return bucketSubspace;
    }
}
