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
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * Represents a Namespace in the system, providing access to specific subspaces
 * within a FoundationDB directory structure. A Namespace is a logical grouping
 * of related subspaces that can be used for organizing data storage.
 */
public class Namespace {
    private final String name;
    private final Subspace zmapSubspace;
    private final Subspace bucketSubspace;

    public Namespace(@Nonnull String name, @Nonnull DirectorySubspace root) {
        this.name = name;
        this.zmapSubspace = root.subspace(Tuple.from(SubspaceMagic.ZMAP.getValue()));
        this.bucketSubspace = root.subspace(Tuple.from(SubspaceMagic.BUCKET.getValue()));
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
     * Returns the Zmap subspace associated with the namespace.
     *
     * @return The Zmap subspace.
     */
    public Subspace getZMap() {
        return zmapSubspace;
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
