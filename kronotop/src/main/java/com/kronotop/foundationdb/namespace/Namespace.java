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

package com.kronotop.foundationdb.namespace;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

/**
 * The Namespace class represents a namespace in a directory hierarchy. It provides access to the root directory and
 * subspace objects associated with the namespace.
 */
public class Namespace {
    private static final byte ZMapSubspaceMagic = 0x01;
    private final String name;
    private final DirectorySubspace root;
    private final Subspace zmap;
    private long lastAccess;

    public Namespace(@Nonnull String name, @Nonnull DirectorySubspace root) {
        this.name = name;
        this.root = root;
        this.zmap = root.subspace(Tuple.from(ZMapSubspaceMagic));
        updateLastAccess();
    }

    private void updateLastAccess() {
        this.lastAccess = System.currentTimeMillis();
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
     * Returns the root directory subspace associated with the namespace.
     * <p>
     * This method updates the last access time and returns the root directory subspace.
     *
     * @return The root directory subspace.
     */
    public DirectorySubspace getRoot() {
        updateLastAccess();
        return root;
    }

    /**
     * Returns the Zmap subspace associated with the namespace.
     * <p>
     * This method updates the last access time and returns the Zmap subspace.
     *
     * @return The Zmap subspace.
     */
    public Subspace getZMap() {
        updateLastAccess();
        return zmap;
    }

    public long getLastAccess() {
        return lastAccess;
    }
}
