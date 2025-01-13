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
 * The Namespace class represents a namespace in a directory hierarchy. It provides access to the root directory and
 * subspace objects associated with the namespace.
 */
public class Namespace {
    private static final byte ZMapSubspaceMagic = 0x01;
    private static final byte RedisSubspaceMagic = 0x02;
    private final String name;
    private final DirectorySubspace directorySubspace;
    private final Subspace zmapSubspace;
    private final Subspace redisSubspace;

    public Namespace(@Nonnull String name, @Nonnull DirectorySubspace root) {
        this.name = name;
        this.directorySubspace = root;
        this.zmapSubspace = root.subspace(Tuple.from(ZMapSubspaceMagic));
        this.redisSubspace = root.subspace(Tuple.from(RedisSubspaceMagic));
    }

    /**
     * Returns the root subspace of the Namespace.
     *
     * @return The root subspace.
     */
    public DirectorySubspace getDirectorySubspace() {
        return directorySubspace;
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

    public Subspace getRedis() {
        return redisSubspace;
    }
}
