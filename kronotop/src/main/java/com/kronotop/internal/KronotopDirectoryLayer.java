/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.internal;

import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.Hashing;
import com.typesafe.config.Config;

import java.nio.charset.StandardCharsets;

/**
 * Builds the {@link DirectoryLayer} a Kronotop instance uses for every directory operation.
 *
 * <p>By default, the global {@link DirectoryLayer#getDefault()} is returned, which shares its
 * node subspace, high-contention allocator, and parent directory metadata with every other
 * cluster on the same FoundationDB. When the optional {@code directory.root} key is present in
 * the configuration, a layer scoped under a dedicated prefix is returned instead: its node
 * subspace and content subspace live entirely under that prefix, so two layers built from
 * different roots share no FoundationDB keys at all. This isolation lets independent clusters
 * (for example, concurrent test instances) create directories without contending on a shared
 * allocator.</p>
 */
public final class KronotopDirectoryLayer {
    public static final String ROOT_CONFIG_KEY = "directory.root";

    private KronotopDirectoryLayer() {
    }

    /**
     * Returns the DirectoryLayer for the given configuration. When {@code directory.root} is set,
     * the returned layer is scoped under a prefix derived from that value; otherwise the global
     * default layer is returned.
     *
     * @param config the instance configuration
     * @return the DirectoryLayer to use for all directory operations
     */
    public static DirectoryLayer fromConfig(Config config) {
        if (config.hasPath(ROOT_CONFIG_KEY)) {
            return scoped(config.getString(ROOT_CONFIG_KEY));
        }
        return DirectoryLayer.getDefault();
    }

    /**
     * Builds a DirectoryLayer scoped under a prefix derived from {@code root}. The root is reduced to
     * a 64-bit SipHash-2-4 digest and Tuple-encoded so the prefix prepended to every key stays short
     * and bounded regardless of the root string length, and always falls in the legal user key range
     * (a raw digest could begin with {@code 0xFF} and land in FoundationDB's system keyspace). The
     * layout mirrors a FoundationDB directory partition: the content subspace is the encoded digest
     * and the node subspace (which holds the allocator and root node) sits at {@code prefix + 0xFE}.
     *
     * @param root the scope identifier
     * @return a DirectoryLayer isolated under the derived prefix
     */
    public static DirectoryLayer scoped(String root) {
        long digest = Hashing.sipHash24().hashBytes(root.getBytes(StandardCharsets.UTF_8)).asLong();
        byte[] prefix = Tuple.from(digest).pack();
        Subspace contentSubspace = new Subspace(prefix);
        Subspace nodeSubspace = new Subspace(ByteArrayUtil.join(prefix, DirectoryLayer.DEFAULT_NODE_SUBSPACE.getKey()));
        return new DirectoryLayer(nodeSubspace, contentSubspace);
    }
}
