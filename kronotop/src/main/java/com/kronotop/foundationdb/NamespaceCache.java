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

package com.kronotop.foundationdb;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.foundationdb.zmap.ZMapService;
import com.kronotop.server.resp.ChannelAttributes;
import com.kronotop.server.resp.Response;
import io.netty.util.Attribute;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

class NamespaceCache {
    synchronized static void add(Response response, List<String> subpath, DirectorySubspace directorySubspace) {
        String namespace = String.join(".", subpath);
        Attribute<ConcurrentMap<String, DirectorySubspace>> openNamespacesAttr = response.
                getContext().
                channel().
                attr(ChannelAttributes.OPEN_NAMESPACES);
        openNamespacesAttr.get().putIfAbsent(namespace, directorySubspace);

        Subspace zmapSubspace = directorySubspace.subspace(Tuple.from(ZMapService.SubspaceMagic));
        Attribute<ConcurrentMap<String, Subspace>> zmapSubspaceAttr = response.
                getContext().
                channel().
                attr(ChannelAttributes.ZMAP_SUBSPACES);
        zmapSubspaceAttr.get().putIfAbsent(namespace, zmapSubspace);
    }

    synchronized static void clear(Response response, List<String> subpath) {
        String namespace = String.join(".", subpath);
        Attribute<ConcurrentMap<String, DirectorySubspace>> openNamespacesAttr = response.
                getContext().
                channel().
                attr(ChannelAttributes.OPEN_NAMESPACES);
        openNamespacesAttr.get().remove(namespace);

        Attribute<ConcurrentMap<String, Subspace>> zmapSubspaceAttr = response.
                getContext().
                channel().
                attr(ChannelAttributes.ZMAP_SUBSPACES);
        zmapSubspaceAttr.get().remove(namespace);
    }

    synchronized static void replace(Response response, List<String> oldPath, List<String> newPath) {
        String oldNamespace = String.join(".", oldPath);
        String newNamespace = String.join(".", newPath);

        ConcurrentMap<String, DirectorySubspace> openNamespaces = response.
                getContext().
                channel().
                attr(ChannelAttributes.OPEN_NAMESPACES).get();
        DirectorySubspace namespace = openNamespaces.remove(oldNamespace);
        if (namespace != null) {
            openNamespaces.putIfAbsent(newNamespace, namespace);
        }

        ConcurrentMap<String, Subspace> zmapSubspaces = response.
                getContext().
                channel().
                attr(ChannelAttributes.ZMAP_SUBSPACES).get();
        Subspace zmapSubspace = zmapSubspaces.remove(oldNamespace);
        if (zmapSubspace != null) {
            zmapSubspaces.putIfAbsent(newNamespace, zmapSubspace);
        }
    }
}
