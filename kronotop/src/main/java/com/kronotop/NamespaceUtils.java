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

package com.kronotop;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.namespace.NoSuchNamespaceException;
import com.kronotop.server.ChannelAttributes;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

/**
 * The NamespaceUtils class provides utility methods for manipulating and accessing namespaces.
 */
public class NamespaceUtils {

    /**
     * Clears all open namespaces associated with the given channel context.
     *
     * @param channelContext the ChannelHandlerContext object representing the channel context
     */
    public static void clearOpenNamespaces(ChannelHandlerContext channelContext) {
        Map<String, Namespace> namespaces = channelContext.channel().attr(ChannelAttributes.OPEN_NAMESPACES).get();
        namespaces.clear();
    }

    /**
     * Opens a Namespace in the specified context and channel context using the provided transaction.
     *
     * @param context        the Context object representing the context of a Kronotop instance
     * @param channelContext the ChannelHandlerContext object representing the channel context
     * @param tr             the Transaction object representing the transaction to perform
     * @return the Namespace object representing the opened namespace
     * @throws IllegalArgumentException if the namespace is not specified in the channel context
     * @throws NoSuchNamespaceException if the specified namespace does not exist
     * @throws KronotopException        if an exception occurs while opening the namespace
     */
    public static Namespace open(Context context, ChannelHandlerContext channelContext, Transaction tr) {
        String name = channelContext.channel().attr(ChannelAttributes.CURRENT_NAMESPACE).get();
        if (name == null) {
            throw new IllegalArgumentException("namespace not specified");
        }

        Map<String, Namespace> namespaces = channelContext.channel().attr(ChannelAttributes.OPEN_NAMESPACES).get();
        Namespace namespace = namespaces.get(name);
        if (namespace != null) {
            return namespace;
        }

        List<String> subpath = DirectoryLayout.Builder.
                clusterName(context.getClusterName()).
                namespaces().
                addAll(name.split("\\.")).
                asList();
        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
            namespace = new Namespace(name, subspace);
            namespaces.put(name, namespace);
            return namespace;
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(name);
            }
            throw new KronotopException(e.getCause());
        }
    }
}
