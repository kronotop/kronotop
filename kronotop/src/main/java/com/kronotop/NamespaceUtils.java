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

package com.kronotop;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.namespace.NoSuchNamespaceException;
import com.kronotop.server.ChannelAttributes;
import io.netty.channel.ChannelHandlerContext;

import javax.annotation.Nonnull;
import java.util.ArrayList;
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

    private static List<String> splitNamespaceHierarchy(String name) {
        return new ArrayList<>(List.of(name.split("\\.")));
    }

    /**
     * Opens a namespace within the specified cluster and uses the provided transaction
     * to access the directory subspace associated with the namespace.
     *
     * @param clusterName the name of the cluster to which the namespace belongs
     * @param name        the name of the namespace to open
     * @param tr          the transaction to use for opening the namespace
     * @return the Namespace object representing the opened namespace
     * @throws NoSuchNamespaceException if the specified namespace does not exist
     * @throws KronotopException        if an exception occurs while opening the namespace
     */
    public static Namespace open(String clusterName, @Nonnull String name, Transaction tr) {
        List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).namespaces().namespace(splitNamespaceHierarchy(name)).toList();
        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, subpath).join();
            return new Namespace(name, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(name);
            }
            throw new KronotopException(e.getCause());
        }
    }

    /**
     * Creates or opens a namespace in the given context. If the namespace already exists, it is opened;
     * otherwise, it is created and then opened. The operation is performed using FoundationDB transactions
     * with a retry mechanism in case of transactional conflicts.
     *
     * @param context the Context object representing the Kronotop instance's operational context
     * @param name    the name of the namespace to create or open
     * @return the Namespace object that represents the created or opened namespace
     * @throws KronotopException if an exception occurs during the operation
     */
    public static Namespace createOrOpen(Context context, String name) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).namespaces().namespace(splitNamespaceHierarchy(name)).toList();
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return new Namespace(name, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // retry
                    return createOrOpen(context, name);
                }
            }
            throw new KronotopException(e);
        }
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

        namespace = open(context.getClusterName(), name, tr);
        namespaces.put(name, namespace);
        return namespace;
    }

    /**
     * Checks if a specified namespace exists in the Kronotop directory within the given context.
     *
     * @param context the Context object representing the context of a Kronotop instance
     * @param names   the list of names representing the hierarchical namespace path
     * @return true if the namespace exists, false otherwise
     */
    public static boolean exists(Context context, List<String> names) {
        List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).namespaces().namespace(names).toList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return DirectoryLayer.getDefault().exists(tr, subpath).join();
        }
    }

    /**
     * Removes a namespace specified by a list of names within the provided context.
     * This method performs the removal using a transaction and retries in case of
     * transactional conflicts.
     *
     * @param context the Context object representing the context of a Kronotop instance
     * @param names   the list of strings representing the hierarchical namespace path to be removed
     */
    public static void remove(Context context, List<String> names) {
        List<String> subpath = KronotopDirectory.kronotop().cluster(context.getClusterName()).namespaces().namespace(names).toList();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectoryLayer.getDefault().remove(tr, subpath).join();
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // retry
                    remove(context, names);
                    return;
                }
            }
            throw e;
        }
    }

    public static void remove(Context context, String name) {
        remove(context, splitNamespaceHierarchy(name));
    }
}
