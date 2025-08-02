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

package com.kronotop.internal;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.namespace.NamespaceAlreadyExistsException;
import com.kronotop.foundationdb.namespace.NoSuchNamespaceException;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

/**
 * The NamespaceUtils class provides utility methods for manipulating and accessing namespaces.
 */
public class NamespaceUtil {

    private static List<String> splitNamespaceHierarchy(String name) {
        return new ArrayList<>(List.of(name.split("\\.")));
    }

    private static DirectorySubspace open(Transaction tr, String clusterName, String name, List<String> names) {
        List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).namespaces().namespace(names).toList();
        try {
            return DirectoryLayer.getDefault().open(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(name);
            }
            throw new KronotopException(e.getCause());
        }
    }

    private static DirectorySubspace open(Transaction tr, String clusterName, String name, DataStructureKind kind) {
        List<String> subpath = splitNamespaceHierarchy(name);
        subpath.add(Namespace.INTERNAL_LEAF);
        subpath.add(kind.name().toLowerCase());
        return open(tr, clusterName, name, subpath);
    }

    /**
     * Opens a specific data structure subspace within the current namespace of the session.
     * If the namespace or data structure subspace does not exist, it is created and added to the session.
     *
     * @param context the Context object representing the operational environment.
     * @param tr      the Transaction object used for operations within the FoundationDB environment.
     * @param session the Session object containing state and attributes for the current execution context.
     * @param kind    the kind of data structure to open or create.
     * @return the DirectorySubspace object representing the opened or created data structure subspace.
     * @throws IllegalArgumentException if the namespace is not specified in the session.
     */
    public static DirectorySubspace openDataStructureSubspace(Context context, Transaction tr, Session session, DataStructureKind kind) {
        String name = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
        if (name == null) {
            throw new IllegalArgumentException("namespace not specified");
        }

        Map<String, Namespace> namespaces = session.attr(SessionAttributes.OPEN_NAMESPACES).get();
        Namespace namespace = namespaces.get(name);
        if (namespace == null) {
            DirectorySubspace subspace = open(tr, context.getClusterName(), name, kind);
            namespace = new Namespace();
            namespace.set(kind, subspace);
            namespaces.put(name, namespace);
            return subspace;
        }

        Optional<DirectorySubspace> dataStructureSubspace = namespace.get(kind);
        if (dataStructureSubspace.isEmpty()) {
            DirectorySubspace subspace = open(tr, context.getClusterName(), name, kind);
            namespace.set(kind, subspace);
            return subspace;
        }
        return dataStructureSubspace.orElseThrow();
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
     * Removes a namespace represented by the hierarchical namespace path from the given cluster
     * using the specified transaction. It interacts with the Kronotop directory structure to
     * resolve the namespace and performs the removal operation synchronously.
     *
     * @param tr          the Transaction object used to perform operations within the FoundationDB environment
     * @param clusterName the name of the cluster containing the namespace to be removed
     * @param names       the list of strings representing the hierarchical namespace path to be removed
     */
    public static void remove(Transaction tr, String clusterName, List<String> names) {
        List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).namespaces().namespace(names).toList();
        DirectoryLayer.getDefault().remove(tr, subpath).join();
    }

    /**
     * Removes a namespace from the cluster using the given list of hierarchical names.
     * This method attempts to remove the namespace via a transactional operation in the FoundationDB environment,
     * with a retry mechanism in case of transactional conflicts.
     *
     * @param context the Context object representing the operational context of the Kronotop instance.
     * @param names   the list of strings representing the hierarchical namespace path to be removed.
     */
    public static void remove(Context context, List<String> names) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            remove(tr, context.getClusterName(), names);
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

    private static String dottedNamespace(List<String> items) {
        return String.join(".", items);
    }

    private static List<String> getNamespaceSubpath(Context context, List<String> subpath) {
        return KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                namespaces().
                namespace(subpath).
                toList();
    }

    /**
     * Creates a namespace in the Kronotop environment using the provided context and namespace name.
     * This method uses a FoundationDB transaction to perform the operation, ensuring transactional
     * consistency. Internally, it delegates the creation process to another method that works with
     * the split hierarchical namespace format.
     *
     * @param context   the Context object representing the operational context of the Kronotop instance.
     * @param namespace the name of the namespace to be created in the Kronotop environment.
     * @throws KronotopException if an unexpected error occurs during the operation.
     * @throws RuntimeException  if an error occurs that is not specifically handled.
     */
    public static void create(Context context, String namespace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            create(context, tr, splitNamespaceHierarchy(namespace));
        }
    }

    /**
     * Creates a namespace in the Kronotop environment by interacting with the FoundationDB directory layer.
     * This method creates the primary namespace and initializes internal data structure subpaths within it.
     * If the specified namespace already exists, an exception is thrown.
     * The operation is performed using a Transaction object with retry logic in case of conflicts.
     *
     * @param context the Context object representing the operational environment of the Kronotop instance.
     * @param tr      the Transaction object used to perform operations within the FoundationDB environment.
     * @param subpath the list of strings representing the hierarchical namespace path to be created.
     * @throws NamespaceAlreadyExistsException if the namespace already exists.
     * @throws KronotopException               if an unexpected error occurs during the operation.
     * @throws RuntimeException                if an error occurs that is not specifically handled.
     */
    public static void create(Context context, Transaction tr, List<String> subpath) {
        List<String> namespaceSubpath = getNamespaceSubpath(context, subpath);
        try {
            DirectoryLayer.getDefault().create(tr, namespaceSubpath).join();
            for (DataStructureKind kind : DataStructureKind.values()) {
                List<String> dataStructureSubpath = new ArrayList<>(namespaceSubpath);
                dataStructureSubpath.add(Namespace.INTERNAL_LEAF);
                dataStructureSubpath.add(kind.name().toLowerCase());
                DirectoryLayer.getDefault().create(tr, dataStructureSubpath).join();
            }
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new NamespaceAlreadyExistsException(dottedNamespace(subpath));
            }
            throw new KronotopException(e.getCause());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        try {
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // retry
                    try (Transaction retryTr = context.getFoundationDB().createTransaction()) {
                        create(context, retryTr, subpath);
                    }
                    return;
                }
            }
            throw new KronotopException(e.getCause());
        }
    }
}
