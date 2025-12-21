/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.namespace;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.cluster.MemberSubspace;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.namespace.handlers.Namespace;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import com.kronotop.namespace.handlers.NamespaceMetadataField;
import com.kronotop.namespace.handlers.NamespaceRemovedEvent;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletionException;

import static com.google.common.hash.Hashing.sipHash24;

/**
 * The NamespaceUtils class provides utility methods for manipulating and accessing namespaces.
 */
public class NamespaceUtil {

    private static List<String> splitNamespaceHierarchy(String namespace) {
        return new ArrayList<>(List.of(namespace.split("\\.")));
    }

    public static DirectorySubspace open(Transaction tr, String clusterName, List<String> subpath) {
        List<String> namespaceSubpath = KronotopDirectory.
                kronotop().
                cluster(clusterName).
                namespaces().
                namespace(subpath).
                toList();
        try {
            return DirectoryLayer.getDefault().open(tr, namespaceSubpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(dottedNamespace(subpath));
            }
            throw e;
        }
    }

    public static DirectorySubspace open(Transaction tr, String clusterName, String namespace) {
        return open(tr, clusterName, splitNamespaceHierarchy(namespace));
    }

    /**
     * Opens the directory subspace for a data structure within a namespace.
     *
     * @param tr          the FoundationDB transaction
     * @param clusterName the cluster name
     * @param namespace   the dot-separated namespace (e.g., "a.b.c")
     * @param kind        the data structure kind
     * @return the DirectorySubspace for the data structure
     * @throws NoSuchNamespaceException       if the namespace does not exist
     * @throws NamespaceBeingRemovedException if the namespace is marked for removal
     */
    public static DirectorySubspace open(Transaction tr, String clusterName, String namespace, DataStructureKind kind) {
        try {
            List<String> hierarchy = splitNamespaceHierarchy(namespace);
            checkBeingRemoved(tr, clusterName, hierarchy);
            hierarchy.add(Namespace.INTERNAL_LEAF);
            hierarchy.add(kind.name().toLowerCase());
            List<String> subpath = KronotopDirectory.kronotop().cluster(clusterName).namespaces().namespace(hierarchy).toList();
            return DirectoryLayer.getDefault().open(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(namespace);
            }
            throw new KronotopException(e.getCause());
        }
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
            boolean exists = DirectoryLayer.getDefault().exists(tr, subpath).join();
            if (!exists) {
                return false;
            }
            checkBeingRemoved(tr, context.getClusterName(), names);
            return true;
        }
    }

    public static void checkBeingRemoved(Transaction tr, String clusterName, List<String> hierarchy) {
        List<String> names = new ArrayList<>();
        for (String name : hierarchy) {
            names.add(name);
            NamespaceMetadata metadata = readMetadata(tr, clusterName, names);
            if (metadata.removed()) {
                throw new NamespaceBeingRemovedException(dottedNamespace(names));
            }
        }
    }

    /**
     * Removes a namespace represented by the hierarchical namespace path from the given cluster
     * using the specified transaction. It interacts with the Kronotop directory structure to
     * resolve the namespace and performs the removal operation synchronously.
     * <p>
     * <b>IMPORTANT</b>: This method removes the namespace from FDB, use it carefully.
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
     * <p>
     * <b>IMPORTANT</b>: This method removes the namespace from FDB, use it carefully.
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
     * @throws NamespaceAlreadyExistsException if the namespace already exists.
     * @throws NamespaceBeingRemovedException  if the namespace exists but is marked for removal.
     */
    public static void create(Context context, String namespace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            create(context, tr, splitNamespaceHierarchy(namespace));
        }
    }

    private static void writeMetadata(Transaction tr, DirectorySubspace subspace, List<String> subpath) {
        UUID uuid = UUID.randomUUID();
        long id = sipHash24().hashBytes(uuid.toString().getBytes()).asLong();
        NamespaceMetadata metadata = new NamespaceMetadata(id, dottedNamespace(subpath), 0, false);

        byte[] idKey = subspace.pack(Tuple.from(Namespace.METADATA, NamespaceMetadataField.ID.getValue()));
        tr.set(idKey, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(metadata.id()).array());

        byte[] nameKey = subspace.pack(Tuple.from(Namespace.METADATA, NamespaceMetadataField.NAME.getValue()));
        tr.set(nameKey, metadata.name().getBytes(StandardCharsets.UTF_8));

        byte[] removedKey = subspace.pack(Tuple.from(Namespace.METADATA, NamespaceMetadataField.REMOVED.getValue()));
        tr.set(removedKey, metadata.removed() ? new byte[]{1} : new byte[]{0});

        byte[] versionKey = subspace.pack(Tuple.from(Namespace.METADATA, NamespaceMetadataField.VERSION.getValue()));
        tr.set(versionKey, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(metadata.version()).array());
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
     * @throws NamespaceBeingRemovedException  if the namespace exists but is marked for removal.
     */
    public static void create(Context context, Transaction tr, List<String> subpath) {
        List<String> namespaceSubpath = getNamespaceSubpath(context, subpath);
        try {
            try {
                checkBeingRemoved(tr, context.getClusterName(), subpath);
            } catch (NoSuchNamespaceException ignored) {
                // ignored
            }

            // Create the namespace
            DirectorySubspace subspace = DirectoryLayer.getDefault().create(tr, namespaceSubpath).join();

            writeMetadata(tr, subspace, subpath);

            // Create the internal leaves
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

    /**
     * Reads the metadata for a namespace from the FoundationDB directory layer.
     *
     * @param tr          the Transaction object used for operations within the FoundationDB environment
     * @param clusterName the name of the cluster containing the namespace
     * @param subpath     the list of strings representing the hierarchical namespace path
     * @return the NamespaceMetadata for the specified namespace
     * @throws NoSuchNamespaceException if the namespace does not exist
     * @throws KronotopException        if an error occurs during the operation
     */
    public static NamespaceMetadata readMetadata(Transaction tr, String clusterName, List<String> subpath) {
        List<String> namespaceSubpath = KronotopDirectory.
                kronotop().
                cluster(clusterName).
                namespaces().
                namespace(subpath).
                toList();
        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, namespaceSubpath).join();

            return readMetadata(tr, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(dottedNamespace(subpath));
            }
            throw e;
        }
    }

    /**
     * Reads the metadata for a namespace from the given directory subspace.
     *
     * @param tr       the Transaction object used for operations within the FoundationDB environment
     * @param subspace the DirectorySubspace representing the namespace
     * @return the NamespaceMetadata for the specified namespace
     * @throws KronotopException if an error occurs during the operation
     */
    public static NamespaceMetadata readMetadata(Transaction tr, DirectorySubspace subspace) {
        try {
            long id = 0;
            String name = null;
            long version = 0;
            boolean removed = false;

            for (KeyValue kv : tr.getRange(subspace.range(Tuple.from(Namespace.METADATA)))) {
                Tuple key = subspace.unpack(kv.getKey());
                byte fieldValue = (byte) key.getLong(1);
                if (fieldValue == NamespaceMetadataField.ID.getValue()) {
                    id = ByteBuffer.wrap(kv.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
                } else if (fieldValue == NamespaceMetadataField.NAME.getValue()) {
                    name = new String(kv.getValue(), StandardCharsets.UTF_8);
                } else if (fieldValue == NamespaceMetadataField.REMOVED.getValue()) {
                    removed = kv.getValue()[0] == 1;
                } else if (fieldValue == NamespaceMetadataField.VERSION.getValue()) {
                    version = ByteBuffer.wrap(kv.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
                }
            }

            return new NamespaceMetadata(id, name, version, removed);
        } catch (CompletionException e) {
            throw new KronotopException(e.getCause());
        }
    }

    /**
     * Reads the metadata for a namespace using the provided context.
     *
     * @param context   the Context object representing the operational environment
     * @param namespace the dotted namespace name (e.g., "a.b.c")
     * @return the NamespaceMetadata for the specified namespace
     * @throws NoSuchNamespaceException if the namespace does not exist
     * @throws KronotopException        if an error occurs during the operation
     */
    public static NamespaceMetadata readMetadata(Context context, String namespace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return readMetadata(tr, context.getClusterName(), splitNamespaceHierarchy(namespace));
        }
    }

    /**
     * Marks a namespace as removed by updating its metadata.
     *
     * @param tr          the Transaction object used for operations within the FoundationDB environment
     * @param clusterName the name of the cluster containing the namespace
     * @param subpath     the list of strings representing the hierarchical namespace path
     * @throws NoSuchNamespaceException if the namespace does not exist
     * @throws KronotopException        if an error occurs during the operation
     */
    public static void setRemoved(Transaction tr, String clusterName, List<String> subpath) {
        List<String> namespaceSubpath = KronotopDirectory.
                kronotop().
                cluster(clusterName).
                namespaces().
                namespace(subpath).
                toList();
        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, namespaceSubpath).join();
            byte[] removedKey = subspace.pack(Tuple.from(Namespace.METADATA, NamespaceMetadataField.REMOVED.getValue()));
            tr.set(removedKey, new byte[]{1});
            incrementVersion(tr, subspace);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchNamespaceException(dottedNamespace(subpath));
            }
            throw new KronotopException(e.getCause());
        }
    }

    /**
     * Marks a namespace as removed using the provided context.
     *
     * @param context   the Context object representing the operational environment
     * @param namespace the dotted namespace name (e.g., "a.b.c")
     * @throws NoSuchNamespaceException if the namespace does not exist
     * @throws KronotopException        if an error occurs during the operation
     */
    public static void setRemoved(Context context, String namespace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            setRemoved(tr, context.getClusterName(), splitNamespaceHierarchy(namespace));
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                if (ex.getCode() == 1020) {
                    setRemoved(context, namespace);
                    return;
                }
            }
            throw new KronotopException(e.getCause());
        }
    }

    /**
     * Atomically increments the namespace version.
     *
     * @param context   the Context object representing the operational environment
     * @param namespace the dotted namespace name (e.g., "a.b.c")
     * @throws NoSuchNamespaceException if the namespace does not exist
     * @throws KronotopException        if an error occurs during the operation
     */
    public static void incrementVersion(Context context, String namespace) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace subspace = open(tr, context.getClusterName(), namespace);
            incrementVersion(tr, subspace);
            tr.commit().join();
        }
    }

    public static void incrementVersion(Transaction tr, DirectorySubspace subspace) {
        byte[] versionKey = subspace.pack(Tuple.from(Namespace.METADATA, NamespaceMetadataField.VERSION.getValue()));
        tr.mutate(MutationType.ADD, versionKey, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(1).array());
    }

    /**
     * Stores the current namespace version in the member's subspace for tracking purposes.
     * Used during namespace removal to record the last seen version.
     *
     * @param tr             the FoundationDB transaction
     * @param memberSubspace the member's directory subspace
     * @param clusterName    the cluster name
     * @param event          the namespace removed event containing namespace ID and name
     */
    public static void setLastSeenNamespaceVersion(Transaction tr, DirectorySubspace memberSubspace, String clusterName, NamespaceRemovedEvent event) {
        byte[] key = memberSubspace.pack(Tuple.from(MemberSubspace.NAMESPACES.getValue(), event.id(), MemberSubspace.LAST_SEEN_NAMESPACE_VERSION.getValue()));
        DirectorySubspace namespaceSubspace = NamespaceUtil.open(tr, clusterName, event.namespace());
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(tr, namespaceSubspace);
        tr.set(key, ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(metadata.version()).array());
    }

    /**
     * Reads the last seen namespace version from the member's subspace.
     *
     * @param tr             the FoundationDB transaction
     * @param memberSubspace the member's directory subspace
     * @param namespaceId    the namespace ID
     * @return the last seen version number, or null if no version has been recorded
     */
    public static Long readLastSeenNamespaceVersion(ReadTransaction tr, DirectorySubspace memberSubspace, long namespaceId) {
        byte[] key = memberSubspace.pack(Tuple.from(MemberSubspace.NAMESPACES.getValue(), namespaceId, MemberSubspace.LAST_SEEN_NAMESPACE_VERSION.getValue()));
        byte[] value = tr.get(key).join();
        if (value == null) {
            return null;
        }
        return ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }
}
