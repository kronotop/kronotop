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

package com.kronotop.bucket;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.Striped;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.Histogram;
import com.kronotop.bucket.index.statistics.HistogramCodec;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.UUIDUtil;
import com.kronotop.journal.JournalName;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.PrefixUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;

/**
 * Utility class for managing and interacting with bucket metadata.
 * Provides methods for creating, opening, reading, and modifying metadata for buckets
 * within a transactional environment.
 */
public class BucketMetadataUtil {
    public static final String INDEXES_DIRECTORY = "indexes";
    public static final byte[] NULL_BYTES = new byte[]{};
    public static final byte[] POSITIVE_DELTA_ONE = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1L, little-endian
    public static final byte[] NEGATIVE_DELTA_ONE = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1}; // -1L, little-endian
    private static final Tuple VERSION_TUPLE = Tuple.from(
            BucketMetadataMagic.HEADER.getValue(),
            BucketMetadataMagic.VERSION.getValue()
    );
    private static final byte BUCKET_POINTER = 0x01;
    private static final long INDEX_STATISTICS_TTL = 1000 * 15;
    private static final Striped<Lock> locks = Striped.lazyWeakLock(271);

    private static String lockKey(String namespace, String bucket) {
        return namespace + ":" + bucket;
    }

    private static byte[] bucketPointerKey(DirectorySubspace pointerSubspace) {
        return pointerSubspace.pack(BUCKET_POINTER);
    }

    private static List<String> registryPath(String clusterName, UUID uuid) {
        return KronotopDirectory.kronotop()
                .cluster(clusterName)
                .metadata()
                .buckets()
                .registry(uuid).toList();
    }

    private static Prefix createPrefix(Context context, Transaction tr, byte[] prefixBindingKey, byte[] pointerBytes) {
        HashCode hashCode = UUIDUtil.hash(UUIDUtil.fromBytes(pointerBytes));
        Prefix prefix = new Prefix(hashCode);
        PrefixUtil.register(context, tr, prefixBindingKey, prefix);
        return prefix;
    }

    public static byte[] prefixBindingKey(DirectorySubspace subspace) {
        return subspace.pack(BucketMetadataMagic.PREFIX_BINDING_KEY.getValue());
    }

    /**
     * Resolves the UUID pointer bytes for a given volume prefix by walking the FDB binding chain
     * in reverse: prefix bytes → prefixBindingKey → pointerSubspace → pointer (UUID) bytes.
     *
     * @param context the context providing environment and services
     * @param tr      the active transaction
     * @param prefix  the 8-byte SipHash prefix identifying the bucket
     * @return the 16-byte UUID pointer bytes, or {@code null} if the prefix is not registered
     */
    public static byte[] resolvePointerBytes(Context context, Transaction tr, byte[] prefix) {
        DirectorySubspace prefixesSubspace = context.getDirectorySubspaceCache()
                .get(DirectorySubspaceCache.Key.PREFIXES);
        byte[] prefixKey = prefixesSubspace.pack(Tuple.from((Object) prefix));
        byte[] prefixBindingKey = tr.get(prefixKey).join();
        if (prefixBindingKey == null) {
            return null;
        }

        byte[] suffix = Tuple.from(BucketMetadataMagic.PREFIX_BINDING_KEY.getValue()).pack();
        byte[] rawPointerSubspacePrefix = Arrays.copyOf(prefixBindingKey, prefixBindingKey.length - suffix.length);
        Subspace pointerSubspace = new Subspace(rawPointerSubspacePrefix);
        return tr.get(pointerSubspace.pack(Tuple.from(BUCKET_POINTER))).join();
    }

    public static byte[] namespaceBindingKey(DirectorySubspace subspace) {
        return subspace.pack(BucketMetadataMagic.NAMESPACE_BINDING_KEY.getValue());
    }

    public static byte[] bucketNameKey(DirectorySubspace subspace) {
        return subspace.pack(BucketMetadataMagic.BUCKET_NAME.getValue());
    }

    private static void setInitialVersion(Context context, Transaction tr, DirectorySubspace subspace) {
        byte[] delta = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(context.now()).array();
        increaseVersion(tr, subspace, delta);
    }

    /**
     * Creates a new bucket with the given shards in its own transaction and updates the metadata cache.
     * Retries on transaction conflicts, as the competing transaction may have already created the bucket.
     *
     * @param context the context providing environment and services
     * @param session the session containing the current namespace
     * @param bucket  the bucket name
     * @param shards  the shard IDs to assign to the bucket
     * @return the newly created bucket metadata
     * @throws BucketAlreadyExistsException if the bucket already exists
     * @throws KronotopException            if a non-retriable transaction error occurs
     */
    public static BucketMetadata create(
            Context context,
            Session session,
            String bucket,
            List<Integer> shards
    ) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            BucketMetadata metadata = create(context, tr, session, bucket, shards, null);
            tr.commit().join();

            // Update the global bucket metadata cache
            String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
            context.getBucketMetadataCache().set(namespace, bucket, metadata);
            return metadata;
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                // The competing transaction already created the bucket, so just open it.
                if (ex.getCode() == 1020) {
                    return create(context, session, bucket, shards);
                }
            }
            throw new KronotopException(e);
        }
    }

    private static void checkShardsList(List<Integer> shards) {
        if (shards == null || shards.isEmpty()) {
            throw new KronotopException("Shards cannot be empty or null");
        }
    }

    private static DirectorySubspace createOrOpenPointerSubspace(
            Context context,
            Transaction tr,
            Session session,
            String bucket
    ) {
        DirectorySubspace parent = NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.BUCKET);
        return parent.createOrOpen(tr, List.of(bucket)).join();
    }

    private static DirectorySubspace openPointerSubspace(
            ReadTransaction tr,
            Context context,
            String namespace,
            String bucket
    ) {
        DirectorySubspace parent = NamespaceUtil.open(tr, context, namespace, DataStructureKind.BUCKET);
        return parent.open(tr, List.of(bucket)).join();
    }

    public static BucketMetadata create(
            Context context,
            Transaction tr,
            Session session,
            String bucket,
            List<Integer> shards,
            Collation collation
    ) {
        checkShardsList(shards);

        DirectorySubspace pointerSubspace = createOrOpenPointerSubspace(context, tr, session, bucket);
        byte[] pointerKey = bucketPointerKey(pointerSubspace);
        byte[] existing = tr.get(pointerKey).join();
        if (existing != null) {
            throw new BucketAlreadyExistsException(bucket);
        }

        UUID pointer = UUID.randomUUID();
        byte[] pointerBytes = UUIDUtil.toBytes(pointer);
        tr.set(pointerKey, pointerBytes);

        byte[] prefixBindingKey = prefixBindingKey(pointerSubspace);
        Prefix prefix = createPrefix(context, tr, prefixBindingKey, pointerBytes);

        DirectorySubspace bucketSubspace = context.getDirectoryLayer()
                .create(
                        tr,
                        registryPath(context.getClusterName(), pointer)
                ).join();

        // This is required to resolve namespace name during vacuum & reclaim
        String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
        DirectorySubspace parent = NamespaceUtil.open(tr, context, namespace);
        byte[] namespaceBindingKey = namespaceBindingKey(bucketSubspace);
        tr.set(namespaceBindingKey, parent.pack());

        tr.set(bucketNameKey(bucketSubspace), bucket.getBytes(StandardCharsets.UTF_8));

        final SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(context);
        setInitialVersion(context, tr, bucketSubspace);
        SingleFieldIndexDefinition primaryIndexDefinition = PrimaryIndex.createDefinition();
        DirectorySubspace idIndexSubspace = SingleFieldIndexUtil.create(tr, bucketSubspace, primaryIndexDefinition);
        indexes.register(primaryIndexDefinition, idIndexSubspace);
        indexes.updateStatistics(Map.of(primaryIndexDefinition.id(), IndexStatistics.empty()));
        writeShards(tr, bucketSubspace, shards);
        if (collation != null) {
            writeCollation(tr, bucketSubspace, collation);
        }
        long version = readVersion(tr, bucketSubspace);

        return new BucketMetadata(
                pointer,
                namespace,
                bucket,
                version,
                false,
                bucketSubspace,
                pointerSubspace,
                prefix,
                indexes,
                new CompoundIndexRegistry(context),
                new VectorIndexRegistry(),
                shards,
                collation,
                CacheBuilder.newBuilder().maximumSize(10_000).build()
        );
    }

    /**
     * Refreshes the index statistics for the provided bucket metadata if the time-to-live (TTL) duration has elapsed
     * since the last statistics update. The index statistics will be read from the FoundationDB and updated in the metadata.
     *
     * @param context  the {@code Context} instance providing the necessary environment and FoundationDB services
     * @param metadata the {@code BucketMetadata} instance whose index statistics need to be refreshed
     * @param ttl      the time-to-live duration in milliseconds used to determine if the statistics need to be updated
     */
    public static void refreshIndexStatistics(Context context, BucketMetadata metadata, long ttl) {
        long now = context.now();
        boolean singleFieldStale = metadata.indexes().getStatsLastRefreshedAt() <= now - ttl;
        boolean compoundStale = metadata.compoundIndexes().getStatsLastRefreshedAt() <= now - ttl;
        if (singleFieldStale || compoundStale) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<Long, IndexStatistics> indexStatistics = readIndexStatistics(tr, metadata);
                if (singleFieldStale) {
                    metadata.indexes().updateStatistics(indexStatistics);
                }
                if (compoundStale) {
                    metadata.compoundIndexes().updateStatistics(indexStatistics);
                }
            }
        }
    }

    /**
     * Loads bucket metadata directly from FDB. Reads the UUID pointer from the
     * namespace-bucket subspace, opens the registry subspace, then reads the volume prefix,
     * header, and indexes from the registry.
     *
     * @param context   the context providing environment and services
     * @param tr        the transaction for database access
     * @param namespace the namespace containing the bucket
     * @param bucket    the bucket name
     * @param force     if true, returns metadata even for buckets marked as removed
     * @return the bucket metadata
     * @throws NoSuchBucketException       if the bucket does not exist
     * @throws BucketBeingRemovedException if the bucket is marked as removed and force is false
     * @throws KronotopException           if the volume prefix is not found
     */
    private static BucketMetadata loadMetadata(
            Context context,
            ReadTransaction tr,
            DirectorySubspace bucketSubspace,
            DirectorySubspace pointerSubspace,
            UUID pointer,
            String namespace,
            String bucket,
            Prefix prefix,
            boolean force
    ) {
        BucketMetadataHeader header = BucketMetadataHeader.read(tr, bucketSubspace);
        if (header.removed()) {
            context.getBucketMetadataCache().invalidate(namespace, bucket);
            if (!force) throw new BucketBeingRemovedException(bucket);
        }

        final SingleFieldIndexRegistry indexes = new SingleFieldIndexRegistry(context);
        final CompoundIndexRegistry compoundIndexes = new CompoundIndexRegistry(context);
        final VectorIndexRegistry vectorIndexes = new VectorIndexRegistry();
        IndexUtil.list(tr, bucketSubspace).forEach(index -> {
            DirectorySubspace indexSubspace = IndexUtil.open(tr, bucketSubspace, index);
            byte[] vectorKey = indexSubspace.pack(BucketMetadataMagic.VECTOR_INDEX_DEFINITION.getValue());
            byte[] vectorValue = tr.get(vectorKey).join();
            if (vectorValue != null) {
                VectorIndexDefinition definition = VectorIndexUtil.loadIndexDefinition(tr, indexSubspace);
                vectorIndexes.register(definition, indexSubspace);
                return;
            }
            byte[] compoundKey = indexSubspace.pack(BucketMetadataMagic.COMPOUND_INDEX_DEFINITION.getValue());
            byte[] compoundValue = tr.get(compoundKey).join();
            if (compoundValue != null) {
                CompoundIndexDefinition definition = CompoundIndexUtil.loadIndexDefinition(tr, indexSubspace);
                compoundIndexes.register(definition, indexSubspace);
            } else {
                SingleFieldIndexDefinition definition = SingleFieldIndexUtil.loadIndexDefinition(tr, indexSubspace);
                indexes.register(definition, indexSubspace);
            }
        });
        indexes.updateStatistics(header.indexStatistics());
        compoundIndexes.updateStatistics(header.indexStatistics());

        BucketMetadata metadata = new BucketMetadata(
                pointer,
                namespace,
                bucket,
                header.version(),
                header.removed(),
                bucketSubspace,
                pointerSubspace,
                prefix,
                indexes,
                compoundIndexes,
                vectorIndexes,
                header.shards(),
                header.collation(),
                CacheBuilder.newBuilder().maximumSize(10_000).build()
        );
        if (!header.removed()) {
            context.getBucketMetadataCache().set(namespace, bucket, metadata);
        }
        return metadata;
    }

    private static BucketMetadata doOpen(
            Context context,
            ReadTransaction tr,
            String namespace,
            String bucket,
            boolean force
    ) {
        try {
            DirectorySubspace pointerSubspace = openPointerSubspace(tr, context, namespace, bucket);

            byte[] rawPointer = tr.get(bucketPointerKey(pointerSubspace)).join();
            if (rawPointer == null) {
                throw new NoSuchBucketException(bucket);
            }

            UUID pointer = UUIDUtil.fromBytes(rawPointer);
            DirectorySubspace bucketSubspace = context.getDirectoryLayer()
                    .open(
                            tr,
                            registryPath(context.getClusterName(), pointer)
                    ).join();

            byte[] rawPrefix = tr.get(prefixBindingKey(pointerSubspace)).join();
            if (rawPrefix == null) {
                throw new KronotopException("Volume prefix for bucket '" + bucket + "' not found.");
            }

            Prefix prefix = Prefix.fromBytes(rawPrefix);
            return loadMetadata(context, tr, bucketSubspace, pointerSubspace, pointer, namespace, bucket, prefix, force);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchBucketException(bucket);
            }
            throw e;
        }
    }

    /**
     * Opens bucket metadata by resolving from a volume prefix. Walks the binding chain in reverse
     * using {@link #resolvePointerBytes}, then reads bucket name and namespace from the registry.
     *
     * @param context     the context providing environment and services
     * @param tr          the transaction for database access
     * @param prefixBytes the 8-byte SipHash prefix identifying the bucket
     * @return the bucket metadata
     * @throws KronotopException if the prefix is not registered or the bucket name is not found
     */
    public static BucketMetadata openByPrefix(Context context, Transaction tr, byte[] prefixBytes) {
        // Slowest way to load a BucketMetadata but this is only used by Vacuum process.
        byte[] rawPointer = resolvePointerBytes(context, tr, prefixBytes);
        if (rawPointer == null) {
            throw new KronotopException("Prefix not registered");
        }

        UUID pointer = UUIDUtil.fromBytes(rawPointer);
        DirectorySubspace bucketSubspace = context.getDirectoryLayer()
                .open(tr, registryPath(context.getClusterName(), pointer)).join();

        byte[] rawBucketName = tr.get(bucketNameKey(bucketSubspace)).join();
        if (rawBucketName == null) {
            throw new KronotopException("Bucket name not found in registry");
        }
        String bucket = new String(rawBucketName, StandardCharsets.UTF_8);

        byte[] rawNamespaceBinding = tr.get(namespaceBindingKey(bucketSubspace)).join();
        if (rawNamespaceBinding == null) {
            throw new KronotopException("Namespace binding not found for bucket '" + bucket + "'");
        }
        List<String> subpath = new ArrayList<>();
        NamespaceUtil.resolveNamespacePath(tr, new Subspace(rawNamespaceBinding), subpath);
        Collections.reverse(subpath);
        String namespace = String.join(".", subpath);

        DirectorySubspace pointerSubspace = openPointerSubspace(tr, context, namespace, bucket);
        Prefix prefix = Prefix.fromBytes(prefixBytes);
        return loadMetadata(context, tr, bucketSubspace, pointerSubspace, pointer, namespace, bucket, prefix, false);
    }

    /**
     * Opens bucket metadata using the namespace from the session, using the cache when available.
     * If not cached, fetches from the database. Index statistics are refreshed based on TTL.
     *
     * @param context the context providing environment and services
     * @param tr      the transaction for database access
     * @param session the session containing the current namespace
     * @param bucket  the bucket name
     * @return the bucket metadata
     * @throws IllegalArgumentException if the namespace is not set in the session
     */
    public static BucketMetadata open(Context context, ReadTransaction tr, Session session, String bucket) {
        String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
        if (namespace == null) {
            throw new IllegalArgumentException("namespace not specified");
        }

        return open(context, tr, namespace, bucket);
    }

    /**
     * Opens bucket metadata, using the cache when available. If not cached, fetches from
     * the database. Index statistics are refreshed based on TTL.
     *
     * @param context   the context providing environment and services
     * @param tr        the transaction for database access
     * @param namespace the namespace containing the bucket
     * @param bucket    the bucket name
     * @return the bucket metadata
     */
    public static BucketMetadata open(Context context, ReadTransaction tr, String namespace, String bucket) {
        BucketMetadata metadata = context.getBucketMetadataCache().get(namespace, bucket);
        if (metadata != null) {
            refreshIndexStatistics(context, metadata, INDEX_STATISTICS_TTL);
            return metadata;
        }

        Lock lock = locks.get(lockKey(namespace, bucket));
        lock.lock();
        try {
            // Re-check the cache after acquiring a lock
            metadata = context.getBucketMetadataCache().get(namespace, bucket);
            if (metadata != null) {
                refreshIndexStatistics(context, metadata, INDEX_STATISTICS_TTL);
                return metadata;
            }
            return doOpen(context, tr, namespace, bucket, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reloads bucket metadata directly from the database, bypassing the cache check.
     * Updates the cache with the fresh data. Use when strong consistency is required.
     *
     * @param context   the context providing environment and services
     * @param tr        the transaction for database access
     * @param namespace the namespace containing the bucket
     * @param bucket    the bucket name
     * @return the bucket metadata
     */
    public static BucketMetadata reload(Context context, ReadTransaction tr, String namespace, String bucket) {
        Lock lock = locks.get(lockKey(namespace, bucket));
        lock.lock();
        try {
            return doOpen(context, tr, namespace, bucket, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Opens bucket metadata even if the bucket is marked for removal. Bypasses both the cache
     * check and the removal guard. Used for administrative operations like bucket cleanup.
     *
     * @param context   the context providing environment and services
     * @param tr        the transaction for database access
     * @param namespace the namespace containing the bucket
     * @param bucket    the bucket name
     * @return the bucket metadata
     */
    public static BucketMetadata forceOpen(Context context, ReadTransaction tr, String namespace, String bucket) {
        Lock lock = locks.get(lockKey(namespace, bucket));
        lock.lock();
        try {
            return doOpen(context, tr, namespace, bucket, true);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads the version of the bucket metadata stored in the database within the specified directory subspace.
     * The method retrieves the version as a long value stored using little-endian byte order.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param subspace the directory subspace containing the bucket metadata version
     * @return the version of the bucket metadata as a long value
     */
    public static long readVersion(ReadTransaction tr, DirectorySubspace subspace) {
        byte[] data = tr.get(subspace.pack(VERSION_TUPLE)).join();
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Increases the bucket metadata version stored in the database for a given directory subspace.
     * The version is updated by applying the specified delta value using an atomic ADD operation.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param subspace the directory subspace containing the bucket metadata version
     * @param delta    the value to add to the current metadata version
     */
    public static void increaseVersion(Transaction tr, DirectorySubspace subspace, byte[] delta) {
        tr.mutate(MutationType.ADD, subspace.pack(VERSION_TUPLE), delta);
    }

    /**
     * Publishes a bucket metadata-updated event to the cluster journal for propagation.
     *
     * <p>Creates a broadcast event containing the bucket's namespace, name, ID, and version, then publishes it
     * to the BUCKET_METADATA_EVENTS journal. Other cluster members watch this journal to detect metadata changes
     * and invalidate their local caches, ensuring eventual consistency across the cluster.
     *
     * @param tx       the transactional context containing both the transaction and system context
     * @param metadata the bucket metadata to broadcast
     */
    public static void publishBucketMetadataUpdatedEvent(TransactionalContext tx, BucketMetadata metadata) {
        BucketMetadataUpdatedEvent event = new BucketMetadataUpdatedEvent(
                metadata.namespace(),
                metadata.name(),
                metadata.uuid(),
                metadata.version()
        );
        tx.context().getJournal().getPublisher().publish(tx.tr(), JournalName.BUCKET_EVENTS, event);
    }

    /**
     * Publishes a bucket removed event to the journal for cluster-wide notification.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata containing namespace, name, uuid, and version
     */
    public static void publishBucketRemovedEvent(TransactionalContext tx, BucketMetadata metadata) {
        BucketRemovedEvent event = new BucketRemovedEvent(
                metadata.namespace(),
                metadata.name(),
                metadata.uuid(),
                metadata.version()
        );
        tx.context().getJournal().getPublisher().publish(tx.tr(), JournalName.BUCKET_EVENTS, event);
    }

    /**
     * Publishes a vector index dropped event to the journal for cluster-wide notification.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata
     * @param indexId  the dropped vector index identifier
     */
    public static void publishVectorIndexDroppedEvent(TransactionalContext tx, BucketMetadata metadata, long indexId) {
        VectorIndexDroppedEvent event = new VectorIndexDroppedEvent(
                metadata.namespace(),
                metadata.name(),
                metadata.uuid(),
                metadata.version(),
                indexId,
                metadata.uuid()
        );
        tx.context().getJournal().getPublisher().publish(tx.tr(), JournalName.BUCKET_EVENTS, event);
    }

    /**
     * Publishes an index statistics updated event to the journal for cluster-wide notification.
     * Triggers plan cache and metadata cache invalidation on all cluster members.
     *
     * @param tr       the FoundationDB transaction
     * @param context  the system context providing journal access
     * @param metadata the bucket metadata
     */
    public static void publishIndexStatisticsUpdatedEvent(Transaction tr, Context context, BucketMetadata metadata) {
        IndexStatisticsUpdatedEvent event = new IndexStatisticsUpdatedEvent(
                metadata.namespace(),
                metadata.name(),
                metadata.uuid()
        );
        context.getJournal().getPublisher().publish(tr, JournalName.BUCKET_EVENTS, event);
    }

    /**
     * Reads index statistics including cardinality and histograms for all indexes in a bucket.
     * Performs a single range scan and optimizes histogram decoding by reusing cached histograms
     * when versions match.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param metadata the bucket metadata containing the subspace and cached index statistics
     * @return an immutable map of index IDs to their statistics (cardinality and histogram)
     */
    public static Map<Long, IndexStatistics> readIndexStatistics(ReadTransaction tr, BucketMetadata metadata) {
        // Performance-critical section:
        // This loop scans index statistics directly from FoundationDB.
        // We deliberately avoid extra abstractions and object churn here.
        // Histograms are only decoded when the version changes to minimize GC pressure.
        // Refactor with caution—readability trade-offs are intentional.

        HashMap<Long, IndexStatistics> stats = new HashMap<>();
        Long currentIndexId = null;
        long cardinality = 0L;
        Histogram histogram = Histogram.create();

        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.INDEX_STATISTICS.getValue()
        );
        byte[] prefix = metadata.subspace().pack(tuple);
        KeySelector begin = KeySelector.firstGreaterThan(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        for (KeyValue entry : tr.snapshot().getRange(begin, end)) {
            Tuple unpackedKey = metadata.subspace().unpack(entry.getKey());
            long indexId = unpackedKey.getLong(2);
            if (currentIndexId == null) {
                // fresh start
                currentIndexId = indexId;
            } else if (currentIndexId != indexId) {
                // finalize the currentIndexId
                stats.put(currentIndexId, new IndexStatistics(cardinality, histogram));
                currentIndexId = indexId;
                cardinality = 0;
                histogram = Histogram.create();
            }

            // Decode index statistics
            long magic = unpackedKey.getLong(3);
            if (magic == BucketMetadataMagic.CARDINALITY.getLong()) {
                cardinality = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            } else if (magic == BucketMetadataMagic.HISTOGRAM.getLong()) {
                long version = HistogramCodec.readVersion(entry.getValue());
                IndexStatistics indexStats = metadata.indexes().getStatistics(currentIndexId);
                if (indexStats == null || indexStats.histogram().version() != version) {
                    histogram = HistogramCodec.decode(entry.getValue());
                }
            }
        }

        if (currentIndexId != null) {
            // Set the final entry
            stats.put(currentIndexId, new IndexStatistics(cardinality, histogram));
        }
        return Collections.unmodifiableMap(stats);
    }

    /**
     * Reads the index statistics for a specified index ID from the bucket metadata in the given subspace.
     *
     * @param tr       the transaction used to access the database
     * @param subspace the subspace where the bucket metadata is stored
     * @param indexId  the unique identifier of the index for which statistics are to be read
     * @return the statistics associated with the specified index ID, or a default IndexStatistics object if no statistics are found
     */
    public static IndexStatistics readIndexStatistics(ReadTransaction tr, DirectorySubspace subspace, long indexId) {
        BucketMetadataHeader header = BucketMetadataHeader.read(tr, subspace);
        IndexStatistics stats = header.indexStatistics().get(indexId);
        if (stats == null) {
            return new IndexStatistics(0, Histogram.create());
        }
        return stats;
    }

    /**
     * Checks whether the bucket has been marked as removed.
     *
     * @param tr       the transaction to use for reading
     * @param subspace the bucket's directory subspace
     * @return true if the bucket is marked as removed, false otherwise
     */
    public static boolean isRemoved(ReadTransaction tr, DirectorySubspace subspace) {
        BucketMetadataHeader header = BucketMetadataHeader.read(tr, subspace);
        return header.removed();
    }

    /**
     * Marks the bucket as removed, increments its version, and publishes an update event.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata to mark as removed
     */
    public static void setRemoved(TransactionalContext tx, BucketMetadata metadata) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.REMOVED.getValue()
        );
        byte[] key = metadata.subspace().pack(tuple);
        tx.tr().set(key, new byte[]{1});
        increaseVersion(tx.tr(), metadata.subspace(), POSITIVE_DELTA_ONE);
        publishBucketRemovedEvent(tx, metadata);
    }

    private static void writeShards(Transaction tr, DirectorySubspace subspace, List<Integer> shards) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.SHARDS.getValue()
        );
        byte[] key = subspace.pack(tuple);
        List<Integer> sorted = new ArrayList<>(shards);
        Collections.sort(sorted);
        ByteBuffer buf = ByteBuffer.allocate(sorted.size() * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int shard : sorted) {
            buf.putInt(shard);
        }
        tr.set(key, buf.array());
    }

    private static void writeCollation(Transaction tr, DirectorySubspace subspace, Collation collation) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.COLLATION.getValue()
        );
        byte[] key = subspace.pack(tuple);
        byte[] value = JSONUtil.writeValueAsBytes(collation);
        tr.set(key, value);
    }

    /**
     * Persists the shard list for a bucket, increments its version, and publishes an update event.
     *
     * @param tx       the transactional context
     * @param metadata the bucket metadata
     * @param shards   the shard IDs to assign to the bucket
     */
    public static void setShards(TransactionalContext tx, BucketMetadata metadata, List<Integer> shards) {
        checkShardsList(shards);

        if (shards.size() > 1 && !metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.ALL).isEmpty()) {
            throw new KronotopException("Vector indexes require single-shard buckets");
        }

        writeShards(tx.tr(), metadata.subspace(), shards);
        increaseVersion(tx.tr(), metadata.subspace(), POSITIVE_DELTA_ONE);
        publishBucketMetadataUpdatedEvent(tx, metadata);
    }

    /**
     * Physically removes the bucket's registry directory and pointer from FoundationDB.
     * This operation is irreversible and should only be called after the bucket has been
     * marked as removed and all cluster members have observed the removal.
     *
     * @param tx        the transactional context
     * @param namespace the namespace containing the bucket
     * @param bucket    the name of the bucket to purge
     */
    public static void purge(TransactionalContext tx, String namespace, String bucket) {
        Transaction tr = tx.tr();
        String clusterName = tx.context().getClusterName();

        DirectorySubspace parentSubspace = NamespaceUtil.open(
                tr,
                tx.context(),
                namespace,
                DataStructureKind.BUCKET
        );
        DirectorySubspace pointerSubspace = parentSubspace.open(tr, List.of(bucket)).join();

        byte[] rawPointer = tr.get(bucketPointerKey(pointerSubspace)).join();
        if (rawPointer != null) {
            UUID pointer = UUIDUtil.fromBytes(rawPointer);
            tx.context().getDirectoryLayer().remove(tr, registryPath(clusterName, pointer)).join();
        }

        parentSubspace.remove(tr, List.of(bucket)).join();
    }

    /**
     * Creates a new index directory subspace, saves the definition, and bumps the metadata version.
     *
     * @param tr             transaction
     * @param bucketSubspace bucket metadata subspace
     * @param indexName      name of the index to create
     * @param saveAction     callback to persist the index definition into the created subspace
     * @return newly created index directory subspace
     * @throws KronotopException if the index name already exists
     */
    public static DirectorySubspace createIndexSubspace(Transaction tr,
                                                        DirectorySubspace bucketSubspace,
                                                        String indexName,
                                                        Consumer<DirectorySubspace> saveAction) {
        try {
            DirectorySubspace indexSubspace = bucketSubspace.create(tr, List.of(INDEXES_DIRECTORY, indexName)).join();
            saveAction.accept(indexSubspace);
            increaseVersion(tr, bucketSubspace, POSITIVE_DELTA_ONE);
            return indexSubspace;
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new KronotopException("'" + indexName + "' has already exist");
            }
            throw e;
        }
    }
}
