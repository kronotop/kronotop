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

package com.kronotop.bucket;

import com.apple.foundationdb.*;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexRegistry;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.bucket.index.statistics.Histogram;
import com.kronotop.bucket.index.statistics.HistogramCodec;
import com.kronotop.internal.NamespaceUtil;
import com.kronotop.journal.JournalName;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.PrefixUtil;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CompletionException;

import static com.google.common.hash.Hashing.sipHash24;

/**
 * Utility class for managing and interacting with bucket metadata.
 * Provides methods for creating, opening, reading, and modifying metadata for buckets
 * within a transactional environment.
 */
public class BucketMetadataUtil {
    public static final String INDEXES_DIRECTORY = "indexes";
    private static final Tuple VERSION_TUPLE = Tuple.from(
            BucketMetadataMagic.HEADER.getValue(),
            BucketMetadataMagic.VERSION.getValue()
    );
    private static final long INDEX_STATISTICS_TTL = 1000 * 15;

    private static Prefix createPrefix(Context context, Transaction tr, byte[] bucketVolumePrefixKey) {
        // We need a unique sequence of bytes. UUID will provide this.
        UUID uuid = UUID.randomUUID();
        HashCode hashCode = sipHash24().hashBytes(uuid.toString().getBytes());

        Prefix prefix = new Prefix(hashCode);
        PrefixUtil.register(context, tr, bucketVolumePrefixKey, prefix);
        return prefix;
    }

    /**
     * Generates a byte array key by packing the {@code VOLUME_PREFIX} value from
     * the {@code BucketMetadataMagic} enum within the provided directory subspace.
     *
     * @param subspace the {@code DirectorySubspace} instance used to generate the packed key
     * @return a byte array representing the packed key for the {@code VOLUME_PREFIX}
     */
    public static byte[] prefixKey(DirectorySubspace subspace) {
        return subspace.pack(BucketMetadataMagic.VOLUME_PREFIX.getValue());
    }

    private static void setInitialVersion(Context context, Transaction tr, DirectorySubspace subspace) {
        byte[] delta = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(context.now()).array();
        increaseVersion(tr, subspace, delta);
    }

    private static long generateAndSetId(Transaction tr, DirectorySubspace subspace) {
        UUID uuid = UUID.randomUUID();
        long id = sipHash24().hashBytes(uuid.toString().getBytes()).asLong();
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.ID.getValue()
        );
        byte[] value = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(id).array();
        tr.set(subspace.pack(tuple), value);
        return id;
    }

    private static BucketMetadata createOrOpen_internal(Context context, Session session, String bucket) {
        /*
            The initial version is set using System.currentTimeMillis(), which may be affected by clock drift.
            However, this does not compromise correctness: when multiple cluster members attempt to create
            the same bucket concurrently, FoundationDB’s DirectoryLayer ensures only one transaction succeeds.
            All other transactions fail with a conflict (error code 1020) and retry, reading the version
            committed by the winner. This guarantees that even if system clocks are slightly skewed,
            all nodes converge on the same version value after conflict resolution.
            NTP synchronization across nodes is assumed, but strict clock precision is not required
            thanks to FoundationDB's serializable transaction model.
        */
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            DirectorySubspace dataStructureSubspace = NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.BUCKET);
            DirectorySubspace subspace = dataStructureSubspace.createOrOpen(tr, List.of(bucket)).join();

            byte[] bucketVolumePrefixKey = prefixKey(subspace);
            byte[] raw = tr.get(bucketVolumePrefixKey).join();

            long id;
            long version;
            Prefix prefix;
            final IndexRegistry indexes = new IndexRegistry(context);
            if (raw != null) {
                // Open
                prefix = Prefix.fromBytes(raw);
                // Open the indexes
                IndexUtil.list(tr, subspace).forEach(index -> {
                    DirectorySubspace indexSubspace = IndexUtil.open(tr, subspace, index);
                    IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
                    indexes.register(definition, indexSubspace);
                });
                BucketMetadataHeader header = BucketMetadataHeader.read(tr, subspace);
                indexes.updateStatistics(header.indexStatistics());
                id = header.version();
                version = header.version();
            } else {
                // Create
                setInitialVersion(context, tr, subspace);
                prefix = createPrefix(context, tr, bucketVolumePrefixKey);
                DirectorySubspace idIndexSubspace = IndexUtil.create(tr, subspace, DefaultIndexDefinition.ID);
                indexes.register(DefaultIndexDefinition.ID, idIndexSubspace);
                indexes.updateStatistics(Map.of(DefaultIndexDefinition.ID.id(), IndexStatistics.empty()));
                version = readVersion(tr, subspace);
                id = generateAndSetId(tr, subspace);
                tr.commit().join();
            }

            // Transaction cannot be used after this point.

            String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
            BucketMetadata metadata = new BucketMetadata(id, namespace, bucket, version, subspace, prefix, indexes);
            // Update the global bucket metadata cache
            context.getBucketMetadataCache().set(namespace, bucket, metadata);
            return metadata;
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // retry
                    return createOrOpen(context, session, bucket);
                }
            }
            throw new KronotopException(e);
        }
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
        if (metadata.indexes().getStatsLastRefreshedAt() <= context.now() - ttl) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                Map<Long, IndexStatistics> indexStatistics = readIndexStatistics(tr, metadata);
                metadata.indexes().updateStatistics(indexStatistics);
            }
        }
    }

    /**
     * Creates or retrieves the metadata for a specified bucket within a given context and session.
     * This method ensures the requested bucket metadata exists and is consistent with the latest version
     * stored in the database. It utilizes a cache to optimize metadata access and validates the bucket's
     * version against the currently stored metadata.
     * If the metadata does not exist or the version has changed, it fetches or creates new metadata.
     * Additionally, the index statistics for the bucket are refreshed based on a predefined time-to-live (TTL).
     *
     * @param context the {@code Context} instance providing the execution environment and FoundationDB services.
     * @param session the {@code Session} instance containing attributes such as the namespace for the bucket.
     * @param bucket  the unique name of the bucket to create or open.
     * @return a {@code BucketMetadata} instance corresponding to the specified bucket.
     * @throws IllegalArgumentException if the namespace is not specified in the session.
     */
    public static BucketMetadata createOrOpen(Context context, Session session, String bucket) {
        String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
        if (namespace == null) {
            throw new IllegalArgumentException("namespace not specified");
        }

        BucketMetadata metadata = context.getBucketMetadataCache().get(namespace, bucket);
        if (metadata == null) {
            return createOrOpen_internal(context, session, bucket);
        }

        refreshIndexStatistics(context, metadata, INDEX_STATISTICS_TTL);
        return metadata;
    }

    private static BucketMetadata open_internal(Context context, Transaction tr, String namespace, String bucket) {
        DirectorySubspace dataStructureSubspace = NamespaceUtil.open(tr, context.getClusterName(), namespace, DataStructureKind.BUCKET);
        try {
            DirectorySubspace subspace = dataStructureSubspace.open(tr, List.of(bucket)).join();
            byte[] bucketVolumePrefixKey = prefixKey(subspace);
            byte[] raw = tr.get(bucketVolumePrefixKey).join();

            if (raw == null) {
                throw new KronotopException("Volume prefix for bucket '" + bucket + "' not found.");
            }

            final IndexRegistry indexes = new IndexRegistry(context);
            // Open the indexes
            IndexUtil.list(tr, subspace).forEach(index -> {
                DirectorySubspace indexSubspace = IndexUtil.open(tr, subspace, index);
                IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
                indexes.register(definition, indexSubspace);
            });

            BucketMetadataHeader header = BucketMetadataHeader.read(tr, subspace);
            indexes.updateStatistics(header.indexStatistics());

            Prefix prefix = Prefix.fromBytes(raw);
            BucketMetadata metadata = new BucketMetadata(header.id(), namespace, bucket, header.version(), subspace, prefix, indexes);
            context.getBucketMetadataCache().set(namespace, bucket, metadata);
            return metadata;
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new NoSuchBucketException(bucket);
            }
            throw e;
        }
    }

    /**
     * Opens the metadata for a specified bucket within a given context and session.
     * This method attempts to retrieve the bucket metadata from a cache. If the metadata is not found
     * in the cache or the version has changed, it fetches the latest metadata from the database.
     * It also ensures that the index statistics are refreshed based on a predefined time-to-live (TTL).
     *
     * @param context the {@code Context} instance providing the necessary environment and services
     * @param tr      the {@code Transaction} instance used to interact with the database
     * @param session the {@code Session} instance containing namespace and bucket-related attributes
     * @param bucket  the unique name of the bucket whose metadata needs to be opened
     * @return the {@code BucketMetadata} instance corresponding to the specified bucket name
     * @throws IllegalArgumentException if the namespace is not specified in the session
     */
    public static BucketMetadata open(Context context, Transaction tr, Session session, String bucket) {
        String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
        if (namespace == null) {
            throw new IllegalArgumentException("namespace not specified");
        }

        return open_internal(context, tr, namespace, bucket);
    }

    /**
     * Opens the metadata for the specified bucket within the provided context, namespace, and transaction.
     * This method attempts to retrieve the bucket metadata from a cache. If the metadata is not found
     * or the version has changed, it fetches the latest metadata from the database. Index statistics
     * are refreshed based on a predefined time-to-live (TTL).
     *
     * @param context   the {@code Context} instance providing the necessary environment and services
     * @param tr        the {@code Transaction} instance used to interact with the database
     * @param namespace the namespace to which the bucket belongs
     * @param bucket    the unique name of the bucket whose metadata needs to be opened
     * @return a {@code BucketMetadata} instance corresponding to the specified bucket
     */
    public static BucketMetadata open(Context context, Transaction tr, String namespace, String bucket) {
        BucketMetadata metadata = context.getBucketMetadataCache().get(namespace, bucket);
        if (metadata == null) {
            return open_internal(context, tr, namespace, bucket);
        }

        refreshIndexStatistics(context, metadata, INDEX_STATISTICS_TTL);
        return metadata;
    }

    /**
     * Forces the opening of a bucket within the specified namespace.
     *
     * @param context   The context of the operation, providing necessary configurations and settings.
     * @param tr        The transaction instance used for ensuring the operation is performed atomically.
     * @param namespace The namespace within which the bucket resides.
     * @param bucket    The name of the bucket to be forcibly opened.
     * @return A BucketMetadata instance containing metadata of the forcibly opened bucket.
     */
    public static BucketMetadata forceOpen(Context context, Transaction tr, String namespace, String bucket) {
        return open_internal(context, tr, namespace, bucket);
    }

    /**
     * Reads the version of the bucket metadata stored in the database within the specified directory subspace.
     * The method retrieves the version as a long value stored using little-endian byte order.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param subspace the directory subspace containing the bucket metadata version
     * @return the version of the bucket metadata as a long value
     */
    public static long readVersion(Transaction tr, DirectorySubspace subspace) {
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
     * Publishes a bucket metadata updated event to the cluster journal for propagation.
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
                metadata.id(), metadata.version()
        );
        tx.context().getJournal().getPublisher().publish(tx.tr(), JournalName.BUCKET_EVENTS, event);
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
    public static Map<Long, IndexStatistics> readIndexStatistics(Transaction tr, BucketMetadata metadata) {
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
     * @param tr the transaction used to access the database
     * @param subspace the subspace where the bucket metadata is stored
     * @param indexId the unique identifier of the index for which statistics are to be read
     * @return the statistics associated with the specified index ID, or a default IndexStatistics object if no statistics are found
     */
    public static IndexStatistics readIndexStatistics(Transaction tr, DirectorySubspace subspace, long indexId) {
        BucketMetadataHeader header = BucketMetadataHeader.read(tr, subspace);
        IndexStatistics stats = header.indexStatistics().get(indexId);
        if (stats == null) {
            return new IndexStatistics(0, Histogram.create());
        }
        return stats;
    }
}
