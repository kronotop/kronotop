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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.KronotopException;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexRegistry;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.NamespaceUtil;
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
                BucketMetadataHeader header = readBucketMetadataHeader(tr, subspace);
                indexes.updateStatistics(header.indexStatistics());
                version = header.version();
            } else {
                // Create
                setInitialVersion(context, tr, subspace);
                prefix = createPrefix(context, tr, bucketVolumePrefixKey);
                DirectorySubspace idIndexSubspace = IndexUtil.create(tr, subspace, DefaultIndexDefinition.ID);
                indexes.register(DefaultIndexDefinition.ID, idIndexSubspace);
                indexes.updateStatistics(Map.of(DefaultIndexDefinition.ID.id(), new IndexStatistics(0)));
                version = readVersion(tr, subspace);
                tr.commit().join();
            }

            // Transaction cannot be used after this point.

            String namespace = session.attr(SessionAttributes.CURRENT_NAMESPACE).get();
            BucketMetadata metadata = new BucketMetadata(namespace, bucket, version, subspace, prefix, indexes);
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
                Map<Long, IndexStatistics> indexStatistics = readIndexStatistics(tr, metadata.subspace());
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

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            long version = readVersion(tr, metadata.subspace());
            if (version != metadata.version()) {
                // version changed, fetch the latest metadata.
                return createOrOpen_internal(context, session, bucket);
            }
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

            BucketMetadataHeader header = readBucketMetadataHeader(tr, subspace);
            indexes.updateStatistics(header.indexStatistics());
            long version = header.version();

            Prefix prefix = Prefix.fromBytes(raw);
            BucketMetadata metadata = new BucketMetadata(namespace, bucket, version, subspace, prefix, indexes);
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

        long version = readVersion(tr, metadata.subspace());
        if (version != metadata.version()) {
            // version changed, fetch the latest metadata.
            return open_internal(context, tr, namespace, bucket);
        }

        refreshIndexStatistics(context, metadata, INDEX_STATISTICS_TTL);
        return metadata;
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

    private static void extractIndexStatistics(HashMap<Long, IndexStatistics> stats, Tuple unpackedKey, KeyValue entry) {
        long cardinality = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
        long id = unpackedKey.getLong(3);
        stats.put(id, new IndexStatistics(cardinality));
    }

    /**
     * Reads index statistics from the FDB within the INDEX_STATISTICS directory subspace.
     * The method retrieves key-value entries representing index statistics, unpacks the keys,
     * and processes the corresponding values to generate a mapping between index IDs
     * and their associated statistics.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param subspace the directory subspace containing the index statistics
     * @return a map where each key represents the index ID as a {@code Long}, and the value
     * is an {@code IndexStatistics} instance containing the cardinality
     */
    public static Map<Long, IndexStatistics> readIndexStatistics(Transaction tr, DirectorySubspace subspace) {
        Tuple tuple = Tuple.from(
                BucketMetadataMagic.HEADER.getValue(),
                BucketMetadataMagic.INDEX_STATISTICS.getValue()
        );
        byte[] begin = subspace.pack(tuple);
        byte[] end = ByteArrayUtil.strinc(begin);

        HashMap<Long, IndexStatistics> stats = new HashMap<>();
        for (KeyValue entry : tr.getRange(begin, end)) {
            Tuple unpackedKey = subspace.unpack(entry.getKey());
            if (unpackedKey.getLong(1) == BucketMetadataMagic.INDEX_STATISTICS.getLong()) {
                extractIndexStatistics(stats, unpackedKey, entry);
            }
        }
        return Collections.unmodifiableMap(stats);
    }

    /**
     * Reads the bucket metadata header stored in the database within the specified directory subspace.
     * This method retrieves the version of the metadata and the index statistics associated with the bucket.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param subspace the directory subspace containing the bucket metadata
     * @return a {@code BucketMetadataHeader} instance containing the version and index statistics
     */
    public static BucketMetadataHeader readBucketMetadataHeader(Transaction tr, DirectorySubspace subspace) {
        Tuple tuple = Tuple.from(BucketMetadataMagic.HEADER.getValue());
        byte[] begin = subspace.pack(tuple);
        byte[] end = ByteArrayUtil.strinc(begin);

        long version = 0;
        HashMap<Long, IndexStatistics> stats = new HashMap<>();
        for (KeyValue entry : tr.getRange(begin, end)) {
            Tuple unpackedKey = subspace.unpack(entry.getKey());
            if (unpackedKey.getLong(1) == BucketMetadataMagic.VERSION.getLong()) {
                version = ByteBuffer.wrap(entry.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
            }
            if (unpackedKey.getLong(1) == BucketMetadataMagic.INDEX_STATISTICS.getLong()) {
                extractIndexStatistics(stats, unpackedKey, entry);
            }
        }
        return new BucketMetadataHeader(version, stats);
    }

    /**
     * Reads the index statistics for a specific index ID from the database within the specified directory subspace.
     * The method retrieves the index cardinality stored in little-endian byte order and encapsulates it in an
     * {@code IndexStatistics} instance. If no data is found, the cardinality is assumed to be zero.
     *
     * @param tr       the transaction instance used to interact with the database
     * @param subspace the directory subspace containing the index statistics
     * @param indexId  the unique identifier of the index whose statistics are to be retrieved
     * @return an {@code IndexStatistics} instance containing the cardinality of the specified index
     */
    public static IndexStatistics readIndexStatistics(Transaction tr, DirectorySubspace subspace, long indexId) {
        byte[] key = IndexUtil.getCardinalityKey(subspace, indexId);
        long cardinality = 0;
        byte[] value = tr.get(key).join();
        if (value == null) {
            return new IndexStatistics(cardinality);
        }

        cardinality = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
        return new IndexStatistics(cardinality);
    }
}
