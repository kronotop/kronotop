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

package com.kronotop.volume;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.CacheLoader;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.volume.changelog.ChangeLog;
import com.kronotop.volume.handlers.PackedEntry;
import com.kronotop.volume.segment.*;
import io.github.resilience4j.retry.Retry;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.google.common.hash.Hashing.murmur3_128;
import static com.kronotop.volume.Subspaces.ENTRY_SUBSPACE;
import static com.kronotop.volume.Subspaces.SEGMENT_CARDINALITY_SUBSPACE;

/**
 * Volume implements a transactional, append-only storage engine for document bodies in Kronotop.
 *
 * <p>Volume provides a hybrid storage architecture combining:</p>
 * <ul>
 *   <li><b>FoundationDB</b>: Stores entry metadata, segment metadata, and transaction state</li>
 *   <li><b>Append-only segments</b>: Stores actual document bodies on local filesystem</li>
 *   <li><b>Segment logs</b>: Tracks all operations (APPEND, DELETE, VACUUM) for replication</li>
 * </ul>
 *
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>ACID transactions through FoundationDB integration</li>
 *   <li>Automatic segment creation when current segment fills up</li>
 *   <li>Entry metadata caching for fast lookups</li>
 *   <li>Vacuum support for reclaiming space from deleted/updated entries</li>
 *   <li>Read-only mode support for maintenance operations</li>
 *   <li>Segment-level replication logs for primary-standby setups</li>
 * </ul>
 *
 * <p><b>Storage Architecture:</b></p>
 * <p>Each Volume consists of multiple segments (append-only files). When an entry is appended:</p>
 * <ol>
 *   <li>Entry is written to the current writable segment</li>
 *   <li>Entry metadata is stored in FoundationDB with a versionstamped key</li>
 *   <li>Segment metadata (cardinality, used bytes) is updated</li>
 *   <li>Operation is logged to the segment log for replication</li>
 *   <li>Streaming subscribers are notified</li>
 * </ol>
 *
 * <p><b>Segment Management:</b></p>
 * <p>Segments are managed automatically. When a segment fills up, a new segment is created.
 * Old segments can be vacuumed to reclaim space from deleted/updated entries. Segments with
 * zero cardinality can be safely deleted during cleanup operations.</p>
 *
 * <p><b>Thread Safety:</b></p>
 * <p>Volume is thread-safe. Segment operations are protected by a {@link StampedLock}.
 * Status changes are protected by a {@link ReadWriteLock}. The entry metadata cache
 * is thread-safe and shared across all operations.</p>
 *
 * <p><b>Usage Example:</b></p>
 * <pre>{@code
 * // Append entries
 * VolumeSession session = new VolumeSession(transaction, prefix);
 * AppendResult result = volume.append(session, entry1, entry2);
 *
 * // Retrieve entries
 * ByteBuffer value = volume.get(session, versionstamp);
 *
 * // Delete entries
 * DeleteResult deleteResult = volume.delete(session, versionstamp1, versionstamp2);
 * }</pre>
 *
 * @see VolumeSession
 * @see Segment
 * @see EntryMetadata
 * @see VolumeConfig
 */
public class Volume {
    // The maximum entry size is 16 mebibytes.
    public static final int ENTRY_SIZE_LIMIT = 1 << 24;

    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);

    /**
     * Byte array representing value 1 in little-endian format for atomic increment operations.
     */
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian

    /**
     * Number of entries to process in a single transaction during segment vacuum operations.
     */
    private static final int SEGMENT_VACUUM_BATCH_SIZE = 100;

    /**
     * Application context providing access to FoundationDB and core services.
     */
    private final Context context;

    /**
     * Unique identifier for this volume, generated using SipHash24 during initialization.
     */
    private final long volumeId;

    /**
     * Configuration settings for this volume (name, data directory, segment size, subspace).
     */
    private final VolumeConfig config;

    /**
     * FoundationDB subspace containing all volume data (entries, metadata, segments).
     */
    private final VolumeSubspace subspace;

    /**
     * Cache for entry metadata, keyed by prefix to optimize reads and avoid FoundationDB lookups.
     */
    private final EntryMetadataCache entryMetadataCache;

    private final byte[] mutationTriggerKey;

    /**
     * Lock protecting the segments map from concurrent modifications.
     */
    private final StampedLock segmentsLock = new StampedLock();

    /**
     * Map of segment names to segment containers, sorted for efficient last-segment access.
     */
    private final TreeMap<Long, SegmentContainer> segments = new TreeMap<>();

    /**
     * Lock protecting volume status changes.
     */
    private final ReadWriteLock statusLock = new ReentrantReadWriteLock();

    /**
     * Runtime attributes for this volume (e.g., ShardId, ownership information).
     */
    private final AttributeMap attributes = new DefaultAttributeMap();

    private final Retry transactionWithRetry;

    private final ChangeLog changeLog;

    /**
     * Current status of the volume (READONLY, READWRITE).
     */
    private VolumeStatus status;
    /**
     * Flag indicating whether the volume has been closed.
     */
    private volatile boolean isClosed;

    /**
     * Constructs a new Volume with the given context and configuration.
     *
     * <p><b>Initialization sequence:</b></p>
     * <ol>
     *   <li>Initializes volume metadata in FoundationDB (assigns unique ID if new)</li>
     *   <li>Loads volume metadata (ID, status, segment list)</li>
     *   <li>Initializes subspace and entry metadata cache</li>
     *   <li>Opens all existing segments from metadata</li>
     * </ol>
     *
     * <p>If this is a new volume (no ID assigned), a unique ID is generated using SipHash24
     * and persisted to FoundationDB before the volume becomes operational.</p>
     *
     * @param context the application context providing access to FoundationDB and services
     * @param config  the volume configuration (name, data directory, segment size, subspace)
     * @throws IOException if an I/O error occurs while opening segments
     */
    public Volume(Context context, VolumeConfig config) throws IOException {
        this.transactionWithRetry = TransactionUtils.retry(10, Duration.ofMillis(100));

        this.context = context;
        this.config = config;

        transactionWithRetry.executeRunnable(this::initialize);

        VolumeMetadata metadata = loadVolumeMetadata();
        this.volumeId = metadata.getVolumeId();
        this.status = metadata.getStatus();
        this.subspace = new VolumeSubspace(config.subspace());
        this.changeLog = new ChangeLog(context, config.subspace());
        this.entryMetadataCache = new EntryMetadataCache(context, subspace);
        this.mutationTriggerKey = computeMutationTriggerKey();

        openSegments(metadata.getSegments());
    }

    /**
     * Generates and returns a byte array key used as a mutation trigger.
     *
     * @return A byte array representing the packed mutation trigger key.
     */
    byte[] computeMutationTriggerKey() {
        return VolumeUtil.computeMutationTriggerKey(config.subspace());
    }

    public long getId() {
        return volumeId;
    }

    /**
     * Notifies watchers by incrementing the mutation trigger counter.
     *
     * @param tr the transaction to use
     */
    private void triggerWatchers(Transaction tr) {
        tr.mutate(MutationType.ADD, mutationTriggerKey, INCREASE_BY_ONE_DELTA);
    }

    /**
     * Initializes the volume by ensuring it has a unique ID in FoundationDB metadata.
     * If the volume does not have an ID (ID is 0), generates a new random ID using
     * SipHash24 and persists it in the volume metadata.
     *
     * <p>This method handles FoundationDB transaction conflicts and retries automatically:
     * <ul>
     *   <li>Error code 1007: Transaction is too old to perform reads or be committed</li>
     *   <li>Error code 1020: Transaction not committed due to conflict with another transaction</li>
     * </ul>
     *
     * <p>The method uses an atomic boolean to track if any modifications were made
     * and only commits the transaction if changes occurred, avoiding unnecessary commits.
     *
     * @throws CompletionException if FoundationDB errors occur that cannot be retried
     */
    private void initialize() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            AtomicBoolean modified = new AtomicBoolean(false);
            VolumeMetadata.compute(tr, config.subspace(), (metadata) -> {
                if (metadata.getVolumeId() == 0) {
                    String random = UUID.randomUUID().toString();
                    long volumeId = murmur3_128().hashBytes(random.getBytes()).asLong();
                    metadata.setVolumeId(volumeId);
                    modified.set(true);
                }
            });
            if (modified.get()) {
                tr.commit().join();
            }
        }
    }

    /**
     * Opens multiple segments during volume initialization by iterating through the provided segment IDs.
     * For each segment ID, this method generates the segment name, determines its current position,
     * opens the segment, and adds it to the volume's segment collection.
     *
     * <p>This method is called during volume construction to restore all existing segments
     * that are recorded in the volume metadata. Each segment is opened with its correct
     * position determined by scanning the entry metadata in FoundationDB.
     *
     * <p>The segments are stored in a TreeMap to maintain ordered access by segment name,
     * which is important for efficient segment management operations.
     *
     * @param segmentIds a list of segment IDs to open, typically retrieved from volume metadata
     * @throws IOException if an I/O error occurs while opening any segment
     */
    private void openSegments(List<Long> segmentIds) throws IOException {
        long stamp = segmentsLock.writeLock();
        try {
            for (long segmentId : segmentIds) {
                long position = SegmentUtil.findNextPosition(context, config.subspace(), segmentId);
                SegmentContainer container = openSegment(segmentId, position);
                segments.put(segmentId, container);
            }
        } finally {
            segmentsLock.unlockWrite(stamp);
        }
    }

    /**
     * Loads the volume metadata from FoundationDB.
     *
     * @return the VolumeMetadata containing ID, status, and segment list
     */
    private VolumeMetadata loadVolumeMetadata() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return VolumeMetadata.load(tr, config.subspace());
        }
    }

    private SegmentContainer tryOptimisticSegmentLookup(long segmentId) {
        long stamp = segmentsLock.tryOptimisticRead();
        SegmentContainer container = segments.get(segmentId);
        if (container != null && segmentsLock.validate(stamp)) {
            return container;
        }
        return null;
    }

    /**
     * Retrieves the segment container for the given segment name in a thread-safe manner.
     *
     * @return the SegmentContainer for the segment, or null if not found
     */
    private SegmentContainer getSegmentContainerOrNull(long segmentId) {
        SegmentContainer container = tryOptimisticSegmentLookup(segmentId);
        if (container != null) {
            return container;
        }
        long stamp = segmentsLock.readLock();
        try {
            return segments.get(segmentId);
        } finally {
            segmentsLock.unlockRead(stamp);
        }
    }

    /**
     * Sets the specified attribute with the provided key and value.
     * The operation is synchronized to ensure thread safety when accessing the attributes map.
     *
     * @param <T>   the type of the attribute value
     * @param key   the key used to identify the attribute
     * @param value the value to set for the specified attribute key
     */
    public <T> void setAttribute(AttributeKey<T> key, T value) {
        synchronized (attributes) {
            attributes.attr(key).set(value);
        }
    }

    /**
     * Retrieves the value associated with the specified attribute key.
     *
     * @param key the attribute key for which the value is to be retrieved
     * @param <T> the type of the value associated with the attribute key
     * @return the value associated with the specified attribute key, or null if no value is assigned
     */
    public <T> T getAttribute(AttributeKey<T> key) {
        synchronized (attributes) {
            return attributes.attr(key).get();
        }
    }

    /**
     * Removes the value associated with the specified attribute key.
     * This operation sets the value of the attribute to null, effectively unsetting it.
     *
     * @param <T> the type of the value associated with the attribute key
     * @param key the key of the attribute to unset, must not be null
     */
    public <T> void unsetAttribute(AttributeKey<T> key) {
        synchronized (attributes) {
            attributes.attr(key).set(null);
        }
    }

    /**
     * Retrieves the current status of the volume.
     *
     * @return the current volume status as a VolumeStatus object
     */
    public VolumeStatus getStatus() {
        statusLock.readLock().lock();
        try {
            return status;
        } finally {
            statusLock.readLock().unlock();
        }
    }

    /**
     * Updates the status of the volume with the specified {@code status}.
     * This method ensures thread-safety during the update by acquiring a write lock.
     * The status change is persisted in the database and updated locally
     * only after a successful transaction commit.
     *
     * @param status the new status to set for the volume
     */
    public void setStatus(VolumeStatus status) {
        transactionWithRetry.executeRunnable(() -> {
            statusLock.writeLock().lock();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeMetadata.compute(tr, config.subspace(), metadata -> metadata.setStatus(status));
                tr.commit().join();

                // Set the status only if the commit was successful.
                this.status = status;
            } finally {
                statusLock.writeLock().unlock();
            }
        });
    }

    /**
     * Validates whether the volume is in a read-only state and raises an exception if it is.
     * This method checks the current status of the volume. If the status corresponds to
     * a read-only state, it throws a {@code VolumeReadOnlyException}.
     * <p>
     * Throws:
     * {@code VolumeReadOnlyException} if the volume is in a read-only state.
     */
    private void raiseExceptionIfVolumeReadOnly() {
        // Raise an exception if the volume is in READONLY status.
        if (getStatus().equals(VolumeStatus.READONLY)) {
            throw new VolumeReadOnlyException("Volume: " + config.name() + " is read-only");
        }
    }

    /**
     * Retrieves the subspace associated with this volume.
     *
     * @return a VolumeSubspace object that represents the subspace of the volume.
     */
    protected VolumeSubspace getSubspace() {
        return subspace;
    }

    /**
     * Retrieves the current configuration of the volume.
     *
     * @return a VolumeConfig object containing the configuration settings of the volume.
     */
    public VolumeConfig getConfig() {
        return config;
    }

    /**
     * Flushes all segments that have been mutated by the given entry metadata list.
     *
     * <p>This method ensures that all data written to segments is persisted to disk
     * before the transaction commits, maintaining durability guarantees.</p>
     *
     * @param entryMetadataList array of entry metadata indicating which segments were mutated
     * @throws IOException if an I/O error occurs during flush operations
     */
    private void flushMutatedSegments(EntryMetadata[] entryMetadataList) throws IOException {
        // Forces any updates to this channel's file to be written to the storage device that contains it.
        for (EntryMetadata entryMetadata : entryMetadataList) {
            Segment segment = getOrOpenSegmentById(entryMetadata.segmentId());
            segment.flush();
        }
    }

    /**
     * Opens a segment using the provided segment ID and position.
     * This method initializes the segment, its associated log, and metadata,
     * and wraps them into a {@link SegmentContainer}.
     * Note: This method must be protected by the `segmentsLock`.
     *
     * @param segmentId the unique identifier of the segment to open
     * @param position  the starting position in the segment
     * @return a {@link SegmentContainer} containing the opened segment, log, and metadata
     * @throws IOException if an I/O error occurs while opening the segment
     */
    private SegmentContainer openSegment(long segmentId, long position) throws IOException {
        // NOTE: must be protected by segmentsLock
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig, position);
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.id());
        return new SegmentContainer(segment, segmentMetadata);
    }

    private long allocateSegmentId(Transaction tr) {
        AtomicLong segmentId = new AtomicLong();
        VolumeMetadata.compute(tr, config.subspace(), (metadata) -> {
            long computedSegmentId = 0L;
            if (!metadata.getSegments().isEmpty()) {
                computedSegmentId = metadata.getSegments().getLast() + 1;
            }
            segmentId.set(computedSegmentId);
            metadata.addSegment(computedSegmentId);
        });
        return segmentId.get();
    }

    private Segment createSegment() throws IOException {
        // createSegment protected by segmentsLock
        AtomicLong segmentId = new AtomicLong();
        transactionWithRetry.executeRunnable(() -> {
            long result = TransactionUtils.executeThenCommit(context, this::allocateSegmentId);
            segmentId.set(result);
        });

        SegmentConfig segmentConfig = new SegmentConfig(segmentId.get(), config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig, 0);
        // After this point, the Segment has been created in the physical medium.

        // Make it available for the rest of the Volume.
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.id());
        segments.put(segment.id(), new SegmentContainer(segment, segmentMetadata));

        return segment;
    }

    /**
     * Retrieves the writable segment. If no segments exist or the latest segment
     * does not have enough free space to accommodate the specified size, a new segment
     * is created and returned.
     *
     * @param size the size that needs to be accommodated in the writable segment
     * @return a writable {@code Segment} instance that can accommodate the specified size
     * @throws IOException if an I/O error occurs while creating a new segment
     */
    private Segment getOrCreateWritableSegment(int size) throws IOException {
        long stamp = segmentsLock.writeLock();
        try {
            Map.Entry<Long, SegmentContainer> entry = segments.lastEntry();
            if (entry == null) {
                return createSegment();
            }

            Segment segment = entry.getValue().segment();
            if (size > segment.getFreeBytes()) {
                return createSegment();
            }
            return segment;
        } finally {
            segmentsLock.unlockWrite(stamp);
        }
    }

    private SegmentContainer lookupWritableSegment(int size) {
        // requires segmentsLock
        Map.Entry<Long, SegmentContainer> entry = segments.lastEntry();
        if (entry != null) {
            SegmentContainer latest = entry.getValue();
            if (size < latest.segment().getFreeBytes()) {
                return latest;
            }
        }
        return null;
    }

    private SegmentContainer tryOptimisticSegmentLookupForWrite(int size) {
        long stamp = segmentsLock.tryOptimisticRead();
        SegmentContainer container = lookupWritableSegment(size);
        if (container != null && segmentsLock.validate(stamp)) {
            return container;
        }
        return null;
    }

    /**
     * Retrieves a writable segment that has sufficient free space to accommodate the specified size.
     * If no existing segment has enough space, a new writable segment is created or acquired.
     *
     * @param size the minimum amount of free space (in bytes) required in the writable segment
     * @return a writable segment with at least the specified free space available
     * @throws IOException if an I/O error occurs while attempting to acquire or create a writable segment
     */
    private Segment getWritableSegment(int size) throws IOException {
        SegmentContainer container = tryOptimisticSegmentLookupForWrite(size);
        if (container != null) {
            return container.segment();
        }

        long stamp = segmentsLock.readLock();
        try {
            container = lookupWritableSegment(size);
            if (container != null) {
                return container.segment();
            }
        } finally {
            segmentsLock.unlockRead(stamp);
        }
        return getOrCreateWritableSegment(size);
    }

    /**
     * Attempts to append an entry to the writable segment that has enough free space.
     * If the current latest segment does not have enough free space, it will find
     * or create a new segment and try again.
     *
     * @param prefix the prefix associated with the entry.
     * @param entry  the byte buffer containing the entry to be appended.
     * @return an EntryMetadata object containing metadata about the appended entry.
     * @throws IOException if an I/O error occurs during the segment retrieval or creation.
     */
    private EntryMetadata tryAppend(Prefix prefix, ByteBuffer entry) throws IOException {
        int size = entry.remaining();
        while (true) {
            Segment segment = getWritableSegment(size);
            try {
                SegmentAppendResult result = segment.append(entry);
                long handle = EntryHandleGenerator.generate(volumeId, segment.getConfig().id(), result.position());
                return new EntryMetadata(segment.id(), prefix.asBytes(), result.position(), result.length(), handle);
            } catch (NotEnoughSpaceException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Trying to find a new segment with length {}", size);
                }
            }
        }
    }

    /**
     * Writes entry metadata to FoundationDB and updates segment statistics.
     *
     * <p>This method performs multiple operations atomically within the session's transaction:</p>
     * <ul>
     *   <li>Stores entry metadata with versionstamped keys (prefix + versionstamp)</li>
     *   <li>Creates reverse index (metadata bytes → versionstamp) for efficient lookups</li>
     *   <li>Updates segment cardinality (increments entry count)</li>
     *   <li>Updates segment used bytes (adds entry length)</li>
     *   <li>Appends operation to segment log for replication</li>
     *   <li>Triggers streaming subscribers</li>
     * </ul>
     *
     * <p>The versionstamp is incomplete at write time and will be filled in by FoundationDB
     * when the transaction commits, ensuring unique, monotonically increasing keys.</p>
     *
     * @param session the volume session containing the transaction and prefix
     * @param entries an array of entry metadata to be written
     * @return a result object containing the appended entries and the transaction's versionstamp future
     */
    private WriteMetadataResult writeMetadata(VolumeSession session, EntryMetadata[] entries) {
        AppendedEntry[] appendedEntries = new AppendedEntry[entries.length];
        Transaction tr = session.transaction();

        for (int index = 0; index < entries.length; index++) {
            EntryMetadata metadata = entries[index];
            int userVersion = session.getAndIncrementUserVersion();
            byte[] encodedEntryMetadata = metadata.encode();
            appendedEntries[index] = new AppendedEntry(index, userVersion, metadata, encodedEntryMetadata);

            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_KEY,
                    subspace.packEntryKeyWithVersionstamp(session.prefix(), userVersion),
                    encodedEntryMetadata
            );
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_VALUE,
                    subspace.packEntryMetadataKey(encodedEntryMetadata),
                    Tuple.from(Versionstamp.incomplete(userVersion)).packWithVersionstamp()
            );

            SegmentContainer container = getSegmentContainerOrNull(metadata.segmentId());
            if (container == null) {
                throw new IllegalStateException("Segment with id " + metadata.segmentId() + " not found");
            }
            container.metadata().increaseCardinalityByOne(session);
            container.metadata().increaseUsedBytes(session, metadata.length());

            changeLog.appendOperation(tr, metadata, session.prefix(), userVersion);
        }

        return new WriteMetadataResult(appendedEntries, tr.getVersionstamp());
    }

    /**
     * Appends multiple entries to the given prefix and returns their metadata.
     *
     * @param prefix  the prefix associated with the entries.
     * @param entries an array of ByteBuffers containing the entries to be appended.
     * @return an array of EntryMetadata objects containing metadata about the appended entries.
     * @throws IOException if an I/O error occurs during the append operation.
     */
    private EntryMetadata[] appendEntries(Prefix prefix, ByteBuffer[] entries) throws IOException {
        EntryMetadata[] appendedEntries = new EntryMetadata[entries.length];
        int index = 0;
        for (ByteBuffer entry : entries) {
            EntryMetadata entryMetadata = tryAppend(prefix, entry);
            appendedEntries[index] = entryMetadata;
            index++;
        }
        return appendedEntries;
    }

    /**
     * Appends multiple entries to the volume within the given session's transaction.
     *
     * <p>This is the primary write operation for Volume. It performs the following steps:</p>
     * <ol>
     *   <li>Validates entry count (must be > 0 and ≤ UserVersion.MAX_VALUE)</li>
     *   <li>Writes each entry to the appropriate segment (creates new segments if needed)</li>
     *   <li>Flushes mutated segments to ensure durability</li>
     *   <li>Writes entry metadata to FoundationDB with versionstamped keys</li>
     *   <li>Updates segment metadata (cardinality, used bytes)</li>
     *   <li>Appends operations to segment logs for replication</li>
     *   <li>Triggers streaming subscribers</li>
     *   <li>Validates that the volume is not read-only</li>
     * </ol>
     *
     * <p><b>Transaction Semantics:</b></p>
     * <p>This method does NOT commit the transaction. The caller is responsible for committing
     * the session's transaction. Entry metadata is written with incomplete versionstamps that
     * will be filled in by FoundationDB when the transaction commits.</p>
     *
     * <p><b>Entry Ordering:</b></p>
     * <p>Entries are assigned user versions starting from 0 within the transaction. The final
     * versionstamp key for each entry is: (transaction versionstamp, user version).</p>
     *
     * @param session the session object specifying the transactional context (must have a transaction)
     * @param entries an array of ByteBuffers containing the entries to be appended
     * @return an AppendResult containing metadata for appended entries and a cache invalidator
     * @throws IOException              if an I/O error occurs during segment operations
     * @throws IllegalArgumentException if the entries array is empty
     * @throws TooManyEntriesException  if more than UserVersion.MAX_VALUE entries are provided
     * @throws VolumeReadOnlyException  if the volume is in read-only mode
     */
    public AppendResult append(@Nonnull VolumeSession session, @Nonnull ByteBuffer... entries) throws IOException {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        if (entries.length > UserVersion.MAX_VALUE) {
            throw new TooManyEntriesException();
        }
        for (ByteBuffer entry : entries) {
            if (entry.remaining() > ENTRY_SIZE_LIMIT) {
                throw new EntrySizeExceedsLimitException();
            }
        }

        EntryMetadata[] appendEntries = appendEntries(session.prefix(), entries);

        // Forces any updates to this channel's file to be written to the storage device that contains it.
        try {
            flushMutatedSegments(appendEntries);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        WriteMetadataResult result = writeMetadata(session, appendEntries);
        triggerWatchers(session.transaction());

        raiseExceptionIfVolumeReadOnly();
        return new AppendResult(result.versionstampFuture(), result.entries(), entryMetadataCache.load(session.prefix())::put);
    }

    /**
     * Retrieves an existing segment by its name or opens a new segment if it does not already exist.
     * This method ensures thread safety by using read and write locks.
     *
     * @return the Segment instance corresponding to the specified name.
     * @throws IOException              if an I/O error occurs while opening a new segment.
     * @throws SegmentNotFoundException if the specified segment cannot be found.
     */
    private Segment getOrOpenSegmentById(long segmentId) throws IOException, SegmentNotFoundException {
        SegmentContainer segmentContainer = tryOptimisticSegmentLookup(segmentId);
        if (segmentContainer != null) {
            return segmentContainer.segment();
        }

        long stamp = segmentsLock.readLock();
        try {
            segmentContainer = segments.get(segmentId);
            if (segmentContainer != null) {
                return segmentContainer.segment();
            }
        } finally {
            segmentsLock.unlockRead(stamp);
        }

        // Try to open the segment but check it first
        long writeStamp = segmentsLock.writeLock();
        try {
            segmentContainer = segments.get(segmentId);
            if (segmentContainer != null) {
                return segmentContainer.segment();
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, config.subspace());
                boolean has = volumeMetadata.getSegments().stream().anyMatch(
                        existingSegmentId -> Objects.equals(existingSegmentId, segmentId)
                );
                if (!has) {
                    throw new SegmentNotFoundException(segmentId);
                }

                long position = SegmentUtil.findNextPosition(context, config.subspace(), segmentId);
                SegmentContainer container = openSegment(segmentId, position);
                segments.put(segmentId, container);
                return container.segment();
            }
        } finally {
            segmentsLock.unlockWrite(writeStamp);
        }
    }

    /**
     * Loads entry metadata from the cache for a given prefix and versionstamp key.
     *
     * @param prefix The prefix associated with the entry metadata.
     * @param key    The versionstamp key for which the metadata is to be retrieved.
     * @return The EntryMetadata object if it exists in the cache, otherwise null.
     * @throws KronotopException If there is an error loading the metadata from FoundationDB.
     */
    private EntryMetadata loadEntryMetadataFromCache(Prefix prefix, Versionstamp key) {
        try {
            return entryMetadataCache.load(prefix).get(key);
        } catch (CacheLoader.InvalidCacheLoadException e) {
            // The requested key doesn't exist in this Volume.
            return null;
        } catch (ExecutionException exp) {
            throw new KronotopException("Failed to load entry metadata from FoundationDB", exp.getCause());
        }
    }

    /**
     * Retrieves a ByteBuffer from a segment based on the provided entry metadata.
     *
     * @param prefix   the prefix used for the entry metadata cache
     * @param key      the versionstamp key to invalidate in cache if segment is not found
     * @param metadata the metadata containing segment name, position, and length of entry
     * @return a ByteBuffer containing the data specified by the entry metadata
     * @throws IOException if an I/O error occurs while accessing the segment
     */
    protected ByteBuffer getByEntryMetadata(Prefix prefix, Versionstamp key, EntryMetadata metadata) throws IOException {
        Segment segment;
        try {
            segment = getOrOpenSegmentById(metadata.segmentId());
        } catch (SegmentNotFoundException e) {
            // Invalidate the cache and try again.
            // It will load the EntryMetadata from FoundationDB.
            // Possible cause: cleanup up filled segments.
            entryMetadataCache.load(prefix).invalidate(key);
            segment = getOrOpenSegmentById(metadata.segmentId());
        }

        return segment.get(metadata.position(), metadata.length());
    }

    /**
     * Retrieves the entry associated with the specified versionstamp key from the volume.
     *
     * <p>This method supports two modes of operation:</p>
     * <ul>
     *   <li><b>Transactional read</b>: If session has a transaction, reads entry metadata from
     *       FoundationDB within the transaction (for consistent reads during updates/deletes)</li>
     *   <li><b>Cached read</b>: If session has no transaction, uses the entry metadata cache
     *       to avoid FoundationDB lookups (optimized for read-heavy workloads)</li>
     * </ul>
     *
     * <p>After retrieving the entry metadata, the method reads the actual entry data from
     * the appropriate segment at the position and length specified in the metadata.</p>
     *
     * @param session the session to be used for the operation, must not be null
     * @param key     the versionstamp key associated with the entry to retrieve, must not be null
     * @return a ByteBuffer containing the entry data, or null if no entry is found
     * @throws IOException if an I/O error occurs during segment read operations
     */
    public ByteBuffer get(@Nonnull VolumeSession session, @Nonnull Versionstamp key) throws IOException {
        EntryMetadata metadata;
        if (session.transaction() == null) {
            metadata = loadEntryMetadataFromCache(session.prefix(), key);
            if (metadata == null) {
                return null;
            }
        } else {
            byte[] value = session.transaction().get(subspace.packEntryKey(session.prefix(), key)).join();
            if (value == null) {
                return null;
            }
            metadata = EntryMetadata.decode(value);
        }
        return getByEntryMetadata(session.prefix(), key, metadata);
    }

    /**
     * Retrieves a ByteBuffer associated with the specified prefix, versionstamp, and metadata.
     *
     * @param prefix   the non-null prefix identifying the entry.
     * @param key      the non-null versionstamp key associated with the entry.
     * @param metadata the non-null metadata descriptor for the entry.
     * @return a ByteBuffer containing the data associated with the specified inputs.
     * @throws IOException if an I/O error occurs during the retrieval operation.
     */
    public ByteBuffer get(@Nonnull Prefix prefix, @Nonnull Versionstamp key, @Nonnull EntryMetadata metadata) throws IOException {
        // This method is tested by PlanExecutor indirectly.
        return getByEntryMetadata(prefix, key, metadata);
    }

    /**
     * Retrieves an array of ByteBuffers from the specified segment based on the given segment ranges.
     *
     * @param segmentRanges an array of SegmentRange objects specifying the positions and lengths of the segments to retrieve
     * @return an array of ByteBuffers corresponding to the specified segment ranges
     * @throws IOException if an I/O error occurs while accessing the segment
     */
    public ByteBuffer[] getSegmentRange(long segmentId, SegmentRange[] segmentRanges) throws IOException {
        Segment segment = getOrOpenSegmentById(segmentId);
        ByteBuffer[] entries = new ByteBuffer[segmentRanges.length];
        for (int i = 0; i < segmentRanges.length; i++) {
            SegmentRange segmentRange = segmentRanges[i];
            entries[i] = segment.get(segmentRange.position(), segmentRange.length());
        }
        return entries;
    }

    /**
     * Deletes the entries associated with the given versionstamp keys within the session's transaction.
     *
     * <p>This method performs the following operations for each key:</p>
     * <ol>
     *   <li>Reads the entry metadata from FoundationDB (within the transaction)</li>
     *   <li>If the entry doesn't exist (already deleted), skips to next key</li>
     *   <li>Clears the entry key and entry metadata reverse index from FoundationDB</li>
     *   <li>Updates segment metadata (decrements cardinality, subtracts used bytes)</li>
     *   <li>Appends DELETE operation to segment log for replication</li>
     *   <li>Triggers streaming subscribers</li>
     * </ol>
     *
     * <p><b>Important:</b> Deletion does NOT reclaim disk space immediately. The entry data
     * remains in the segment file until a vacuum operation is performed. Segment metadata
     * tracks used bytes to identify segments that need vacuuming.</p>
     *
     * <p><b>Transaction Semantics:</b></p>
     * <p>This method does NOT commit the transaction. The caller is responsible for committing
     * the session's transaction. The delete operation is not visible until the transaction commits.</p>
     *
     * @param session the session within which the delete operation is to be performed, must not be null
     * @param keys    the versionstamps of the entries to be deleted, must not be null
     * @return a DeleteResult containing the count of deleted entries and a cache invalidator
     * @throws IllegalArgumentException if the keys array is empty
     * @throws VolumeReadOnlyException  if the volume is in read-only mode
     */
    public DeleteResult delete(@Nonnull VolumeSession session, @Nonnull Versionstamp... keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("Empty keys array");
        }
        Transaction tr = session.transaction();

        DeleteResult result = new DeleteResult(keys.length, entryMetadataCache.load(session.prefix())::invalidate);

        int index = 0;
        for (Versionstamp key : keys) {
            byte[] entryKey = subspace.packEntryKey(session.prefix(), key);
            byte[] encodedMetadata = tr.get(entryKey).join();
            if (encodedMetadata == null) {
                // Already deleted by a previously committed transaction.
                continue;
            }
            tr.clear(entryKey);
            tr.clear(subspace.packEntryMetadataKey(encodedMetadata));

            EntryMetadata metadata = EntryMetadata.decode(encodedMetadata);

            SegmentContainer container = getSegmentContainerOrNull(metadata.segmentId());
            if (container == null) {
                throw new RetryableStateException("Segment: " + metadata.segmentId() + " could not be found");
            }
            container.metadata().decreaseCardinalityByOne(session);
            container.metadata().increaseUsedBytes(session, -1 * metadata.length());

            changeLog.deleteOperation(tr, metadata, session.prefix(), key);

            result.add(index, key);
            index++;
        }
        triggerWatchers(tr);
        raiseExceptionIfVolumeReadOnly();
        return result;
    }

    /**
     * Updates existing entries with new data within the session's transaction.
     *
     * <p>This method performs an atomic update operation for each key-entry pair:</p>
     * <ol>
     *   <li>Appends new entry data to segments (may use different segments than original)</li>
     *   <li>Flushes mutated segments to ensure durability</li>
     *   <li>For each key:
     *     <ul>
     *       <li>Verifies the key exists (throws KeyNotFoundException if not)</li>
     *       <li>Reads old entry metadata</li>
     *       <li>Updates segment metadata (cardinality, used bytes) for both old and new segments</li>
     *       <li>Replaces entry metadata in FoundationDB</li>
     *       <li>Appends DELETE log for old entry and APPEND log for new entry</li>
     *       <li>Triggers streaming subscribers</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <p><b>Segment Metadata Handling:</b></p>
     * <p>If the new entry is written to a different segment than the old entry:
     * <ul>
     *   <li>Old segment: cardinality decremented, used bytes reduced by old entry length</li>
     *   <li>New segment: cardinality incremented, used bytes increased by new entry length</li>
     * </ul>
     * If the same segment is used, only the used bytes delta is applied.</p>
     *
     * <p><b>Transaction Semantics:</b></p>
     * <p>This method does NOT commit the transaction. The caller is responsible for committing
     * the session's transaction. The update is not visible until the transaction commits.</p>
     *
     * @param session the current session running the update transaction, must not be null
     * @param pairs   the key-entry pairs to be updated (key must exist), must not be null
     * @return the result of the update operation containing updated entry metadata and a cache invalidator
     * @throws IOException              if an I/O error occurs during segment operations
     * @throws KeyNotFoundException     if any key is not found in the volume
     * @throws IllegalArgumentException if the pairs array is empty
     * @throws VolumeReadOnlyException  if the volume is in read-only mode
     */
    public UpdateResult update(@Nonnull VolumeSession session, @Nonnull KeyEntry... pairs) throws IOException, KeyNotFoundException {
        if (pairs.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        for (KeyEntry pair : pairs) {
            if (pair.entry().remaining() > ENTRY_SIZE_LIMIT) {
                throw new EntrySizeExceedsLimitException();
            }
        }

        ByteBuffer[] entries = new ByteBuffer[pairs.length];
        for (int i = 0; i < pairs.length; i++) {
            entries[i] = pairs[i].entry();
        }
        EntryMetadata[] entryMetadataList = appendEntries(session.prefix(), entries);
        flushMutatedSegments(entryMetadataList);

        UpdatedEntry[] updatedEntries = new UpdatedEntry[pairs.length];
        Transaction tr = session.transaction();
        int index = 0;
        for (KeyEntry keyEntry : pairs) {
            Versionstamp key = keyEntry.key();
            byte[] packedKey = subspace.packEntryKey(session.prefix(), key);
            byte[] encodedPrevEntryMetadata = tr.get(packedKey).join();
            if (encodedPrevEntryMetadata == null) {
                throw new KeyNotFoundException(key);
            }

            EntryMetadata prevMetadata = EntryMetadata.decode(encodedPrevEntryMetadata);
            SegmentContainer prevContainer = getSegmentContainerOrNull(prevMetadata.segmentId());

            EntryMetadata metadata = entryMetadataList[index];
            SegmentContainer container = getSegmentContainerOrNull(metadata.segmentId());
            if (container == null) {
                throw new IllegalStateException("Segment: " + metadata.segmentId() + " could not be found");
            }

            // Check the previous container here, it might be removed by the vacuum thread.
            if (prevContainer != null && prevMetadata.segmentId() != (metadata.segmentId())) {
                prevContainer.metadata().decreaseCardinalityByOne(session);
                container.metadata().increaseCardinalityByOne(session);
                prevContainer.metadata().increaseUsedBytes(session, -1 * prevMetadata.length());
            } else {
                container.metadata().increaseUsedBytes(session, -1 * metadata.length());
            }
            container.metadata().increaseUsedBytes(session, metadata.length());

            tr.clear(subspace.packEntryMetadataKey(encodedPrevEntryMetadata));
            byte[] encodedEntryMetadata = metadata.encode();
            tr.set(packedKey, encodedEntryMetadata);
            updatedEntries[index] = new UpdatedEntry(key, metadata, encodedEntryMetadata);

            changeLog.updateOperation(tr, prevMetadata, metadata, session.prefix(), key);

            index++;
        }

        triggerWatchers(tr);

        raiseExceptionIfVolumeReadOnly();
        return new UpdateResult(updatedEntries, entryMetadataCache.load(session.prefix())::put);
    }

    /**
     * Flushes all segments within the segment container. This method iterates
     * through each entry in the segments map, and attempts to invoke the
     * flush operation on the segment associated with each container entry.
     * <p>
     * If a segment fails to flush, an error is logged, but the method
     * continues to process the remaining segments.
     * <p>
     * The operation is performed inside a read lock to ensure thread-safe
     * access to the segments map.
     * <p>
     * Exceptions thrown during the flush operation are caught and logged,
     * preventing them from propagating further.
     */
    public void flush() {
        long stamp = segmentsLock.readLock();
        try {
            for (Map.Entry<Long, SegmentContainer> entry : segments.entrySet()) {
                try {
                    entry.getValue().segment().flush();
                } catch (IOException e) {
                    LOGGER.error("Failed to flush Segment: {}", entry.getKey(), e);
                }
            }
        } finally {
            segmentsLock.unlockRead(stamp);
        }
    }

    /**
     * Closes the current instance, marking it as closed and ensuring that all segments are properly closed.
     * This method will lock the segments for reading and attempt to close each segment within the collection.
     * Any IOException encountered while closing a segment will be logged as an error.
     * <p>
     * The method ensures the following steps:
     * - Marks the instance as closed.
     * - Acquires a read lock on the segments.
     * - Iterates through the segments and closes each one.
     * - Logs any IOExceptions encountered during the close process.
     */
    public void close() {
        isClosed = true;
        long stamp = segmentsLock.readLock();
        try {
            for (Map.Entry<Long, SegmentContainer> entry : segments.entrySet()) {
                Segment segment = entry.getValue().segment();
                try {
                    // This also flushes the underlying files with metadata = true.
                    segment.close();
                } catch (IOException e) {
                    LOGGER.error("Failed to close Segment: {}", entry.getKey(), e);
                }
            }
        } finally {
            segmentsLock.unlockRead(stamp);
        }
    }

    /**
     * Checks if the resource is closed.
     *
     * @return true if the resource is closed, false otherwise.
     */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Retrieves an iterable range of KeyEntry objects for the specified session.
     *
     * @param session the session for which to retrieve the KeyEntry objects
     * @return an iterable collection of KeyEntry objects within the specified session
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session) {
        return getRange(session, ReadTransaction.ROW_LIMIT_UNLIMITED);
    }

    /**
     * Retrieves a range of KeyEntry objects based on the provided session and reverse flag.
     *
     * @param session the session used to access the data; must not be null
     * @param reverse a boolean indicating the traversal direction; if true, traverses in reverse order
     * @return an Iterable of KeyEntry objects representing the resulting range
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, boolean reverse) {
        return getRange(session, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
    }

    /**
     * Retrieves a range of KeyEntry objects based on the specified session and limit.
     *
     * @param session the active session used to retrieve the KeyEntry objects, must not be null
     * @param limit   the maximum number of KeyEntry objects to retrieve
     * @return an Iterable of KeyEntry objects within the specified range
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, int limit) {
        return getRange(session, limit, false);
    }

    /**
     * Retrieves a range of KeyEntry objects based on the specified session and limit.
     *
     * @param session the session object used for retrieving the range, must not be null
     * @param limit   the maximum number of entries to retrieve
     * @param reverse if true, retrieves the entries in reverse order
     * @return an iterable collection of KeyEntry objects matching the specified range and order
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, int limit, boolean reverse) {
        return new VolumeIterable(this, session, null, null, limit, reverse);
    }

    /**
     * Retrieves a range of KeyEntry objects between the specified begin and end VersionstampedKeySelectors.
     *
     * @param session the Session object used for the operation, must not be null
     * @param begin   the starting VersionstampedKeySelector for the range
     * @param end     the ending VersionstampedKeySelector for the range
     * @return an Iterable of KeyEntry objects within the specified range
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        return getRange(session, begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
    }

    /**
     * Retrieves a range of KeyEntry objects between the specified begin and end VersionstampedKeySelectors.
     *
     * @param session the current session used for executing the operation, must not be null
     * @param begin   the starting key selector defining the beginning of the range
     * @param end     the ending key selector defining the end of the range
     * @param limit   the maximum number of key entries to include in the range
     * @return an iterable collection of KeyEntry objects that falls within the specified range
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) {
        return new VolumeIterable(this, session, begin, end, limit, false);
    }

    /**
     * Retrieves a range of KeyEntry objects between the specified begin and end VersionstampedKeySelectors.
     *
     * @param session the current database session, must not be null
     * @param begin   the starting key selector for the range
     * @param end     the ending key selector for the range
     * @param limit   the maximum number of entries to retrieve
     * @param reverse whether to retrieve the range in reverse order
     * @return an Iterable collection of KeyEntry objects within the specified range
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit, boolean reverse) {
        return new VolumeIterable(this, session, begin, end, limit, reverse);
    }

    /**
     * Analyzes the given segment in the context of the provided transaction.
     *
     * @param tr        the transaction associated with the segment analysis
     * @param container the container holding the segment to be analyzed
     * @return a SegmentAnalysis object containing details about the segment
     */
    private SegmentAnalysis analyzeSegment(Transaction tr, SegmentContainer container) {
        int cardinality = 0;
        long usedBytes = 0;

        Range range = Range.startsWith(
                config.subspace().pack(Tuple.from(SEGMENT_CARDINALITY_SUBSPACE, container.segment().id()))
        );
        for (KeyValue keyValue : tr.getRange(range)) {
            Tuple tuple = config.subspace().unpack(keyValue.getKey());
            byte[] prefixBytes = tuple.getBytes(2);
            Prefix prefix = Prefix.fromBytes(prefixBytes);
            VolumeSession session = new VolumeSession(tr, prefix);

            cardinality += container.metadata().cardinality(session);
            usedBytes += container.metadata().usedBytes(session);
        }

        Segment segment = container.segment();
        return new SegmentAnalysis(segment.id(), segment.getSize(), usedBytes, segment.getFreeBytes(), cardinality);
    }

    /**
     * Analyzes the segments associated with the provided transaction.
     * <p>
     * This method performs a read-only analysis of the current segments,
     * creating a shallow copy to avoid holding locks for an extended period.
     *
     * @param tr The transaction context used for the analysis.
     * @return A list of SegmentAnalysis objects containing the analysis results.
     */
    @SuppressWarnings("unchecked")
    public List<SegmentAnalysis> analyze(Transaction tr) {
        // Create a read-only copy of segments to prevent acquiring segmentsLock for a long time.
        // Read-only access to the segments is not an issue. A segment can only be removed by the Vacuum daemon.
        TreeMap<Long, SegmentContainer> swallowCopy;
        List<SegmentAnalysis> result = new ArrayList<>();
        long stamp = segmentsLock.readLock();
        try {
            if (segments.isEmpty()) {
                return result;
            }
            swallowCopy = (TreeMap<Long, SegmentContainer>) segments.clone();
        } finally {
            segmentsLock.unlockRead(stamp);
        }
        for (Map.Entry<Long, SegmentContainer> entry : swallowCopy.entrySet()) {
            result.add(analyzeSegment(tr, entry.getValue()));
        }
        return result;
    }

    /**
     * Analyzes the segments within a transaction context obtained from the FoundationDB database.
     *
     * @return a list of SegmentAnalysis objects resulting from the analysis.
     */
    public List<SegmentAnalysis> analyze() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return analyze(tr);
        }
    }

    private byte[] vacuumSegmentInternal(VacuumContext vacuumContext,
                                         KeySelector begin,
                                         KeySelector end) throws IOException, KeyNotFoundException {
        HashMap<Prefix, List<KeyEntry>> pairsByPrefix = new HashMap<>();
        byte[] cursor = null;

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            tr.options().setPriorityBatch();
            for (KeyValue keyValue : tr.getRange(begin, end, SEGMENT_VACUUM_BATCH_SIZE)) {
                if (vacuumContext.stop()) {
                    break;
                }

                byte[] key = keyValue.getKey();
                byte[] value = keyValue.getValue();

                Versionstamp versionstampedKey = VolumeUtil.extractVersionstampFromValue(value);

                byte[] encodedEntryMetadata = (byte[]) config.subspace().unpack(key).get(1);
                EntryMetadata entryMetadata = EntryMetadata.decode(encodedEntryMetadata);

                Prefix prefix = Prefix.fromBytes(entryMetadata.prefix());
                List<KeyEntry> pairs = pairsByPrefix.computeIfAbsent(prefix, (prefixAsLong) -> new ArrayList<>());

                ByteBuffer buffer = getByEntryMetadata(prefix, versionstampedKey, entryMetadata);
                pairs.add(new KeyEntry(versionstampedKey, buffer));
                cursor = key;
            }

            if (pairsByPrefix.isEmpty()) {
                // End of the segment
                return null;
            }

            List<UpdateResult> results = new ArrayList<>();
            for (Map.Entry<Prefix, List<KeyEntry>> entry : pairsByPrefix.entrySet()) {
                UpdateResult updateResult = update(new VolumeSession(tr, entry.getKey()), entry.getValue().toArray(new KeyEntry[0]));
                results.add(updateResult);
            }

            // commit the vacuum operation if the volume is still writable
            raiseExceptionIfVolumeReadOnly();
            tr.commit().join();

            for (UpdateResult updateResult : results) {
                updateResult.complete();
            }
        }

        return cursor;
    }

    @SuppressWarnings("ConstantConditions")
    protected void vacuumSegment(VacuumContext vacuumContext) throws IOException {
        Segment segment = getOrOpenSegmentById(vacuumContext.segmentId());
        byte[] segmentPrefix = SegmentUtil.segmentPrefix(config.subspace(), segment.id());
        KeySelector begin = KeySelector.firstGreaterOrEqual(segmentPrefix);
        KeySelector end = KeySelector.firstGreaterThan(ByteArrayUtil.strinc(segmentPrefix));

        AtomicReference<KeySelector> currentBegin = new AtomicReference<>(begin);
        while (!vacuumContext.stop()) {
            byte[] cursor = transactionWithRetry.executeSupplier(() -> {
                raiseExceptionIfVolumeReadOnly();
                try {
                    return vacuumSegmentInternal(vacuumContext, currentBegin.get(), end);
                } catch (Exception exp) {
                    LOGGER.error("Error while running Vacuum on Segment: {} of Volume: {}", segment.id(), config.name());
                    throw new RuntimeException(exp);
                }
            });
            if (cursor == null) {
                return;
            }
            currentBegin.set(KeySelector.firstGreaterThan(cursor));
        }
    }

    /**
     * Cleans up a stale segment by removing its associated metadata, files, and references.
     * <p>
     * This method first retrieves or opens the segment by its name. It destroys the segment, removes
     * its metadata in the FoundationDB database, and deletes its files from disk. The method ensures
     * thread-safety by utilizing a write lock during the cleanup process. If an error occurs during the
     * cleanup, it will log the error.
     *
     * @throws IOException If an I/O error occurs during the cleanup process.
     */
    private String cleanupStaleSegment(long segmentId) throws IOException {
        Segment segment = getOrOpenSegmentById(segmentId);
        // This will retry.
        transactionWithRetry.executeRunnable(() -> {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeMetadata.compute(tr, config.subspace(), (metadata) ->
                        metadata.removeSegment(segment.getConfig().id()));
                tr.commit().join();
            }
        });
        long stamp = segmentsLock.writeLock();
        segments.remove(segmentId);
        segmentsLock.unlockWrite(stamp);
        return segment.delete();
    }

    /**
     * Cleans up stale segments that have zero cardinality (no live entries).
     *
     * <p>This method is typically called after vacuum operations to reclaim disk space
     * from segments whose entries have all been moved to other segments. The cleanup process:</p>
     * <ol>
     *   <li>Analyzes all segments to identify their cardinality</li>
     *   <li>Sorts segments by name to process them in order</li>
     *   <li>Skips the last segment (always writable, never stale)</li>
     *   <li>For each segment with zero cardinality:
     *     <ul>
     *       <li>Removes the segment from volume metadata in FoundationDB</li>
     *       <li>Removes the segment from the in-memory segments map</li>
     *       <li>Deletes the segment file from disk</li>
     *     </ul>
     *   </li>
     * </ol>
     *
     * <p><b>Safety:</b></p>
     * <p>This method is synchronized to prevent concurrent cleanup operations. It's safe to
     * delete segments with zero cardinality because vacuum has already moved all live entries
     * to other segments.</p>
     *
     * <p><b>Error Handling:</b></p>
     * <p>If cleanup fails for a segment, an error is logged but the operation continues with
     * remaining segments. Failed cleanups may leave orphan segment files that require manual cleanup.</p>
     *
     * @return a list of file paths for segments that were successfully deleted
     */
    protected synchronized List<String> cleanupStaleSegments() {
        // This method should be used carefully.
        List<String> result = new ArrayList<>();
        List<SegmentAnalysis> analyses = analyze();
        analyses.sort(Comparator.comparing(SegmentAnalysis::segmentId));
        for (int i = 0; i < analyses.size() - 1; i++) {
            // found stale segments by iteration over segments and trying to find segments with zero cardinality
            // the latest segment is writable, don't touch it.
            SegmentAnalysis analysis = analyses.get(i);
            if (analysis.cardinality() == 0) {
                try {
                    String deletedFile = cleanupStaleSegment(analysis.segmentId());
                    result.add(deletedFile);
                } catch (Exception e) {
                    LOGGER.error("Volume '{}' may has orphan segments", config.name(), e);
                }
            }
        }
        return result;
    }

    /**
     * Clears all segment entries in the database that match the specified prefix within the given volume session.
     * This method effectively removes segment data and resets metadata associated with the cleared segments.
     *
     * @param session the VolumeSession object containing the transaction context and prefix used to identify segments.
     */
    private void clearSegmentsByPrefix(VolumeSession session) {
        assert session.transaction() != null;
        VolumeMetadata volumeMetadata = VolumeMetadata.load(session.transaction(), config.subspace());
        for (long segmentId : volumeMetadata.getSegments()) {

            byte[] begin = SegmentUtil.prefixOfVolumePrefix(config.subspace(), segmentId, session.prefix());
            Range range = Range.startsWith(begin);
            session.transaction().clear(range);

            SegmentMetadata metadata = new SegmentMetadata(subspace, segmentId);
            metadata.resetCardinality(session);
            metadata.resetUsedBytes(session);
        }
    }

    private void clearEntrySubspace(VolumeSession session) {
        byte[] prefix = config.subspace().pack(Tuple.from(ENTRY_SUBSPACE, session.prefix().asBytes()));
        Range range = Range.startsWith(prefix);
        session.transaction().clear(range);
    }

    /**
     * Clears all entries with a specific prefix from the volume within the session's transaction.
     *
     * <p>This operation removes all entries associated with the session's prefix by:</p>
     * <ul>
     *   <li>Clearing all entry metadata for this prefix across all segments</li>
     *   <li>Clearing all entry keys in the entry subspace for this prefix</li>
     *   <li>Resetting segment cardinality and used bytes for this prefix to zero</li>
     * </ul>
     *
     * <p><b>Important:</b> Like delete operations, clearPrefix does NOT immediately reclaim
     * disk space. The entry data remains in segment files until a vacuum operation is performed.</p>
     *
     * <p><b>Use Case:</b></p>
     * <p>This operation is typically used to delete all documents in a bucket or collection.
     * It's more efficient than deleting entries individually when removing large numbers of entries.</p>
     *
     * <p><b>Transaction Semantics:</b></p>
     * <p>This method does NOT commit the transaction. The caller is responsible for committing
     * the session's transaction. The clear operation is not visible until the transaction commits.</p>
     *
     * @param session the session containing the transaction and prefix information, must not be null
     * @throws NullPointerException    if session, transaction, or prefix is null
     * @throws VolumeReadOnlyException if the volume is in read-only mode
     */
    public void clearPrefix(@Nonnull VolumeSession session) {
        Objects.requireNonNull(session.transaction());
        Objects.requireNonNull(session.prefix());

        raiseExceptionIfVolumeReadOnly();
        clearSegmentsByPrefix(session);
        clearEntrySubspace(session);
    }

    /**
     * Inserts pre-packed entries into a specific segment at exact positions (used for replication).
     *
     * <p>This method is designed for replication scenarios where entries need to be inserted
     * at specific positions in specific segments to maintain identical layout across primary
     * and standby instances.</p>
     *
     * <p><b>Important Differences from append():</b></p>
     * <ul>
     *   <li>Does NOT update entry metadata in FoundationDB (metadata is replicated separately)</li>
     *   <li>Does NOT update segment metadata (replicated separately)</li>
     *   <li>Does NOT append to segment logs (already logged on primary)</li>
     *   <li>Inserts at exact positions specified in PackedEntry (not appended)</li>
     *   <li>Does NOT automatically flush (caller must call flush())</li>
     * </ul>
     *
     * <p><b>Replication Flow:</b></p>
     * <ol>
     *   <li>Primary logs operation to segment log</li>
     *   <li>Standby reads segment log</li>
     *   <li>Standby calls insert() to replicate entry data at exact position</li>
     *   <li>Standby replicates FoundationDB metadata separately</li>
     * </ol>
     *
     * <p><b>Caller Responsibilities:</b></p>
     * <p>The caller MUST call {@link #flush()} after a successful return to ensure durability.</p>
     *
     * @param entries the packed entries with exact positions to be inserted
     * @throws IOException              if an I/O error occurs while accessing the segment
     * @throws IllegalArgumentException if the entries array is empty
     * @throws KronotopException        if NotEnoughSpaceException occurs (should never happen in replication)
     * @throws VolumeReadOnlyException  if the volume is in read-only mode
     */
    public void insert(long segmentId, PackedEntry... entries) throws IOException {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        for (PackedEntry entry : entries) {
            if (entry.data().length > ENTRY_SIZE_LIMIT) {
                throw new EntrySizeExceedsLimitException();
            }
        }
        Segment segment = getOrOpenSegmentById(segmentId);
        for (PackedEntry entry : entries) {
            try {
                raiseExceptionIfVolumeReadOnly();
                segment.insert(ByteBuffer.wrap(entry.data()), entry.position());
                // Assumed that callers of this method call flush after a successful return.
            } catch (NotEnoughSpaceException e) {
                // This should never happen.
                throw new KronotopException(e);
            }
        }
    }

    /**
     * Invalidates the entry metadata cache for a specific key within a given prefix.
     *
     * @param prefix The prefix associated with the cache to be invalidated.
     * @param key    The specific key within the given prefix whose metadata cache should be invalidated.
     */
    public void invalidateEntryMetadataCacheEntry(Prefix prefix, Versionstamp key) {
        entryMetadataCache.load(prefix).invalidate(key);
    }

    @Override
    public String toString() {
        return String.format("Volume [%s]", config.name());
    }

    record WriteMetadataResult(AppendedEntry[] entries, CompletableFuture<byte[]> versionstampFuture) {
    }
}
