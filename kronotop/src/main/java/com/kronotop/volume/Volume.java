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
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.CacheLoader;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.volume.handlers.PackedEntry;
import com.kronotop.volume.replication.SegmentLog;
import com.kronotop.volume.replication.SegmentLogValue;
import com.kronotop.volume.replication.SegmentNotFoundException;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentAnalysis;
import com.kronotop.volume.segment.SegmentAppendResult;
import com.kronotop.volume.segment.SegmentConfig;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import static com.kronotop.volume.EntryMetadata.*;
import static com.kronotop.volume.Subspaces.*;
import static com.kronotop.volume.segment.Segment.SEGMENT_NAME_SIZE;

/**
 * Volume implements a transactional key/value store based on append-only log files
 * and stores its metadata in FoundationDB.
 */
public class Volume {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private static final int SEGMENT_VACUUM_BATCH_SIZE = 100;

    private final Context context;
    private final VolumeConfig config;
    private final VolumeSubspace subspace;
    private final EntryMetadataCache entryMetadataCache;
    private final byte[] streamingSubscribersTriggerKey;

    // segmentsLock protects segments map
    private final StampedLock segmentsLock = new StampedLock();
    private final TreeMap<String, SegmentContainer> segments = new TreeMap<>();

    private final ReadWriteLock statusLock = new ReentrantReadWriteLock();
    // The "attributes" variable is used to set, get and unset runtime attributes of the Volume
    // A runtime attribute might be ShardId or ownership of the Volume.
    private final AttributeMap attributes = new DefaultAttributeMap();
    private VolumeStatus status;
    private volatile boolean isClosed;

    public Volume(Context context, VolumeConfig config) throws IOException {
        this.context = context;
        this.config = config;
        this.subspace = new VolumeSubspace(config.subspace());
        this.status = loadStatusFromMetadata();
        this.entryMetadataCache = new EntryMetadataCache(context, subspace);
        this.streamingSubscribersTriggerKey = this.config.subspace().pack(Tuple.from(STREAMING_SUBSCRIBERS_SUBSPACE));
    }

    private SegmentContainer getSegmentContainer(String segmentName) {
        long stamp = segmentsLock.readLock();
        try {
            return segments.get(segmentName);
        } finally {
            segmentsLock.unlockRead(stamp);
        }
    }

    /**
     * Loads the status of the volume from the metadata stored in the Volume's subspace.
     * The method creates a transaction to read the metadata and retrieves the volume status.
     *
     * @return the status of the volume as retrieved from the metadata
     */
    private VolumeStatus loadStatusFromMetadata() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return VolumeMetadata.load(tr, config.subspace()).getStatus();
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
        statusLock.writeLock().lock();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata.compute(tr, config.subspace(), volumeMetadata -> {
                volumeMetadata.setStatus(status);
            });
            tr.commit().join();

            // Set the status only if the commit was successful.
            this.status = status;
        } finally {
            statusLock.writeLock().unlock();
        }
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

    private void triggerStreamingSubscribers(Transaction tr) {
        tr.mutate(MutationType.ADD, streamingSubscribersTriggerKey, INCREASE_BY_ONE_DELTA);
    }

    private void flushMutatedSegments(EntryMetadata[] entryMetadataList) throws IOException {
        // Forces any updates to this channel's file to be written to the storage device that contains it.
        for (EntryMetadata entryMetadata : entryMetadataList) {
            Segment segment = getOrOpenSegmentByName(entryMetadata.segment());
            segment.flush();
        }
    }

    /**
     * Opens and initializes a new segment with the specified segment ID and position.
     * The method creates a new segment, segment log, and segment metadata,
     * then stores them in a segment container protected by the `segmentsLock`.
     *
     * @param segmentId the unique identifier for the segment to be opened
     * @param position  the initial position within the segment
     * @return the initialized Segment instance
     * @throws IOException if an I/O error occurs during segment initialization
     */
    private Segment openSegment(long segmentId, long position) throws IOException {
        // NOTE: must be protected by segmentsLock
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig, position);
        SegmentLog segmentLog = new SegmentLog(segment.getName(), config.subspace());
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.getName());
        segments.put(segment.getName(), new SegmentContainer(segment, segmentLog, segmentMetadata));
        return segment;
    }

    /**
     * Retrieves the current highest segment ID and returns the next segment ID by incrementing the highest value by one.
     * If no segments are available, returns 0.
     * <p>
     * This method is protected by the `segmentsLock` to ensure thread safety.
     *
     * @return the next segment ID. If no segments are available, returns 0.
     */
    private long getAndIncreaseSegmentId() {
        // protected by segmentsLock
        return context.getFoundationDB().run(tr -> {
            List<Long> availableSegments = VolumeMetadata.load(tr, config.subspace()).getSegments();
            if (availableSegments.isEmpty()) {
                return 0L;
            }
            return availableSegments.getLast() + 1;
        });
    }

    /**
     * Creates a new segment in the volume. This method handles the initialization
     * of segment configuration, segment creation on physical medium, updating
     * volume metadata in FoundationDB, and making the segment available for the
     * rest of the volume.
     *
     * @return the Segment instance that has been created.
     * @throws IOException if an I/O error occurs during the creation of the segment.
     */
    private Segment createSegment() throws IOException {
        // createsSegment protected by segmentsLock
        long segmentId = getAndIncreaseSegmentId();
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig, 0);

        // After this point, the Segment has been created on the physical medium.

        // Update the volume metadata on FoundationDB
        context.getFoundationDB().run(tr -> {
            VolumeMetadata.compute(tr, config.subspace(), (volumeMetadata) -> volumeMetadata.addSegment(segmentId));
            return null;
        });

        // Make it available for the rest of the Volume.
        SegmentLog segmentLog = new SegmentLog(segment.getName(), config.subspace());
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.getName());
        segments.put(segment.getName(), new SegmentContainer(segment, segmentLog, segmentMetadata));

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
            Map.Entry<String, SegmentContainer> entry = segments.lastEntry();
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

    /**
     * Retrieves a writable segment that has sufficient free space to accommodate the specified size.
     * If no existing segment has enough space, a new writable segment is created or acquired.
     *
     * @param size the minimum amount of free space (in bytes) required in the writable segment
     * @return a writable segment with at least the specified free space available
     * @throws IOException if an I/O error occurs while attempting to acquire or create a writable segment
     */
    private Segment getWritableSegment(int size) throws IOException {
        long stamp = segmentsLock.readLock();
        try {
            Map.Entry<String, SegmentContainer> entry = segments.lastEntry();
            if (entry != null) {
                Segment latest = entry.getValue().segment();
                if (size < latest.getFreeBytes()) {
                    return latest;
                }
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
                int id = EntryMetadataIdGenerator.generate(segment.getConfig().id(), result.position());
                return new EntryMetadata(segment.getName(), prefix.asBytes(), result.position(), result.length(), id);
            } catch (NotEnoughSpaceException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Trying to find a new segment with length {}", size);
                }
            }
        }
    }

    /**
     * Appends a segment log entry to the given session.
     *
     * @param session       the session object containing the current transaction.
     * @param kind          the kind of operation being logged.
     * @param entryMetadata metadata of the entry being appended to the segment.
     */
    private void appendSegmentLog(VolumeSession session, OperationKind kind, Versionstamp versionstamp, EntryMetadata entryMetadata) {
        appendSegmentLog(session.transaction(), kind, versionstamp, session.getAndIncrementUserVersion(), session.prefix().asLong(), entryMetadata);
    }

    /**
     * Appends a segment log entry to the specified transaction.
     *
     * @param tr            the transaction object to which the log entry is appended.
     * @param kind          the kind of operation being logged (e.g., APPEND, DELETE, VACUUM).
     * @param userVersion   the user-defined version associated with the operation.
     * @param entryMetadata metadata of the entry being appended, which includes segment, position, and length information.
     * @throws IllegalStateException if the segment specified in the entry metadata cannot be found.
     */
    private void appendSegmentLog(Transaction tr, OperationKind kind, Versionstamp versionstamp, int userVersion, long prefix, EntryMetadata entryMetadata) {
        SegmentContainer segmentContainer = getSegmentContainer(entryMetadata.segment());
        if (segmentContainer == null) {
            throw new IllegalStateException("Segment " + entryMetadata.segment() + " not found");
        }
        SegmentLogValue value = new SegmentLogValue(kind, prefix, entryMetadata.position(), entryMetadata.length());
        segmentContainer.log().append(tr, versionstamp, userVersion, value);
    }

    /**
     * Writes metadata entries to the specified volume session and updates the transaction accordingly.
     * Also handles segment metadata updates and triggers any associated subscribers.
     *
     * @param session the volume session containing the transaction and associated metadata.
     * @param entries an array of metadata entries to be written.
     * @return a result object containing the appended entries and the transaction's versionstamp.
     */
    private WriteMetadataResult writeMetadata(VolumeSession session, EntryMetadata[] entries) {
        AppendedEntry[] appendedEntries = new AppendedEntry[entries.length];
        Transaction tr = session.transaction();

        for (int index = 0; index < entries.length; index++) {
            EntryMetadata entryMetadata = entries[index];
            int userVersion = session.getAndIncrementUserVersion();
            byte[] encodedEntryMetadata = entryMetadata.encode().array();
            appendedEntries[index] = new AppendedEntry(index, userVersion, entryMetadata, encodedEntryMetadata);

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

            SegmentContainer segmentContainer = getSegmentContainer(entryMetadata.segment());
            if (segmentContainer == null) {
                throw new IllegalStateException("Segment " + entryMetadata.segment() + " not found");
            }
            segmentContainer.metadata().increaseCardinalityByOne(session);
            segmentContainer.metadata().increaseUsedBytes(session, entryMetadata.length());

            // Passing versionstamp as null because we don't have any key for this entry for now.
            // It will be automatically filled by FDB during the commit. It'll be the same versionstamp with entry's key.
            appendSegmentLog(tr, OperationKind.APPEND, null, userVersion, session.prefix().asLong(), entryMetadata);
        }

        triggerStreamingSubscribers(tr);
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
     * Appends multiple entries to the storage within the context of the given session.
     *
     * @param session The session object specifying the transactional context for the append operation.
     * @param entries An array of ByteBuffers containing the entries to be appended.
     * @return An AppendResult object containing the result of the append operation,
     * including metadata about the appended entries and a future for the transaction versionstamp.
     * @throws IOException If an I/O error occurs during the append operation.
     */
    public AppendResult append(@Nonnull VolumeSession session, @Nonnull ByteBuffer... entries) throws IOException {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        if (entries.length > UserVersion.MAX_VALUE) {
            throw new TooManyEntriesException();
        }

        EntryMetadata[] appendEntries = appendEntries(session.prefix(), entries);

        Thread asyncFlush = Thread.ofVirtual().start(() -> {
            // Forces any updates to this channel's file to be written to the storage device that contains it.
            try {
                flushMutatedSegments(appendEntries);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        WriteMetadataResult result = writeMetadata(session, appendEntries);

        try {
            asyncFlush.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        raiseExceptionIfVolumeReadOnly();
        return new AppendResult(result.versionstampFuture(), result.entries(), entryMetadataCache.load(session.prefix())::put);
    }

    /**
     * Finds the position of a segment based on its name. The method interacts with FDB
     * to retrieve metadata related to the segment and computes its position.
     *
     * @param name the name of the segment whose position is being determined
     * @return the position of the segment as a long value
     */
    private long findSegmentPosition(String name) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, name.getBytes()));
            byte[] end = ByteArrayUtil.strinc(begin);

            AsyncIterable<KeyValue> iterable = tr.getRange(new Range(begin, end), 1, true);
            List<KeyValue> result = iterable.asList().join();
            if (result.isEmpty()) {
                // No entries found
                return 0;
            }
            byte[] data = (byte[]) config.subspace().unpack(result.getFirst().getKey()).get(1);
            EntryMetadata last = EntryMetadata.decode(ByteBuffer.wrap(data));

            return last.position() + last.length();
        }
    }

    /**
     * Retrieves an existing segment by its name or opens a new segment if it does not already exist.
     * This method ensures thread safety by using read and write locks.
     *
     * @param name the name of the segment to retrieve or open.
     * @return the Segment instance corresponding to the specified name.
     * @throws IOException              if an I/O error occurs while opening a new segment.
     * @throws SegmentNotFoundException if the specified segment cannot be found.
     */
    private Segment getOrOpenSegmentByName(String name) throws IOException, SegmentNotFoundException {
        long stamp = segmentsLock.readLock();
        try {
            SegmentContainer segmentContainer = segments.get(name);
            if (segmentContainer != null) {
                return segmentContainer.segment();
            }
        } finally {
            segmentsLock.unlockRead(stamp);
        }

        // Try to open the segment but check it first
        long writeStamp = segmentsLock.writeLock();
        try {
            SegmentContainer segmentContainer = segments.get(name);
            if (segmentContainer != null) {
                return segmentContainer.segment();
            }
            long segmentId = Segment.extractIdFromName(name);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, config.subspace());
                boolean has = volumeMetadata.getSegments().stream().anyMatch(
                        existingSegmentId -> Objects.equals(existingSegmentId, segmentId)
                );
                if (!has) {
                    throw new SegmentNotFoundException(name);
                }

                long position = findSegmentPosition(name);
                return openSegment(segmentId, position);
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
        } catch (ExecutionException e) {
            throw new KronotopException("Failed to load entry metadata from FoundationDB", e);
        }
    }

    /**
     * Retrieves a ByteBuffer from a segment based on the provided entry metadata.
     *
     * @param prefix        the prefix used for the entry metadata cache
     * @param key           the versionstamp key to invalidate in cache if segment is not found
     * @param entryMetadata the metadata containing segment name, position, and length of entry
     * @return a ByteBuffer containing the data specified by the entry metadata
     * @throws IOException if an I/O error occurs while accessing the segment
     */
    protected ByteBuffer getByEntryMetadata(Prefix prefix, Versionstamp key, EntryMetadata entryMetadata) throws IOException {
        Segment segment;
        try {
            segment = getOrOpenSegmentByName(entryMetadata.segment());
        } catch (SegmentNotFoundException e) {
            // Invalidate the cache and try again.
            // It will load the EntryMetadata from FoundationDB.
            // Possible cause: cleanup up filled segments.
            entryMetadataCache.load(prefix).invalidate(key);
            segment = getOrOpenSegmentByName(entryMetadata.segment());
        }

        return segment.get(entryMetadata.position(), entryMetadata.length());
    }

    /**
     * Retrieves the value associated with the specified key from the given session.
     *
     * @param session the session to be used for the operation, must not be null
     * @param key     the key associated with the value to retrieve, must not be null
     * @return a ByteBuffer containing the value associated with the specified key, or null if no value is found
     * @throws IOException if an I/O error occurs during the operation
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
            metadata = EntryMetadata.decode(ByteBuffer.wrap(value));
        }
        return getByEntryMetadata(session.prefix(), key, metadata);
    }

    /**
     * Retrieves a ByteBuffer associated with the specified prefix, versionstamp, and metadata.
     *
     * @param prefix the non-null prefix identifying the entry.
     * @param key the non-null versionstamp key associated with the entry.
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
     * @param segmentName   the name of the segment from which to retrieve the ByteBuffers
     * @param segmentRanges an array of SegmentRange objects specifying the positions and lengths of the segments to retrieve
     * @return an array of ByteBuffers corresponding to the specified segment ranges
     * @throws IOException if an I/O error occurs while accessing the segment
     */
    public ByteBuffer[] getSegmentRange(String segmentName, SegmentRange[] segmentRanges) throws IOException {
        Segment segment = getOrOpenSegmentByName(segmentName);
        ByteBuffer[] entries = new ByteBuffer[segmentRanges.length];
        for (int i = 0; i < segmentRanges.length; i++) {
            SegmentRange segmentRange = segmentRanges[i];
            entries[i] = segment.get(segmentRange.position(), segmentRange.length());
        }
        return entries;
    }

    /**
     * Deletes the entries associated with the given keys within the provided session.
     *
     * @param session The session within which the delete operation is to be performed. Must not be null.
     * @param keys    The versionstamps of the entries to be deleted. Must not be null.
     * @return A DeleteResult containing the results of the deletion operation, including the count of deleted entries
     * and a callback for invalidating related cache entries.
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
            byte[] encodedEntryMetadata = tr.get(entryKey).join();
            if (encodedEntryMetadata == null) {
                // Already deleted by a previously committed transaction.
                continue;
            }
            tr.clear(entryKey);
            tr.clear(subspace.packEntryMetadataKey(encodedEntryMetadata));

            EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(encodedEntryMetadata));

            SegmentContainer segmentContainer = segments.get(entryMetadata.segment());
            segmentContainer.metadata().decreaseCardinalityByOne(session);
            segmentContainer.metadata().increaseUsedBytes(session, -1 * entryMetadata.length());

            appendSegmentLog(session, OperationKind.DELETE, key, entryMetadata);
            triggerStreamingSubscribers(tr);

            result.add(index, key);
            index++;
        }
        raiseExceptionIfVolumeReadOnly();
        return result;
    }

    /**
     * Updates the given key entries in the session.
     *
     * @param session The current session running the update transaction.
     * @param pairs   The key entries to be updated.
     * @return The result of the update operation containing updated key entries and a cache invalidator.
     * @throws IOException          If an I/O error occurs during the update.
     * @throws KeyNotFoundException If a key is not found during the update.
     */
    public UpdateResult update(@Nonnull VolumeSession session, @Nonnull KeyEntry... pairs) throws IOException, KeyNotFoundException {
        if (pairs.length == 0) {
            throw new IllegalArgumentException("Empty key pairs array");
        }

        ByteBuffer[] entries = new ByteBuffer[pairs.length];
        for (int i = 0; i < pairs.length; i++) {
            entries[i] = pairs[i].entry();
        }
        EntryMetadata[] entryMetadataList = appendEntries(session.prefix(), entries);
        flushMutatedSegments(entryMetadataList);

        Transaction tr = session.transaction();
        int index = 0;
        for (KeyEntry keyEntry : pairs) {
            Versionstamp key = keyEntry.key();
            byte[] packedKey = subspace.packEntryKey(session.prefix(), key);
            byte[] encodedPrevEntryMetadata = tr.get(packedKey).join();
            if (encodedPrevEntryMetadata == null) {
                throw new KeyNotFoundException(key);
            }

            EntryMetadata prevEntryMetadata = EntryMetadata.decode(ByteBuffer.wrap(encodedPrevEntryMetadata));
            SegmentContainer prevSegmentContainer = segments.get(prevEntryMetadata.segment());

            EntryMetadata entryMetadata = entryMetadataList[index];
            SegmentContainer segmentContainer = segments.get(entryMetadata.segment());

            if (!prevEntryMetadata.segment().equals(entryMetadata.segment())) {
                prevSegmentContainer.metadata().decreaseCardinalityByOne(session);
                segmentContainer.metadata().increaseCardinalityByOne(session);
                prevSegmentContainer.metadata().increaseUsedBytes(session, -1 * prevEntryMetadata.length());
            } else {
                segmentContainer.metadata().increaseUsedBytes(session, -1 * entryMetadata.length());
            }
            segmentContainer.metadata().increaseUsedBytes(session, entryMetadata.length());

            tr.clear(subspace.packEntryMetadataKey(encodedPrevEntryMetadata));
            tr.set(packedKey, entryMetadata.encode().array());

            appendSegmentLog(session, OperationKind.DELETE, key, prevEntryMetadata);
            appendSegmentLog(session, OperationKind.APPEND, key, entryMetadata);
            triggerStreamingSubscribers(tr);

            index++;
        }

        raiseExceptionIfVolumeReadOnly();
        return new UpdateResult(pairs, entryMetadataCache.load(session.prefix())::invalidate);
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
            for (Map.Entry<String, SegmentContainer> entry : segments.entrySet()) {
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
            for (Map.Entry<String, SegmentContainer> entry : segments.entrySet()) {
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
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session) {
        return getRange(session, ReadTransaction.ROW_LIMIT_UNLIMITED);
    }

    /**
     * Retrieves a range of KeyEntry objects based on the provided session and reverse flag.
     *
     * @param session the session used to access the data; must not be null
     * @param reverse a boolean indicating the traversal direction; if true, traverses in reverse order
     * @return an Iterable of KeyEntry objects representing the resulting range
     */
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session, boolean reverse) {
        return getRange(session, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
    }

    /**
     * Retrieves a range of KeyEntry objects based on the specified session and limit.
     *
     * @param session the active session used to retrieve the KeyEntry objects, must not be null
     * @param limit   the maximum number of KeyEntry objects to retrieve
     * @return an Iterable of KeyEntry objects within the specified range
     */
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session, int limit) {
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
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session, int limit, boolean reverse) {
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
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
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
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) {
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
    public Iterable<KeyEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit, boolean reverse) {
        return new VolumeIterable(this, session, begin, end, limit, reverse);
    }

    /**
     * Analyzes the given segment in the context of the provided transaction.
     *
     * @param tr               the transaction associated with the segment analysis
     * @param segmentContainer the container holding the segment to be analyzed
     * @return a SegmentAnalysis object containing details about the segment
     */
    private SegmentAnalysis analyzeSegment(Transaction tr, SegmentContainer segmentContainer) {
        byte[] begin = config.subspace().pack(Tuple.from(SEGMENT_CARDINALITY_SUBSPACE, segmentContainer.segment().getName()));
        byte[] end = ByteArrayUtil.strinc(begin);

        int cardinality = 0;
        long usedBytes = 0;

        for (KeyValue keyValue : tr.getRange(begin, end)) {
            Tuple tuple = config.subspace().unpack(keyValue.getKey());
            byte[] prefixBytes = tuple.getBytes(2);
            Prefix prefix = Prefix.fromBytes(prefixBytes);
            VolumeSession session = new VolumeSession(tr, prefix);

            cardinality += segmentContainer.metadata().cardinality(session);
            usedBytes += segmentContainer.metadata().usedBytes(session);
        }

        Segment segment = segmentContainer.segment();
        return new SegmentAnalysis(segment.getName(), segment.getSize(), usedBytes, segment.getFreeBytes(), cardinality);
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
        TreeMap<String, SegmentContainer> swallowCopy;
        List<SegmentAnalysis> result = new ArrayList<>();
        long stamp = segmentsLock.readLock();
        try {
            if (segments.isEmpty()) {
                return result;
            }
            swallowCopy = (TreeMap<String, SegmentContainer>) segments.clone();
        } finally {
            segmentsLock.unlockRead(stamp);
        }
        for (Map.Entry<String, SegmentContainer> entry : swallowCopy.entrySet()) {
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

    private void commitVacuumIfVolumeWritable(Transaction tr) {
        raiseExceptionIfVolumeReadOnly();
        tr.commit().join();
    }

    /**
     * Performs a vacuum operation on a specific segment to clean up stale or unnecessary data.
     * The method processes the segment in batches, checks for stale data prefixes, updates valid entries,
     * and removes stale data, applying transactions to the underlying FDB cluster as needed.
     *
     * @param vacuumContext Provides the context for the vacuum process, including the segment to vacuum
     *                      and control flags to stop the operation gracefully if required.
     * @throws IOException If an I/O error occurs during the vacuum process.
     */
    protected void vacuumSegment(VacuumContext vacuumContext) throws IOException {
        Segment segment = getOrOpenSegmentByName(vacuumContext.segment());
        byte[] begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, segment.getName().getBytes()));
        byte[] end = ByteArrayUtil.strinc(begin);

        while (!vacuumContext.stop()) {
            raiseExceptionIfVolumeReadOnly();

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                int batchSize = 0;
                Range range = new Range(begin, end);
                HashMap<Prefix, List<KeyEntry>> pairsByPrefix = new HashMap<>();
                for (KeyValue keyValue : tr.getRange(range)) {
                    if (vacuumContext.stop()) {
                        break;
                    }

                    byte[] key = keyValue.getKey();
                    if (Arrays.equals(key, begin)) {
                        // begin is inclusive.
                        continue;
                    }

                    byte[] value = keyValue.getValue();
                    byte[] trVersion = Arrays.copyOfRange(value, 0, 10);
                    int userVersion = ByteBuffer.wrap(Arrays.copyOfRange(keyValue.getValue(), 11, 13)).getShort();

                    byte[] encodedEntryMetadata = (byte[]) config.subspace().unpack(key).get(1);
                    EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(encodedEntryMetadata));

                    Prefix prefix = Prefix.fromBytes(entryMetadata.prefix());
                    Versionstamp versionstampedKey = Versionstamp.complete(trVersion, userVersion);
                    List<KeyEntry> pairs = pairsByPrefix.computeIfAbsent(prefix, (prefixAsLong) -> new ArrayList<>());

                    ByteBuffer buffer = getByEntryMetadata(prefix, versionstampedKey, entryMetadata);
                    pairs.add(new KeyEntry(versionstampedKey, buffer));
                    batchSize++;
                    if (batchSize >= SEGMENT_VACUUM_BATCH_SIZE) {
                        break;
                    }
                    begin = key;
                }

                if (pairsByPrefix.isEmpty()) {
                    // End of the segment
                    break;
                }

                List<UpdateResult> results = new ArrayList<>();
                for (Map.Entry<Prefix, List<KeyEntry>> entry : pairsByPrefix.entrySet()) {
                    UpdateResult updateResult = update(new VolumeSession(tr, entry.getKey()), entry.getValue().toArray(new KeyEntry[0]));
                    results.add(updateResult);
                }

                commitVacuumIfVolumeWritable(tr);

                for (UpdateResult updateResult : results) {
                    updateResult.complete();
                }
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException fdbException) {
                    if (fdbException.getCode() == 1007) {
                        // Transaction is too old to perform reads or be committed
                        LOGGER.trace("Vacuum on '{}', Segment: '{}' - Transaction is too old, retrying",
                                config.name(),
                                segment.getName()
                        );
                    } else if (fdbException.getCode() == 1020) {
                        // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                        LOGGER.trace("Vacuum on '{}', Segment: '{}' - Transaction not committed due to conflict with another transaction",
                                config.name(),
                                segment.getName()
                        );
                    }
                }
            } catch (IOException e) {
                // It might be critical: disk errors, etc.
                throw e;
            } catch (Exception e) {
                // Catch all exceptions and start from scratch
                begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, segment.getName().getBytes()));
                LOGGER.error("Vacuum on {}, Segment: {} has failed", config.name(), segment.getName(), e);
            }
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
     * @param name The name of the segment to be cleaned up.
     * @throws IOException If an I/O error occurs during the cleanup process.
     */
    private String cleanupStaleSegment(String name) throws IOException {
        Segment segment = getOrOpenSegmentByName(name);
        // This will retry.
        context.getFoundationDB().run(tr -> {
            VolumeMetadata.compute(tr, config.subspace(), (volumeMetadata) -> {
                volumeMetadata.removeSegment(segment.getConfig().id());
            });
            return null;
        });
        long stamp = segmentsLock.writeLock();
        segments.remove(name);
        segmentsLock.unlockWrite(stamp);
        return segment.delete();
    }

    /**
     * Cleans up stale segments based on the specified allowed garbage ratio.
     * This method analyzes segments and identifies those with zero cardinality
     * and a garbage ratio exceeding the specified threshold for cleanup.
     *
     * @return a list of file names that were successfully cleaned up
     */
    protected synchronized List<String> cleanupStaleSegments() {
        // This method should be used carefully.
        List<String> result = new ArrayList<>();
        List<SegmentAnalysis> analyses = analyze();
        analyses.sort(Comparator.comparing(SegmentAnalysis::name));
        for (int i = 0; i < analyses.size() - 1; i++) {
            // found stale segments by iteration over segments and trying to find segments with zero cardinality
            // the latest segment is writable, don't touch it.
            SegmentAnalysis analysis = analyses.get(i);
            if (analysis.cardinality() == 0) {
                try {
                    String deletedFile = cleanupStaleSegment(analysis.name());
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
        for (long id : volumeMetadata.getSegments()) {
            String segmentName = Segment.generateName(id);

            int capacity = SEGMENT_NAME_SIZE + ENTRY_PREFIX_SIZE + SUBSPACE_SEPARATOR_SIZE;
            ByteBuffer buffer = ByteBuffer.
                    allocate(capacity).
                    put(segmentName.getBytes()).
                    put(SUBSPACE_SEPARATOR).
                    put(session.prefix().asBytes()).flip();

            byte[] begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, buffer.array()));
            byte[] end = ByteArrayUtil.strinc(begin);
            session.transaction().clear(begin, end);

            SegmentMetadata metadata = new SegmentMetadata(subspace, segmentName);
            metadata.resetCardinality(session);
            metadata.resetUsedBytes(session);
        }
    }

    private void clearEntrySubspace(VolumeSession session) {
        byte[] begin = config.subspace().pack(Tuple.from(ENTRY_SUBSPACE, session.prefix().asBytes()));
        byte[] end = ByteArrayUtil.strinc(begin);
        session.transaction().clear(begin, end);
    }

    /**
     * Clears all entries with a specific prefix from the given session's transaction.
     *
     * @param session the session containing the transaction and prefix information.
     */
    public void clearPrefix(@Nonnull VolumeSession session) {
        Objects.requireNonNull(session.transaction());
        Objects.requireNonNull(session.prefix());

        raiseExceptionIfVolumeReadOnly();
        clearSegmentsByPrefix(session);
        clearEntrySubspace(session);
    }

    /**
     * Inserts the given entries into the specified segment.
     * <p>
     * Assumed that callers of this method call flush after a successful return.
     *
     * @param segmentName the name of the segment into which entries will be inserted
     * @param entries     the entries to be inserted into the segment
     * @throws IOException if an I/O error occurs while accessing the segment
     */
    public void insert(String segmentName, PackedEntry... entries) throws IOException {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        Segment segment = getOrOpenSegmentByName(segmentName);
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
