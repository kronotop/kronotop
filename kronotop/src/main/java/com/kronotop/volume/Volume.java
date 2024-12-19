/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.common.KronotopException;
import com.kronotop.volume.handlers.PackedEntry;
import com.kronotop.volume.replication.SegmentLog;
import com.kronotop.volume.replication.SegmentLogValue;
import com.kronotop.volume.replication.SegmentNotFoundException;
import com.kronotop.volume.segment.Segment;
import com.kronotop.volume.segment.SegmentAnalysis;
import com.kronotop.volume.segment.SegmentAppendResult;
import com.kronotop.volume.segment.SegmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final ReadWriteLock segmentsLock = new ReentrantReadWriteLock();
    private final TreeMap<String, SegmentContainer> segments = new TreeMap<>();

    private volatile boolean isClosed;

    public Volume(Context context, VolumeConfig config) throws IOException {
        this.context = context;
        this.config = config;
        this.subspace = new VolumeSubspace(config.subspace());
        this.entryMetadataCache = new EntryMetadataCache(context, subspace);
        this.streamingSubscribersTriggerKey = this.config.subspace().pack(Tuple.from(STREAMING_SUBSCRIBERS_SUBSPACE));
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
            segment.flush(false);
        }
    }

    private boolean hasSegment(Transaction tr, long segmentId) {
        VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, config.subspace());
        return volumeMetadata.getSegments().stream().anyMatch(existingSegmentId -> Objects.equals(existingSegmentId, segmentId));
    }

    /**
     * Opens a segment based on the given segment ID. This method initializes the segment configuration,
     * segment log, and segment metadata, and stores them in the segments map.
     *
     * @param segmentId the ID of the segment to be opened.
     * @return the Segment instance that has been created and opened.
     * @throws IOException if an I/O error occurs while creating or opening the segment.
     */
    private Segment openSegment(long segmentId) throws IOException {
        // NOTE: should be protected by segmentsLock
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig);
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
        Segment segment = new Segment(segmentConfig);

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
     * Retrieves the latest segment if there is enough free space; otherwise, creates a new segment.
     *
     * @param size the size in bytes required in the segment.
     * @return the Segment instance that has enough free bytes or a newly created Segment.
     * @throws IOException if an I/O error occurs during the segment retrieval or creation.
     */
    private Segment getOrCreateLatestSegment(int size) throws IOException {
        segmentsLock.writeLock().lock();
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
            segmentsLock.writeLock().unlock();
        }
    }

    /**
     * Retrieves the latest segment if there is enough free space; otherwise, creates a new segment.
     *
     * @param size the size in bytes required in the segment.
     * @return the Segment instance that has enough free bytes or a newly created Segment.
     * @throws IOException if an I/O error occurs during the segment retrieval or creation.
     */
    private Segment getLatestSegment(int size) throws IOException {
        segmentsLock.readLock().lock();
        try {
            Map.Entry<String, SegmentContainer> entry = segments.lastEntry();
            if (entry != null) {
                Segment latest = entry.getValue().segment();
                if (size < latest.getFreeBytes()) {
                    return latest;
                }
            }
        } finally {
            segmentsLock.readLock().unlock();
        }
        return getOrCreateLatestSegment(size);
    }

    /**
     * Attempts to append an entry to the latest segment that has enough free space.
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
            Segment segment = getLatestSegment(size);
            try {
                SegmentAppendResult result = segment.append(entry);
                return new EntryMetadata(segment.getName(), prefix.asBytes(), result.position(), result.length());
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
    private void appendSegmentLog(Session session, OperationKind kind, Versionstamp versionstamp, EntryMetadata entryMetadata) {
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
        segmentsLock.readLock().lock();
        try {
            SegmentContainer segmentContainer = segments.get(entryMetadata.segment());
            if (segmentContainer == null) {
                throw new IllegalStateException("Segment " + entryMetadata.segment() + " not found");
            }
            SegmentLogValue value = new SegmentLogValue(kind, prefix, entryMetadata.position(), entryMetadata.length());
            segmentContainer.log().append(tr, versionstamp, userVersion, value);
        } finally {
            segmentsLock.readLock().unlock();
        }
    }

    /**
     * Writes an array of entry metadata to the current session and updates the transaction with the provided metadata.
     * Each metadata entry will be encoded and set with a versionstamp key and value.
     * Furthermore, it updates the segment metadata and triggers streaming subscribers as necessary.
     *
     * @param session           The session object containing the current transaction and user version.
     * @param entryMetadataList An array of entry metadata to be written.
     * @return A CompletableFuture containing the byte array of the transaction versionstamp.
     */
    private CompletableFuture<byte[]> writeMetadata(Session session, EntryMetadata[] entryMetadataList) {
        Transaction tr = session.transaction();
        for (EntryMetadata entryMetadata : entryMetadataList) {
            int userVersion = session.getAndIncrementUserVersion();
            byte[] encodedEntryMetadata = entryMetadata.encode().array();

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

            SegmentContainer segmentContainer = segments.get(entryMetadata.segment());
            segmentContainer.metadata().increaseCardinalityByOne(session);
            segmentContainer.metadata().increaseUsedBytes(session, entryMetadata.length());

            // Passing versionstamp as null because we don't have any key for this entry for now.
            // It will be automatically filled by FDB during the commit. It'll be the same versionstamp with entry's key.
            appendSegmentLog(tr, OperationKind.APPEND, null, userVersion, session.prefix().asLong(), entryMetadata);
            triggerStreamingSubscribers(tr);
        }
        return tr.getVersionstamp();
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
        EntryMetadata[] entryMetadataList = new EntryMetadata[entries.length];
        int index = 0;
        for (ByteBuffer entry : entries) {
            EntryMetadata entryMetadata = tryAppend(prefix, entry);
            entryMetadataList[index] = entryMetadata;
            index++;
        }
        return entryMetadataList;
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
    public AppendResult append(@Nonnull Session session, @Nonnull ByteBuffer... entries) throws IOException {
        if (entries.length > UserVersion.MAX_VALUE) {
            throw new TooManyEntriesException();
        }

        EntryMetadata[] entryMetadataList = appendEntries(session.prefix(), entries);

        // Forces any updates to this channel's file to be written to the storage device that contains it.
        flushMutatedSegments(entryMetadataList);

        CompletableFuture<byte[]> future = writeMetadata(session, entryMetadataList);
        return new AppendResult(future, entryMetadataList, entryMetadataCache.load(session.prefix())::put);
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
        segmentsLock.readLock().lock();
        try {
            SegmentContainer segmentContainer = segments.get(name);
            if (segmentContainer != null) {
                return segmentContainer.segment();
            }
        } finally {
            segmentsLock.readLock().unlock();
        }

        // Try to open the segment but check it first
        segmentsLock.writeLock().lock();
        try {
            SegmentContainer segmentContainer = segments.get(name);
            if (segmentContainer != null) {
                return segmentContainer.segment();
            }
            long segmentId = Segment.extractIdFromName(name);
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (!hasSegment(tr, segmentId)) {
                    throw new SegmentNotFoundException(name);
                }
                return openSegment(segmentId);
            }
        } finally {
            segmentsLock.writeLock().unlock();
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
    public ByteBuffer get(@Nonnull Session session, @Nonnull Versionstamp key) throws IOException {
        EntryMetadata entryMetadata;
        if (session.transaction() == null) {
            entryMetadata = loadEntryMetadataFromCache(session.prefix(), key);
            if (entryMetadata == null) {
                return null;
            }
        } else {
            byte[] value = session.transaction().get(subspace.packEntryKey(session.prefix(), key)).join();
            if (value == null) {
                return null;
            }
            entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(value));
        }
        return getByEntryMetadata(session.prefix(), key, entryMetadata);
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
    public DeleteResult delete(@Nonnull Session session, @Nonnull Versionstamp... keys) {
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
    public UpdateResult update(@Nonnull Session session, @Nonnull KeyEntry... pairs) throws IOException, KeyNotFoundException {
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
        return new UpdateResult(pairs, entryMetadataCache.load(session.prefix())::invalidate);
    }

    /**
     * Flushes all segments in the current container.
     *
     * @param metaData a boolean flag indicating whether metadata should be flushed as well
     */
    public void flush(boolean metaData) {
        segmentsLock.readLock().lock();
        try {
            for (Map.Entry<String, SegmentContainer> entry : segments.entrySet()) {
                try {
                    entry.getValue().segment().flush(metaData);
                } catch (IOException e) {
                    LOGGER.error("Failed to flush Segment: {}", entry.getKey(), e);
                }
            }
        } finally {
            segmentsLock.readLock().unlock();
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
        segmentsLock.readLock().lock();
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
            segmentsLock.readLock().unlock();
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
    public Iterable<KeyEntry> getRange(@Nonnull Session session) {
        return new VolumeIterable(this, session, null, null);
    }

    /**
     * Retrieves a range of KeyEntry objects between the specified begin and end VersionstampedKeySelectors.
     *
     * @param session the Session object used for the operation, must not be null
     * @param begin   the starting VersionstampedKeySelector for the range
     * @param end     the ending VersionstampedKeySelector for the range
     * @return an Iterable of KeyEntry objects within the specified range
     */
    public Iterable<KeyEntry> getRange(@Nonnull Session session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        return new VolumeIterable(this, session, begin, end);
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
            Session session = new Session(tr, prefix);

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
        segmentsLock.readLock().lock();
        try {
            if (segments.isEmpty()) {
                return result;
            }
            swallowCopy = (TreeMap<String, SegmentContainer>) segments.clone();
        } finally {
            segmentsLock.readLock().unlock();
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

    /**
     * This method vacuums a specified segment by reading and updating key-value pairs within a specified range.
     * It processes the entries in batches to remove stale or outdated entries, and handles retries in case of
     * failures caused by transaction issues.
     *
     * @param name        the name of the segment to be vacuumed.
     * @param readVersion the read version to be used for the transaction.
     * @throws IOException if an I/O error occurs.
     */
    protected void vacuumSegment(String name, long readVersion) throws IOException {
        Segment segment = getOrOpenSegmentByName(name);
        byte[] begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, segment.getName().getBytes()));
        byte[] end = ByteArrayUtil.strinc(begin);

        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                tr.setReadVersion(readVersion);

                int batchSize = 0;
                Range range = new Range(begin, end);
                HashMap<Prefix, List<KeyEntry>> pairsByPrefix = new HashMap<>();
                for (KeyValue keyValue : tr.getRange(range)) {
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

                    Versionstamp versionstampedKey = Versionstamp.complete(trVersion, userVersion);
                    Prefix prefix = Prefix.fromBytes(entryMetadata.prefix());

                    List<KeyEntry> pairs = pairsByPrefix.computeIfAbsent(prefix, (prefixAsLong) -> new ArrayList<>());
                    ByteBuffer buffer = getByEntryMetadata(Prefix.fromBytes(entryMetadata.prefix()), versionstampedKey, entryMetadata);
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
                    UpdateResult updateResult = update(new Session(tr, entry.getKey()), entry.getValue().toArray(new KeyEntry[0]));
                    results.add(updateResult);
                }

                tr.commit().join();
                for (UpdateResult updateResult : results) {
                    updateResult.complete();
                }

                break;
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException fdbException) {
                    if (fdbException.getCode() == 1007) {
                        // Transaction is too old to perform reads or be committed
                        LOGGER.trace("Transaction is too old, retrying");
                    }
                }
            } catch (IOException e) {
                // It might be critical: disk errors, etc.
                throw e;
            } catch (Exception e) {
                // Catch all exceptions and start from scratch
                begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, segment.getName().getBytes()));
                LOGGER.error("Vacuum on Segment: {} has failed", segment.getName(), e);
            }
        }
    }

    private void clearSegmentsByPrefix(Session session) {
        segmentsLock.readLock().lock();
        try {
            for (Map.Entry<String, SegmentContainer> entry : segments.entrySet()) {
                String segmentName = entry.getKey();

                int capacity = SEGMENT_NAME_SIZE + ENTRY_PREFIX_SIZE + SUBSPACE_SEPARATOR_SIZE;
                ByteBuffer buffer = ByteBuffer.
                        allocate(capacity).
                        put(segmentName.getBytes()).
                        put(SUBSPACE_SEPARATOR).
                        put(session.prefix().asBytes()).flip();

                byte[] begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_SUBSPACE, buffer.array()));
                byte[] end = ByteArrayUtil.strinc(begin);
                session.transaction().clear(begin, end);

                SegmentContainer segmentContainer = entry.getValue();
                segmentContainer.metadata().resetCardinality(session);
                segmentContainer.metadata().resetUsedBytes(session);
            }
        } finally {
            segmentsLock.readLock().unlock();
        }
    }

    private void clearEntrySubspace(Session session) {
        byte[] begin = config.subspace().pack(Tuple.from(ENTRY_SUBSPACE, session.prefix().asBytes()));
        byte[] end = ByteArrayUtil.strinc(begin);
        session.transaction().clear(begin, end);
    }

    /**
     * Clears all entries with a specific prefix from the given session's transaction.
     *
     * @param session the session containing the transaction and prefix information.
     */
    public void clearPrefix(@Nonnull Session session) {
        Objects.requireNonNull(session.transaction());
        Objects.requireNonNull(session.prefix());

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
        Segment segment = getOrOpenSegmentByName(segmentName);
        for (PackedEntry entry : entries) {
            try {
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
     * @param key The specific key within the given prefix whose metadata cache should be invalidated.
     */
    public void invalidateEntryMetadataCacheEntry(Prefix prefix, Versionstamp key) {
        entryMetadataCache.load(prefix).invalidate(key);
    }
}
