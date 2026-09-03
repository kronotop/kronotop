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

package com.kronotop.volume;

import com.apple.foundationdb.*;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.internal.ExecutorServiceUtil;
import com.kronotop.internal.KrExecutors;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.transaction.TransactionUtil;
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
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

import static com.kronotop.volume.Subspaces.ENTRY_SUBSPACE;
import static com.kronotop.volume.Subspaces.SEGMENT_POSITION_SUBSPACE;

public class Volume {
    // The maximum entry size is 16 mebibytes.
    public static final int ENTRY_SIZE_LIMIT = 1 << 24;

    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);

    /**
     * Little-endian encoding of value 1, used for atomic increments.
     */
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian


    /**
     * Application context that gives access to FoundationDB and core services.
     */
    private final Context context;

    /**
     * Unique volume identifier, generated with SipHash24 during initialization.
     */
    private final long volumeId;

    /**
     * Volume configuration: name, data directory, segment size and subspace.
     */
    private final VolumeConfig config;

    /**
     * FoundationDB subspace that holds all volume data: entries, metadata and segments.
     */
    private final VolumeSubspace subspace;

    /**
     * Entry metadata cache, keyed by prefix. It avoids FoundationDB lookups on reads.
     */
    private final EntryMetadataCache entryMetadataCache;

    private final byte[] mutationTriggerKey;

    /**
     * Lock that protects volume status changes.
     */
    private final ReadWriteLock statusLock = new ReentrantReadWriteLock();

    /**
     * Runtime attributes of this volume, such as shard id and ownership information.
     */
    private final AttributeMap attributes = new DefaultAttributeMap();

    private final Retry transactionWithRetry;

    private final ChangeLog changeLog;
    private final VolumeStats stats = new VolumeStats();

    private final ReadWriteLock segmentLock = new ReentrantReadWriteLock();
    private final AtomicReference<WritableSegmentContainer> writableSegment = new AtomicReference<>();
    private final Map<Long, ReadableSegmentContainer> readableSegments = new ConcurrentHashMap<>();

    private final StampedLock replicaSegmentLock = new StampedLock();
    private final Map<Long, WritableSegment> replicaSegments = new ConcurrentHashMap<>();

    private final ExecutorService vacuumExecutor;
    private final ReentrantLock vacuumLock = new ReentrantLock();
    private VacuumWatchDog vacuumWatchDog;

    /**
     * Current status of the volume: READONLY or READWRITE.
     */
    private VolumeStatus status;
    /**
     * True when the volume has been closed.
     */
    private volatile boolean closed;

    public Volume(Context context, VolumeConfig config) throws IOException {
        this.transactionWithRetry = TransactionUtil.retry(10, Duration.ofMillis(100));

        this.context = context;
        this.config = config;
        this.subspace = new VolumeSubspace(config.subspace());
        this.changeLog = new ChangeLog(context, config.subspace());
        this.entryMetadataCache = new EntryMetadataCache(context, subspace);
        this.mutationTriggerKey = computeMutationTriggerKey();

        ThreadFactory vacuumThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("kr.volume-vacuum-%d")
                .build();
        this.vacuumExecutor = KrExecutors.newBoundedExecutor(1, 1, TimeUnit.MINUTES, vacuumThreadFactory);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeMetadata metadata = VolumeMetadataUtil.createOrOpen(tr, subspace);
            this.volumeId = metadata.id();
            this.status = metadata.status();

            tr.commit().join();

            long writableSegmentId = metadata.segmentIds().getLast();
            openWritableSegment(writableSegmentId);
            openReadableSegments(metadata.segmentIds());
        }
    }

    /**
     * Computes the packed mutation trigger key of this volume.
     *
     * @return the packed mutation trigger key
     */
    byte[] computeMutationTriggerKey() {
        return VolumeUtil.computeMutationTriggerKey(config.subspace());
    }

    public long getId() {
        return volumeId;
    }

    long getWritableSegmentId() {
        WritableSegmentContainer writable = writableSegment.get();
        return writable != null ? writable.segment().id() : -1;
    }

    public VolumeStats getStats() {
        return stats;
    }

    /**
     * Notifies watchers by incrementing the mutation trigger counter.
     *
     * @param tr the transaction to use
     */
    private void triggerWatchers(Transaction tr) {
        tr.mutate(MutationType.ADD, mutationTriggerKey, INCREASE_BY_ONE_DELTA);
    }

    private long computeNextSegmentId() {
        // protected by segmentLock
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
            if (segmentIds.isEmpty()) {
                return 0L;
            }
            return segmentIds.getLast() + 1;
        }
    }

    private void openReadableSegment(long segmentId) throws IOException {
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        ReadableSegment segment = new MappedSegment(segmentConfig);
        SegmentMetadata metadata = new SegmentMetadata(subspace, segment.id());
        readableSegments.put(segment.id(), new ReadableSegmentContainer(segment, metadata));
    }

    private ReadableSegment getReadableSegment(long segmentId) {
        ReadableSegmentContainer container = readableSegments.get(segmentId);
        if (container != null) {
            return container.segment();
        }
        throw new SegmentNotFoundException(segmentId);
    }

    private void openWritableSegment(long writableSegmentId) throws IOException {
        long position = SegmentSubspaceUtil.findNextPosition(context, config.subspace(), writableSegmentId);
        SegmentConfig segmentConfig = new SegmentConfig(writableSegmentId, config.dataDir(), config.segmentSize());
        WritableSegment segment = new FileSegment(segmentConfig, position);
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.id());
        writableSegment.set(new WritableSegmentContainer(segment, segmentMetadata));
    }

    private WritableSegment openReplicatedSegment(long writableSegmentId) throws IOException {
        long stamp = replicaSegmentLock.tryOptimisticRead();
        WritableSegment segment = replicaSegments.get(writableSegmentId);
        if (segment != null && replicaSegmentLock.validate(stamp)) {
            return segment;
        }

        stamp = replicaSegmentLock.writeLock();
        try {
            segment = replicaSegments.get(writableSegmentId);
            if (segment != null) {
                return segment;
            }

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(tr, subspace);
                if (!segmentIds.contains(writableSegmentId)) {
                    throw new SegmentNotFoundException(writableSegmentId);
                }
            }

            long position = SegmentSubspaceUtil.findNextPosition(context, config.subspace(), writableSegmentId);
            SegmentConfig segmentConfig = new SegmentConfig(writableSegmentId, config.dataDir(), config.segmentSize());
            segment = new FileSegment(segmentConfig, position);
            replicaSegments.put(writableSegmentId, segment);
            return segment;
        } finally {
            replicaSegmentLock.unlockWrite(stamp);
        }
    }

    private void openReadableSegments(List<Long> segments) throws IOException {
        for (long readableSegmentId : segments) {
            openReadableSegment(readableSegmentId);
        }
    }

    private WritableSegmentContainer getOrCreateWritableSegment(int size) throws IOException {
        // Fast path for fresh segments
        WritableSegmentContainer container = writableSegment.get();
        if (size <= container.segment().getFreeBytes()) {
            return container;
        }

        segmentLock.writeLock().lock();
        try {
            raiseExceptionIfVolumeClosed();
            // Try again against the concurrent callers
            container = writableSegment.get();
            if (size <= container.segment().getFreeBytes()) {
                return container;
            }

            long newSegmentId = computeNextSegmentId();

            Path segmentPath = SegmentUtil.getFilePath(config.dataDir(), newSegmentId);
            if (Files.isRegularFile(segmentPath)) {
                throw new IOException("Segment file already exists: " + segmentPath);
            }
            if (!Files.isWritable(segmentPath.getParent())) {
                throw new IOException("Segment directory is not writable: " + segmentPath.getParent());
            }

            SegmentConfig segmentConfig = new SegmentConfig(newSegmentId, config.dataDir(), config.segmentSize());
            WritableSegment segment = new FileSegment(segmentConfig, 0);

            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] segmentIdKey = subspace.packVolumeSegmentIdKey(newSegmentId);
                tr.set(segmentIdKey, VolumeMetadataUtil.NULL_BYTES);
                tr.commit().join();
            } catch (RuntimeException e) {
                try {
                    segment.close();
                    Files.deleteIfExists(segmentPath);
                } catch (IOException ignored) {
                }
                throw e;
            }

            SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.id());
            container = new WritableSegmentContainer(segment, segmentMetadata);

            // Close the previous WritableSegment and open it as a ReadableSegment.
            WritableSegmentContainer previous = writableSegment.getAndSet(container);
            previous.segment().close();
            if (!readableSegments.containsKey(previous.segment().id())) {
                openReadableSegment(previous.segment().id());
            }

            if (!readableSegments.containsKey(newSegmentId)) {
                openReadableSegment(newSegmentId);
            }
            stats.incrementSegmentsCreated();
            return container;
        } finally {
            segmentLock.writeLock().unlock();
        }
    }

    private SegmentMetadata getSegmentMetadata(long segmentId) {
        ReadableSegmentContainer container = readableSegments.get(segmentId);
        if (container != null) {
            return container.metadata();
        }
        WritableSegmentContainer writable = writableSegment.get();
        if (writable.segment().id() == segmentId) {
            return writable.metadata();
        }
        throw new SegmentNotFoundException(segmentId);
    }

    /**
     * Sets an attribute. Access to the attribute map is synchronized.
     *
     * @param <T>   the type of the attribute value
     * @param key   the attribute key
     * @param value the value to set
     */
    public <T> void setAttribute(AttributeKey<T> key, T value) {
        synchronized (attributes) {
            attributes.attr(key).set(value);
        }
    }

    /**
     * Returns the value of an attribute.
     *
     * @param key the attribute key
     * @param <T> the type of the attribute value
     * @return the attribute value, or null if no value is set
     */
    public <T> T getAttribute(AttributeKey<T> key) {
        synchronized (attributes) {
            return attributes.attr(key).get();
        }
    }

    /**
     * Unsets an attribute by setting its value to null.
     *
     * @param <T> the type of the attribute value
     * @param key the attribute key, must not be null
     */
    public <T> void unsetAttribute(AttributeKey<T> key) {
        synchronized (attributes) {
            attributes.attr(key).set(null);
        }
    }

    /**
     * Returns the current status of the volume.
     *
     * @return the current volume status
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
     * Sets the volume status. The write lock is held during the update.
     * The new status is written to FoundationDB first and applied locally
     * only after the commit succeeds.
     *
     * @param status the new volume status
     */
    public void setStatus(VolumeStatus status) {
        transactionWithRetry.executeRunnable(() -> {
            statusLock.writeLock().lock();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                byte[] statusKey = subspace.packVolumeStatusKey();
                tr.set(statusKey, status.toString().getBytes(StandardCharsets.US_ASCII));
                tr.commit().join();
                // Set the status only if the commit was successful.
                this.status = status;
            } finally {
                statusLock.writeLock().unlock();
            }
        });
    }

    /**
     * Raises an exception if the volume is in READONLY status.
     *
     * @throws VolumeReadOnlyException if the volume is read-only
     */
    private void raiseExceptionIfVolumeReadOnly() {
        // Raise an exception if the volume is in READONLY status.
        if (getStatus().equals(VolumeStatus.READONLY)) {
            throw new VolumeReadOnlyException("Volume: " + config.name() + " is read-only");
        }
    }

    private void raiseExceptionIfVolumeClosed() {
        if (closed) {
            throw new ClosedVolumeException(config.name());
        }
    }

    /**
     * Returns the subspace of this volume.
     *
     * @return the volume subspace
     */
    protected VolumeSubspace getSubspace() {
        return subspace;
    }

    /**
     * Returns the configuration of this volume.
     *
     * @return the volume configuration
     */
    public VolumeConfig getConfig() {
        return config;
    }

    public ChangeLog getChangeLog() {
        return changeLog;
    }

    private void flushWritableSegment() throws IOException {
        // Flush the writable segment if the writeable segment is rolled over
        // it's already flushed by close method.
        try {
            WritableSegmentContainer container = writableSegment.get();
            container.segment().flush();
        } catch (ClosedChannelException ignored) {
            // Already flushed
        }
    }

    /**
     * Appends an entry to a writable segment with enough free space.
     * If the current segment is full, a new segment is created and the append is retried.
     *
     * @param prefix the prefix of the entry
     * @param entry  the entry to append
     * @return metadata of the appended entry
     * @throws IOException if opening or creating a segment fails
     */
    private EntryMetadata tryAppend(Prefix prefix, ByteBuffer entry) throws IOException {
        int size = entry.remaining();
        while (true) {
            WritableSegmentContainer container = getOrCreateWritableSegment(size);
            try {
                SegmentAppendResult result = container.segment().append(entry);
                long handle = EntryHandleGenerator.generate(volumeId, container.segment().getConfig().id(), result.position());
                return new EntryMetadata(container.segment().id(), prefix.asBytes(), result.position(), result.length(), handle);
            } catch (NotEnoughSpaceException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Trying to find a new segment with length {}", size);
                }
            } catch (ClosedChannelException e) {
                raiseExceptionIfVolumeClosed();
            }
        }
    }

    /**
     * Writes entry metadata to FoundationDB within the session's transaction.
     * For each entry it stores the metadata under a versionstamped key (prefix plus versionstamp),
     * the reverse index (metadata bytes to versionstamp), and the segment position index.
     * It also increments segment cardinality and used bytes, and appends the operation to the changelog.
     * <p>
     * The versionstamp is incomplete at write time. FoundationDB fills it in at commit,
     * which gives unique and monotonically increasing keys.
     *
     * @param session the volume session with the transaction and prefix
     * @param entries the entry metadata to write
     * @return the appended entries and the versionstamp future of the transaction
     */
    private WriteMetadataResult writeMetadata(VolumeSession session, EntryMetadata[] entries) {
        AppendedEntry[] appendedEntries = new AppendedEntry[entries.length];
        Transaction tr = session.transaction();

        for (int index = 0; index < entries.length; index++) {
            EntryMetadata metadata = entries[index];
            int userVersion = session.getAndIncrementUserVersion();
            byte[] metadataBytes = metadata.encode();
            appendedEntries[index] = new AppendedEntry(index, userVersion, metadata, metadataBytes);

            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_KEY,
                    subspace.packEntryKeyWithVersionstamp(session.prefix(), userVersion),
                    metadataBytes
            );
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_VALUE,
                    subspace.packEntryMetadataKey(metadataBytes),
                    Tuple.from(Versionstamp.incomplete(userVersion)).packWithVersionstamp()
            );
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_VALUE,
                    subspace.packSegmentPositionKey(metadata.segmentId(), metadata.position(), metadata.length()),
                    Tuple.from(Versionstamp.incomplete(userVersion)).packWithVersionstamp()
            );

            SegmentMetadata segmentMetadata = getSegmentMetadata(metadata.segmentId());
            segmentMetadata.increaseCardinalityByOne(session);
            segmentMetadata.increaseUsedBytes(session, metadata.length());

            changeLog.appendOperation(tr, metadata, session.prefix(), userVersion);
        }

        return new WriteMetadataResult(appendedEntries, tr.getVersionstamp());
    }

    /**
     * Appends multiple entries under the given prefix and returns their metadata.
     *
     * @param prefix  the prefix of the entries
     * @param entries the entries to append
     * @return metadata of the appended entries, in input order
     * @throws IOException if an I/O error occurs during the append
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

    public AppendResult append(@Nonnull VolumeSession session, @Nonnull ByteBuffer... entries) throws IOException {
        raiseExceptionIfVolumeClosed();
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
            flushWritableSegment();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        WriteMetadataResult result = writeMetadata(session, appendEntries);
        triggerWatchers(session.transaction());

        raiseExceptionIfVolumeReadOnly();

        stats.addAppends(entries.length);
        long totalBytes = 0;
        for (ByteBuffer entry : entries) {
            totalBytes += entry.limit();
        }
        stats.addBytesAppended(totalBytes);

        return new AppendResult(result.versionstampFuture(), result.entries(), entryMetadataCache.load(session.prefix())::put);
    }

    /**
     * Loads entry metadata from the cache for a prefix and versionstamp key.
     *
     * @param prefix the prefix of the entry
     * @param key    the versionstamp key of the entry
     * @return the entry metadata, or null if the key does not exist in this volume
     * @throws KronotopException if loading the metadata from FoundationDB fails
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
     * Reads an entry from its segment using the given metadata.
     * If the segment no longer exists, the cached metadata for the key is invalidated,
     * fresh metadata is loaded from FoundationDB, and the read is retried.
     *
     * @param prefix   the prefix used for the entry metadata cache
     * @param key      the versionstamp key of the entry
     * @param metadata the metadata with segment id, position and length of the entry
     * @return the entry data
     * @throws IOException if reading the segment fails
     */
    protected ByteBuffer getByEntryMetadata(Prefix prefix, Versionstamp key, EntryMetadata metadata) throws IOException {
        try {
            return getByEntryMetadata(metadata);
        } catch (SegmentNotFoundException e) {
            // Stale metadata points to a segment that no longer exists.
            // Invalidate the cache, reload fresh metadata from FoundationDB, and retry.
            entryMetadataCache.load(prefix).invalidate(key);
            EntryMetadata fresh = loadEntryMetadataFromCache(prefix, key);
            if (fresh == null) {
                throw new SegmentNotFoundException(metadata.segmentId());
            }
            return getByEntryMetadata(fresh);
        }
    }

    public ByteBuffer get(VolumeSession session, Versionstamp key) throws IOException {
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
     * Reads an entry using the given prefix, versionstamp key and metadata.
     *
     * @param prefix   the prefix of the entry, must not be null
     * @param key      the versionstamp key of the entry, must not be null
     * @param metadata the metadata of the entry, must not be null
     * @return the entry data
     * @throws IOException if reading the segment fails
     */
    public ByteBuffer get(@Nonnull Prefix prefix, @Nonnull Versionstamp key, @Nonnull EntryMetadata metadata) throws IOException {
        // This method is tested by PlanExecutor indirectly.
        return getByEntryMetadata(prefix, key, metadata);
    }

    /**
     * Reads an entry directly from its segment. No versionstamp key is needed
     * and the entry metadata cache is bypassed.
     *
     * @param metadata the metadata with segment id, position and length of the entry
     * @return the entry data
     * @throws IOException if reading the segment fails
     */
    public ByteBuffer getByEntryMetadata(EntryMetadata metadata) throws IOException {
        Objects.requireNonNull(metadata);

        ReadableSegment segment = getReadableSegment(metadata.segmentId());
        ByteBuffer result = segment.get(metadata.position(), metadata.length());
        stats.incrementGets();
        stats.addBytesRead(result.remaining());
        return result;
    }

    /**
     * Reads several ranges from one segment.
     *
     * @param segmentId     the id of the segment to read from
     * @param segmentRanges the positions and lengths to read
     * @return the data of each range, in input order
     * @throws IOException if reading the segment fails
     */
    public ByteBuffer[] getSegmentRange(long segmentId, SegmentRange[] segmentRanges) throws IOException {
        ReadableSegment segment = getReadableSegment(segmentId);
        ByteBuffer[] entries = new ByteBuffer[segmentRanges.length];
        long totalBytesRead = 0;
        for (int i = 0; i < segmentRanges.length; i++) {
            SegmentRange segmentRange = segmentRanges[i];
            entries[i] = segment.get(segmentRange.position(), segmentRange.length());
            totalBytesRead += entries[i].remaining();
        }
        stats.addBytesRead(totalBytesRead);
        stats.incrementGets();
        return entries;
    }

    public DeleteResult delete(@Nonnull VolumeSession session, @Nonnull Versionstamp... keys) {
        if (keys.length == 0) {
            throw new IllegalArgumentException("Empty keys array");
        }
        Transaction tr = session.transaction();

        DeleteResult result = new DeleteResult(keys.length, entryMetadataCache.load(session.prefix())::invalidate);

        int index = 0;
        for (Versionstamp key : keys) {
            byte[] entryKey = subspace.packEntryKey(session.prefix(), key);
            byte[] metadataBytes = tr.get(entryKey).join();
            if (metadataBytes == null) {
                // Already deleted by a previously committed transaction.
                continue;
            }
            tr.clear(entryKey);
            tr.clear(subspace.packEntryMetadataKey(metadataBytes));

            EntryMetadata metadata = EntryMetadata.decode(metadataBytes);
            applyDelete(session, metadata, key, result, index);
            index++;
        }
        triggerWatchers(tr);
        raiseExceptionIfVolumeReadOnly();
        stats.addDeletes(keys.length);
        return result;
    }

    /**
     * Deletes entries within the session's transaction. Unlike {@link #delete}, no extra
     * FoundationDB lookups are needed because each {@link VersionstampedEntry} already
     * carries the versionstamp key and the entry metadata.
     *
     * @param session the session that provides the transaction and prefix
     * @param entries the entries to delete, must not be null or empty
     * @return the deleted versionstamp keys and a cache invalidator
     * @throws IllegalArgumentException if the entries array is empty
     * @throws VolumeReadOnlyException  if the volume is read-only
     */
    public DeleteResult deleteByVersionstampedEntry(@Nonnull VolumeSession session, @Nonnull VersionstampedEntry... entries) {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        DeleteResult result = new DeleteResult(entries.length, entryMetadataCache.load(session.prefix())::invalidate);
        Transaction tr = session.transaction();
        int index = 0;
        for (VersionstampedEntry entry : entries) {
            byte[] metadataBytes = entry.metadata().encode();

            byte[] entryKey = subspace.packEntryKey(session.prefix(), entry.key());
            tr.clear(entryKey);

            byte[] reverseKey = subspace.packEntryMetadataKey(metadataBytes);
            tr.clear(reverseKey);

            applyDelete(session, entry.metadata(), entry.key(), result, index);
            index++;
        }
        triggerWatchers(tr);
        raiseExceptionIfVolumeReadOnly();
        stats.addDeletes(entries.length);
        return result;
    }

    /**
     * Applies a single delete. It adjusts segment cardinality and used bytes,
     * writes to the changelog, and records the deleted key in the result.
     */
    private void applyDelete(VolumeSession session, EntryMetadata metadata, Versionstamp key, DeleteResult result, int index) {
        try {
            SegmentMetadata segmentMetadata = getSegmentMetadata(metadata.segmentId());
            segmentMetadata.decreaseCardinalityByOne(session);
            segmentMetadata.increaseUsedBytes(session, -1 * metadata.length());

            changeLog.deleteOperation(session.transaction(), metadata, session.prefix(), key);

            result.add(index, key);
        } catch (SegmentNotFoundException exp) {
            throw new RetryableStateException(exp.getMessage());
        }
    }

    /**
     * Applies a single update. It adjusts segment cardinality and used bytes,
     * clears the old reverse index entry, writes the new metadata, and writes to the changelog.
     */
    private UpdatedEntry applyUpdate(
            VolumeSession session,
            Versionstamp key,
            byte[] packedKey,
            EntryMetadata prevMetadata,
            byte[] encodedPrevMetadata,
            EntryMetadata newMetadata
    ) {
        Transaction tr = session.transaction();

        SegmentMetadata prev = getSegmentMetadata(prevMetadata.segmentId());
        SegmentMetadata curr = getSegmentMetadata(newMetadata.segmentId());
        if (curr == null) {
            throw new IllegalStateException("Segment: " + newMetadata.segmentId() + " could not be found");
        }

        // Check the previous container here, it might have been removed.
        if (prev != null && prevMetadata.segmentId() != (newMetadata.segmentId())) {
            prev.decreaseCardinalityByOne(session);
            curr.increaseCardinalityByOne(session);
            prev.increaseUsedBytes(session, -1 * prevMetadata.length());
        } else {
            curr.increaseUsedBytes(session, -1 * prevMetadata.length());
        }
        curr.increaseUsedBytes(session, newMetadata.length());

        // Update ENTRY_SUBSPACE (forward index)
        byte[] encodedNewMetadata = newMetadata.encode();
        tr.set(packedKey, encodedNewMetadata);

        // Update ENTRY_METADATA_SUBSPACE (reverse index)
        tr.clear(subspace.packEntryMetadataKey(encodedPrevMetadata));
        byte[] encodedKey = VersionstampUtil.encodeVersionstampedValue(key);
        tr.set(subspace.packEntryMetadataKey(encodedNewMetadata), encodedKey);

        // Write the segment position index for the new metadata
        tr.set(
                subspace.packSegmentPositionKey(newMetadata.segmentId(), newMetadata.position(), newMetadata.length()),
                encodedKey
        );

        changeLog.updateOperation(tr, prevMetadata, newMetadata, session.prefix(), key);

        return new UpdatedEntry(key, newMetadata, encodedNewMetadata);
    }

    private EntryMetadata[] validateAndAppendEntries(Prefix prefix, KeyEntry[] pairs) throws IOException {
        raiseExceptionIfVolumeClosed();
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
        EntryMetadata[] entryMetadataList = appendEntries(prefix, entries);
        flushWritableSegment();
        return entryMetadataList;
    }

    public UpdateResult update(@Nonnull VolumeSession session, @Nonnull KeyEntry... pairs) throws IOException, KeyNotFoundException {
        EntryMetadata[] entryMetadataList = validateAndAppendEntries(session.prefix(), pairs);

        UpdatedEntry[] updatedEntries = new UpdatedEntry[pairs.length];
        Transaction tr = session.transaction();
        int index = 0;
        for (KeyEntry keyEntry : pairs) {
            Versionstamp key = keyEntry.key();
            byte[] packedKey = subspace.packEntryKey(session.prefix(), key);
            byte[] encodedPrevMetadata = tr.get(packedKey).join();
            if (encodedPrevMetadata == null) {
                throw new KeyNotFoundException(key);
            }

            EntryMetadata prevMetadata = EntryMetadata.decode(encodedPrevMetadata);
            updatedEntries[index] = applyUpdate(session, key, packedKey, prevMetadata, encodedPrevMetadata, entryMetadataList[index]);
            index++;
        }

        triggerWatchers(tr);

        raiseExceptionIfVolumeReadOnly();
        stats.addUpdates(pairs.length);
        long totalBytes = 0;
        for (KeyEntry pair : pairs) {
            totalBytes += pair.entry().limit();
        }
        stats.addBytesAppended(totalBytes);
        return new UpdateResult(updatedEntries, entryMetadataCache.load(session.prefix())::put);
    }

    public UpdateResult updateByVersionstampedEntryUpdate(VolumeSession session, VersionstampedEntryUpdate... entries) throws IOException {
        raiseExceptionIfVolumeClosed();
        Objects.requireNonNull(session);
        Objects.requireNonNull(entries);

        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        for (VersionstampedEntryUpdate entry : entries) {
            if (entry.entry().remaining() > ENTRY_SIZE_LIMIT) {
                throw new EntrySizeExceedsLimitException();
            }
        }

        ByteBuffer[] buffers = new ByteBuffer[entries.length];
        for (int i = 0; i < entries.length; i++) {
            buffers[i] = entries[i].entry();
        }
        EntryMetadata[] newMetadataList = appendEntries(session.prefix(), buffers);
        flushWritableSegment();

        UpdatedEntry[] updatedEntries = new UpdatedEntry[entries.length];
        int index = 0;
        for (VersionstampedEntryUpdate entry : entries) {
            byte[] packedKey = subspace.packEntryKey(session.prefix(), entry.key());
            byte[] encodedPrevMetadata = entry.metadata().encode();
            updatedEntries[index] = applyUpdate(
                    session,
                    entry.key(),
                    packedKey,
                    entry.metadata(),
                    encodedPrevMetadata,
                    newMetadataList[index]
            );
            index++;
        }

        triggerWatchers(session.transaction());

        raiseExceptionIfVolumeReadOnly();
        stats.addUpdates(entries.length);
        long totalBytes = 0;
        for (ByteBuffer buffer : buffers) {
            totalBytes += buffer.limit();
        }
        stats.addBytesAppended(totalBytes);
        return new UpdateResult(updatedEntries, entryMetadataCache.load(session.prefix())::put);
    }

    /**
     * Inserts entries with caller-supplied versionstamp keys and rejects duplicates.
     * <p>
     * This sits between {@link #append} and {@link #update}. With {@code append} the key does not
     * exist yet and FoundationDB assigns a new versionstamp via {@code SET_VERSIONSTAMPED_KEY}.
     * With {@code insert} the key does not exist yet but the caller supplies the versionstamp.
     * This is used when an entry moves to another volume, for example during shard migration,
     * and must keep its original identity. With {@code update} the key already exists. Its metadata
     * is overwritten, the old reverse index is cleared, and segment stats are adjusted for the
     * previous entry.
     * <p>
     * Each entry is appended to a segment on disk. Then its metadata, reverse index and segment
     * position index are written to FoundationDB. Segment cardinality and used bytes are only
     * incremented because there is no previous entry to compensate for.
     *
     * @param session the volume session that provides the transaction and prefix
     * @param pairs   one or more key-entry pairs, where each key is the versionstamp to keep
     * @return the insert result with metadata of all inserted entries
     * @throws VersionstampAlreadyExistsException if any key already exists in this volume
     * @throws IOException                        if a segment write fails
     */
    public InsertResult insert(@Nonnull VolumeSession session, @Nonnull KeyEntry... pairs) throws IOException, VersionstampAlreadyExistsException {
        EntryMetadata[] entryMetadataList = validateAndAppendEntries(session.prefix(), pairs);

        Transaction tr = session.transaction();
        InsertedEntry[] insertedEntries = new InsertedEntry[pairs.length];
        for (int i = 0; i < pairs.length; i++) {
            Versionstamp key = pairs[i].key();
            byte[] packedKey = subspace.packEntryKey(session.prefix(), key);

            // Check for duplicate
            byte[] existing = tr.get(packedKey).join();
            if (existing != null) {
                throw new VersionstampAlreadyExistsException(key);
            }

            EntryMetadata metadata = entryMetadataList[i];
            byte[] metadataBytes = metadata.encode();

            // Write entry metadata (regular set, not SET_VERSIONSTAMPED_KEY)
            tr.set(packedKey, metadataBytes);

            // Write reverse index
            byte[] reverseIndexKey = VersionstampUtil.encodeVersionstampedValue(key);
            tr.set(subspace.packEntryMetadataKey(metadataBytes), reverseIndexKey);

            // Write segment position index
            tr.set(
                    subspace.packSegmentPositionKey(metadata.segmentId(), metadata.position(), metadata.length()),
                    reverseIndexKey
            );

            // Update segment stats
            SegmentMetadata segmentMetadata = getSegmentMetadata(metadata.segmentId());
            segmentMetadata.increaseCardinalityByOne(session);
            segmentMetadata.increaseUsedBytes(session, metadata.length());

            // Changelog
            changeLog.insertOperation(tr, metadata, session.prefix(), key);

            insertedEntries[i] = new InsertedEntry(key, metadata, metadataBytes);
        }

        triggerWatchers(tr);
        raiseExceptionIfVolumeReadOnly();
        stats.addAppends(pairs.length);
        long totalBytes = 0;
        for (KeyEntry pair : pairs) {
            totalBytes += pair.entry().limit();
        }
        stats.addBytesAppended(totalBytes);
        return new InsertResult(insertedEntries, entryMetadataCache.load(session.prefix())::put);
    }

    public void flush() throws IOException {
        WritableSegmentContainer container = writableSegment.get();
        container.segment().flush();

        for (WritableSegment segment : replicaSegments.values()) {
            segment.flush();
        }
    }

    public void vacuumStart(float garbageThreshold, Supplier<EntryEvacuator> evacuatorFactory) {
        vacuumLock.lock();
        try {
            if (vacuumWatchDog != null && !vacuumWatchDog.isStopped()) {
                throw new KronotopException("Vacuum is already running on volume " + config.name());
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (VacuumMetadataUtil.exists(tr, subspace)) {
                    throw new KronotopException("Stale vacuum metadata exists on volume " + config.name() + ", run DROP first");
                }
            }
            int maxWorkers = context.getConfig().getInt("volume.vacuum.max_workers");
            if (maxWorkers <= 0) {
                maxWorkers = Runtime.getRuntime().availableProcessors();
            }
            vacuumWatchDog = new VacuumWatchDog(context, this, garbageThreshold, maxWorkers, evacuatorFactory);
            vacuumExecutor.submit(vacuumWatchDog);
        } finally {
            vacuumLock.unlock();
        }
    }

    public void vacuumStop() {
        vacuumLock.lock();
        try {
            VacuumWatchDog watchDog = vacuumWatchDog;
            if (watchDog == null || watchDog.isStopped()) {
                throw new KronotopException("No active vacuum on volume " + config.name());
            }
            watchDog.stop();
            vacuumWatchDog = null;
        } finally {
            vacuumLock.unlock();
        }
    }

    public void vacuumDrop() {
        vacuumLock.lock();
        try {
            if (vacuumWatchDog != null && !vacuumWatchDog.isStopped()) {
                throw new KronotopException("Vacuum is still running on volume " + config.name() + ", run STOP first");
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (!VacuumMetadataUtil.exists(tr, subspace)) {
                    throw new KronotopException("No active vacuum on volume " + config.name());
                }
                VacuumMetadataUtil.deleteAll(tr, subspace);
                tr.commit().join();
            }
        } finally {
            vacuumLock.unlock();
        }
    }

    public VacuumStatusResult vacuumStatus() {
        boolean active;
        vacuumLock.lock();
        try {
            active = vacuumWatchDog != null && !vacuumWatchDog.isStopped();
        } finally {
            vacuumLock.unlock();
        }
        if (!active) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                if (!VacuumMetadataUtil.exists(tr, subspace)) {
                    throw new KronotopException("No active vacuum on volume " + config.name());
                }
            }
        }
        VacuumMetadata metadata;
        List<VacuumSegmentMetadata> segments;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            metadata = VacuumMetadataUtil.load(tr, subspace);
            segments = VacuumSegmentMetadataUtil.loadAll(tr, subspace);
        }
        return new VacuumStatusResult(active, metadata, segments);
    }

    public void close() {
        closed = true;

        vacuumLock.lock();
        try {
            VacuumWatchDog watchDog = vacuumWatchDog;
            if (watchDog != null && !watchDog.isStopped()) {
                watchDog.stop();
                vacuumWatchDog = null;
            }
        } finally {
            vacuumLock.unlock();
        }
        ExecutorServiceUtil.shutdownNowThenAwaitTermination(vacuumExecutor);

        // Acquire the write lock here. After closing the segments, they are not usable anymore.
        segmentLock.writeLock().lock();
        try {
            WritableSegmentContainer writable = writableSegment.get();
            writable.segment().close();

            for (ReadableSegmentContainer readable : readableSegments.values()) {
                if (writable.segment().id() == readable.segment().id()) {
                    continue;
                }
                readable.segment().close();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to close the writable segment", e);
        } finally {
            segmentLock.writeLock().unlock();
        }

        long stamp = replicaSegmentLock.writeLock();
        try {
            for (WritableSegment segment : replicaSegments.values()) {
                segment.close();
            }
        } catch (IOException e) {
            LOGGER.error("Failed to close the replica segment", e);
        } finally {
            replicaSegmentLock.unlockWrite(stamp);
        }
    }

    /**
     * Returns whether the volume is closed.
     *
     * @return true if the volume is closed, false otherwise
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * Returns all entries of the session's prefix.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session) {
        return getRange(session, ReadTransaction.ROW_LIMIT_UNLIMITED);
    }

    /**
     * Returns all entries of the session's prefix in the given direction.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @param reverse if true, entries are returned in reverse order
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, boolean reverse) {
        return getRange(session, ReadTransaction.ROW_LIMIT_UNLIMITED, reverse);
    }

    /**
     * Returns up to {@code limit} entries of the session's prefix.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @param limit   the maximum number of entries to return
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, int limit) {
        return getRange(session, limit, false);
    }

    /**
     * Returns up to {@code limit} entries of the session's prefix in the given direction.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @param limit   the maximum number of entries to return
     * @param reverse if true, entries are returned in reverse order
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, int limit, boolean reverse) {
        return new VolumeIterable(this, session, null, null, limit, reverse);
    }

    /**
     * Returns the entries between the begin and end key selectors.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @param begin   the key selector where the range starts
     * @param end     the key selector where the range ends
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        return getRange(session, begin, end, ReadTransaction.ROW_LIMIT_UNLIMITED);
    }

    /**
     * Returns up to {@code limit} entries between the begin and end key selectors.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @param begin   the key selector where the range starts
     * @param end     the key selector where the range ends
     * @param limit   the maximum number of entries to return
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) {
        return new VolumeIterable(this, session, begin, end, limit, false);
    }

    /**
     * Returns up to {@code limit} entries between the begin and end key selectors in the given direction.
     *
     * @param session the session that provides the transaction and prefix, must not be null
     * @param begin   the key selector where the range starts
     * @param end     the key selector where the range ends
     * @param limit   the maximum number of entries to return
     * @param reverse if true, entries are returned in reverse order
     * @return an iterable over the entries
     */
    public Iterable<VolumeEntry> getRange(@Nonnull VolumeSession session, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit, boolean reverse) {
        return new VolumeIterable(this, session, begin, end, limit, reverse);
    }

    /**
     * Analyzes all segments using the given transaction.
     *
     * @param tr the transaction used for the analysis
     * @return the analysis result of each segment
     */
    public List<SegmentAnalysis> analyze(Transaction tr) {
        WritableSegmentContainer writable = writableSegment.get();
        long writableSegmentId = writable != null ? writable.segment().id() : -1;

        // Fire async tail pointer reads for all readable segments
        Map<Long, CompletableFuture<List<KeyValue>>> tailPointerFutures = new HashMap<>();
        Map<Long, ReadableSegment> segmentsById = new HashMap<>();

        for (ReadableSegmentContainer container : readableSegments.values()) {
            ReadableSegment segment = container.segment();
            if (segment.id() == writableSegmentId) {
                continue;
            }
            segmentsById.put(segment.id(), segment);

            byte[] prefix = config.subspace().pack(Tuple.from(SEGMENT_POSITION_SUBSPACE, segment.id()));
            Range range = Range.startsWith(prefix);
            tailPointerFutures.put(segment.id(), tr.snapshot().getRange(range, 1, true).asList());
        }

        // Single range scan for all segment stats, summed across prefixes
        Map<Long, Integer> cardinalityBySegment = new HashMap<>();
        Map<Long, Long> usedBytesBySegment = new HashMap<>();

        byte[] statsPrefix = subspace.packSegmentStatsVolumePrefix();
        Range statsRange = Range.startsWith(statsPrefix);
        for (KeyValue keyValue : tr.snapshot().getRange(statsRange)) {
            Tuple tuple = config.subspace().unpack(keyValue.getKey());
            long segmentId = tuple.getLong(1);
            long statType = tuple.getLong(3);
            if (statType == SegmentStatsSubspaces.CARDINALITY) {
                int value = ByteBuffer.wrap(keyValue.getValue()).order(ByteOrder.LITTLE_ENDIAN).getInt();
                cardinalityBySegment.merge(segmentId, value, Integer::sum);
            } else if (statType == SegmentStatsSubspaces.USED_BYTES) {
                long value = ByteBuffer.wrap(keyValue.getValue()).order(ByteOrder.LITTLE_ENDIAN).getLong();
                usedBytesBySegment.merge(segmentId, value, Long::sum);
            }
        }

        // Join tail pointer futures and assemble results
        List<SegmentAnalysis> result = new ArrayList<>();

        for (Map.Entry<Long, CompletableFuture<List<KeyValue>>> entry : tailPointerFutures.entrySet()) {
            long segmentId = entry.getKey();
            ReadableSegment segment = segmentsById.get(segmentId);

            List<KeyValue> tailResult = entry.getValue().join();
            long freeBytes;
            if (tailResult.isEmpty()) {
                freeBytes = segment.getSize();
            } else {
                Tuple keyTuple = config.subspace().unpack(tailResult.getFirst().getKey());
                long position = keyTuple.getLong(2);
                long length = keyTuple.getLong(3);
                freeBytes = segment.getSize() - (position + length);
            }

            int cardinality = cardinalityBySegment.getOrDefault(segmentId, 0);
            long usedBytes = usedBytesBySegment.getOrDefault(segmentId, 0L);
            result.add(new SegmentAnalysis(segmentId, segment.getSize(), usedBytes, freeBytes, cardinality));
        }

        if (writable != null) {
            WritableSegment segment = writable.segment();
            int cardinality = cardinalityBySegment.getOrDefault(segment.id(), 0);
            long usedBytes = usedBytesBySegment.getOrDefault(segment.id(), 0L);
            result.add(new SegmentAnalysis(segment.id(), segment.getSize(), usedBytes, segment.getFreeBytes(), cardinality));
        }

        return result;
    }

    /**
     * Analyzes all segments in a new FoundationDB transaction.
     *
     * @return the analysis result of each segment
     */
    public List<SegmentAnalysis> analyze() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return analyze(tr);
        }
    }

    /**
     * Clears the segment position entries of the session's prefix in every segment
     * and resets the cardinality and used bytes of that prefix.
     *
     * @param session the session that provides the transaction and prefix
     */
    private void clearSegmentsByPrefix(VolumeSession session) {
        assert session.transaction() != null;
        List<Long> segmentIds = VolumeMetadataUtil.loadSegmentIds(session.transaction(), subspace);
        for (long segmentId : segmentIds) {
            byte[] begin = SegmentSubspaceUtil.prefixOfVolumePrefix(config.subspace(), segmentId, session.prefix());
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

    public void clearPrefix(@Nonnull VolumeSession session) {
        Objects.requireNonNull(session.transaction());
        Objects.requireNonNull(session.prefix());

        raiseExceptionIfVolumeReadOnly();
        clearSegmentsByPrefix(session);
        clearEntrySubspace(session);
    }

    public void insert(long segmentId, PackedEntry... entries) throws IOException {
        Objects.requireNonNull(entries);

        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        for (PackedEntry entry : entries) {
            if (entry.data().length > ENTRY_SIZE_LIMIT) {
                throw new EntrySizeExceedsLimitException();
            }
        }

        WritableSegment segment = openReplicatedSegment(segmentId);

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
        stats.addAppends(entries.length);
        long totalBytes = 0;
        for (PackedEntry entry : entries) {
            totalBytes += entry.data().length;
        }
        stats.addBytesAppended(totalBytes);
    }

    void destroyStaleSegment(long segmentId) throws IOException {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            // Second check against possible stale reads.
            ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
            buf.putLong(segmentId);
            byte[] packed = subspace.packEntryMetadataKey(buf.array());
            byte[] scanPrefix = Arrays.copyOf(packed, packed.length - 1);
            if (!tr.getRange(Range.startsWith(scanPrefix), 1).asList().join().isEmpty()) {
                throw new IllegalStateException("Segment " + segmentId + " still has entries, refusing to clean up");
            }

            tr.clear(Range.startsWith(subspace.packSegmentPositionPrefix(segmentId)));
            tr.clear(Range.startsWith(subspace.packSegmentStatsSegmentPrefix(segmentId)));
            tr.clear(subspace.packVolumeSegmentIdKey(segmentId));
            VacuumSegmentMetadataUtil.delete(tr, subspace, segmentId);
            tr.commit().join();
        }

        segmentLock.writeLock().lock();
        try {
            ReadableSegmentContainer container = readableSegments.remove(segmentId);
            if (container != null) {
                String path = container.segment().destroy();
                LOGGER.info("Destroyed vacuumed segment {} at {}", segmentId, path);
            }
        } finally {
            segmentLock.writeLock().unlock();
        }
    }

    void vacuum(VacuumContext ctx, long segmentId, EntryEvacuator evacuator) throws IOException {
        WritableSegmentContainer writable = writableSegment.get();
        if (writable != null && writable.segment().id() == segmentId) {
            throw new VacuumWritableSegmentException(segmentId);
        }

        VacuumSegment vs = new VacuumSegment(context, subspace);
        boolean completed = vs.vacuum(ctx, segmentId, evacuator);
        if (completed) {
            destroyStaleSegment(segmentId);
        }
    }

    @Override
    public String toString() {
        return String.format("Volume [%s]", config.name());
    }

    record WriteMetadataResult(AppendedEntry[] entries, CompletableFuture<byte[]> versionstampFuture) {
    }
}
