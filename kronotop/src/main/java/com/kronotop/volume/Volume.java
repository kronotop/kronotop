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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.kronotop.volume.Prefixes.*;

public class Volume {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private static final byte[] SEGMENT_CARDINALITY_INCREASE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private static final byte[] SEGMENT_CARDINALITY_DECREASE_DELTA = new byte[]{-1, -1, -1, -1}; // -1, byte order: little-endian
    private static final int SEGMENT_VACUUM_BATCH_SIZE = 100;
    private static final byte[] CDC_TRIGGER_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian

    private final Context context;
    private final VolumeConfig config;
    private final LoadingCache<Versionstamp, EntryMetadata> entryMetadataCache;
    private final byte[] watchChangesStageTriggerKey;

    // segmentsLock protects segments array
    private final ReadWriteLock segmentsLock = new ReentrantReadWriteLock();
    private final List<Segment> segments = new ArrayList<>();
    private final HashMap<String, Segment> segmentsByName = new HashMap<>();
    private final HashMap<String, SegmentLog> segmentLogs = new HashMap<>();

    private volatile boolean isClosed;

    public Volume(Context context, VolumeConfig volumeConfig) throws IOException {
        this.context = context;
        this.config = volumeConfig;
        this.entryMetadataCache = CacheBuilder.newBuilder().expireAfterAccess(30, TimeUnit.MINUTES).build(new EntryMetadataLoader());
        this.watchChangesStageTriggerKey = config.subspace().pack(Tuple.from(VOLUME_WATCH_CHANGES_TRIGGER_PREFIX));
    }

    protected VolumeConfig getConfig() {
        return config;
    }

    private void triggerWatchChangesSubscribers(Transaction tr) {
        tr.mutate(MutationType.ADD, watchChangesStageTriggerKey, CDC_TRIGGER_DELTA);
    }

    private void mutateSegmentCardinality(Transaction tr, String name, byte[] delta) {
        byte[] segmentCardinalityKey = packSegmentCardinalityKey(name);
        tr.mutate(MutationType.ADD, segmentCardinalityKey, delta);
    }

    private byte[] packSegmentCardinalityKey(String name) {
        Tuple key = Tuple.from(SEGMENT_CARDINALITY_PREFIX, name);
        return config.subspace().pack(key);
    }

    private int decodeSegmentCardinality(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    private byte[] encodeSegmentUsedBytes(long length) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(length).array();
    }

    private byte[] packSegmentUsedBytesKey(String name) {
        Tuple key = Tuple.from(SEGMENT_USED_BYTES_PREFIX, name);
        return config.subspace().pack(key);
    }

    private void mutateSegmentUsedBytes(Transaction tr, String name, byte[] delta) {
        byte[] segmentUsedBytesKey = packSegmentUsedBytesKey(name);
        tr.mutate(MutationType.ADD, segmentUsedBytesKey, delta);
    }

    private long decodeSegmentUsedBytes(byte[] data) {
        return ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong();
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

    // should be protected by segmentsLock
    private Segment openSegment(long segmentId) throws IOException {
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig);
        segments.add(segment);
        segmentsByName.put(segment.getName(), segment);
        segmentLogs.put(segment.getName(), new SegmentLog(segment.getName(), config.subspace()));
        segments.sort(Comparator.comparing(Segment::getName));
        return segment;
    }

    // protected by segmentsLock
    private long getAndIncreaseSegmentId() {
        return context.getFoundationDB().run(tr -> {
            List<Long> availableSegments = VolumeMetadata.load(tr, config.subspace()).getSegments();
            if (availableSegments.isEmpty()) {
                return 0L;
            }
            return availableSegments.getLast() + 1;
        });
    }

    // createsSegment protected by segmentsLock
    private Segment createSegment() throws IOException {
        long segmentId = getAndIncreaseSegmentId();
        SegmentConfig segmentConfig = new SegmentConfig(segmentId, config.dataDir(), config.segmentSize());
        Segment segment = new Segment(segmentConfig);

        // After this point, the Segment has been created on the physical medium.

        // Update the volume metadata on FoundationDB
        context.getFoundationDB().run(tr -> {
            VolumeMetadata.compute(tr, config.subspace(), (volumeMetadata) -> {
                volumeMetadata.addSegment(segmentId);
            });
            return null;
        });

        // Make it available for the rest of the Volume.
        segments.add(segment);
        segments.sort(Comparator.comparing(Segment::getName));
        segmentsByName.put(segment.getName(), segment);
        segmentLogs.put(segment.getName(), new SegmentLog(segment.getName(), config.subspace()));
        return segment;
    }

    private Segment getOrCreateLatestSegment(int size) throws IOException {
        segmentsLock.writeLock().lock();
        try {
            if (segments.isEmpty()) {
                return createSegment();
            }
            Segment latest = segments.getLast();
            if (size > latest.getFreeBytes()) {
                return createSegment();
            }
            return latest;
        } finally {
            segmentsLock.writeLock().unlock();
        }
    }

    private Segment getLatestSegment(int size) throws IOException {
        segmentsLock.readLock().lock();
        try {
            Segment latest = segments.getLast();
            if (size < latest.getFreeBytes()) {
                return latest;
            }
        } catch (NoSuchElementException e) {
            // Ignore it, a new Segment will be created after releasing the read lock.
        } finally {
            segmentsLock.readLock().unlock();
        }
        return getOrCreateLatestSegment(size);
    }

    private EntryMetadata tryAppend(ByteBuffer entry) throws IOException {
        int size = entry.remaining();
        while (true) {
            Segment segment = getLatestSegment(size);
            try {
                return segment.append(entry);
            } catch (NotEnoughSpaceException e) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Trying to find a new segment with length {}", size);
                }
            }
        }
    }

    private byte[] packEntryKeyWithVersionstamp(int version) {
        Tuple key = Tuple.from(ENTRY_PREFIX, Versionstamp.incomplete(version));
        return config.subspace().packWithVersionstamp(key);
    }

    private byte[] packEntryMetadataKey(byte[] data) {
        return config.subspace().pack(Tuple.from(ENTRY_METADATA_PREFIX, data));
    }

    private void appendSegmentLog(Session session, OperationKind kind, EntryMetadata entryMetadata) {
        appendSegmentLog(session.getTransaction(), kind, session.getAndIncrementUserVersion(), entryMetadata);
    }

    private void appendSegmentLog(Transaction tr, OperationKind kind, int userVersion, EntryMetadata entryMetadata) {
        segmentsLock.readLock().lock();
        try {
            SegmentLog segmentLog = segmentLogs.get(entryMetadata.segment());
            if (segmentLog == null) {
                throw new IllegalStateException("Segment " + entryMetadata.segment() + " not found");
            }
            segmentLog.append(tr, userVersion, new SegmentLogValue(kind, entryMetadata.position(), entryMetadata.length()));
        } finally {
            segmentsLock.readLock().unlock();
        }
    }

    private CompletableFuture<byte[]> writeMetadata(Session session, EntryMetadata[] entryMetadataList) {
        Transaction tr = session.getTransaction();
        for (EntryMetadata entryMetadata : entryMetadataList) {
            int userVersion = session.getAndIncrementUserVersion();
            byte[] encodedEntryMetadata = entryMetadata.encode().array();

            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, packEntryKeyWithVersionstamp(userVersion), encodedEntryMetadata);
            tr.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, packEntryMetadataKey(encodedEntryMetadata), Tuple.from(Versionstamp.incomplete(userVersion)).packWithVersionstamp());

            mutateSegmentCardinality(tr, entryMetadata.segment(), SEGMENT_CARDINALITY_INCREASE_DELTA);
            mutateSegmentUsedBytes(tr, entryMetadata.segment(), encodeSegmentUsedBytes(entryMetadata.length()));

            appendSegmentLog(tr, OperationKind.APPEND, userVersion, entryMetadata);
            triggerWatchChangesSubscribers(tr);
        }
        return tr.getVersionstamp();
    }

    private EntryMetadata[] appendEntries(ByteBuffer[] entries) throws IOException {
        EntryMetadata[] entryMetadataList = new EntryMetadata[entries.length];
        int index = 0;
        for (ByteBuffer entry : entries) {
            EntryMetadata entryMetadata = tryAppend(entry);
            entryMetadataList[index] = entryMetadata;
            index++;
        }
        return entryMetadataList;
    }

    public AppendResult append(@Nonnull Session session, @Nonnull ByteBuffer... entries) throws IOException {
        if (entries.length > UserVersion.MAX_VALUE) {
            throw new TooManyEntriesException();
        }

        EntryMetadata[] entryMetadataList = appendEntries(entries);

        // Forces any updates to this channel's file to be written to the storage device that contains it.
        flushMutatedSegments(entryMetadataList);

        CompletableFuture<byte[]> future = writeMetadata(session, entryMetadataList);
        return new AppendResult(future, entryMetadataList, entryMetadataCache::put);
    }

    private byte[] packEntryKey(Versionstamp key) {
        return config.subspace().pack(Tuple.from(ENTRY_PREFIX, key));
    }

    private Segment getOrOpenSegmentByName(String name) throws IOException, SegmentNotFoundException {
        segmentsLock.readLock().lock();
        try {
            Segment segment = segmentsByName.get(name);
            if (segment != null) {
                return segment;
            }
        } finally {
            segmentsLock.readLock().unlock();
        }

        // Try to open the segment but check it first
        segmentsLock.writeLock().lock();
        try {
            Segment segment = segmentsByName.get(name);
            if (segment != null) {
                return segment;
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

    private EntryMetadata loadEntryMetadataFromCache(Versionstamp key) {
        try {
            return entryMetadataCache.get(key);
        } catch (CacheLoader.InvalidCacheLoadException e) {
            // The requested key doesn't exist in this Volume.
            return null;
        } catch (ExecutionException e) {
            throw new KronotopException("Failed to load entry metadata from FoundationDB", e);
        }
    }

    protected ByteBuffer getByEntryMetadata(Versionstamp key, EntryMetadata entryMetadata) throws IOException {
        Segment segment;
        try {
            segment = getOrOpenSegmentByName(entryMetadata.segment());
        } catch (SegmentNotFoundException e) {
            // Invalidate the cache and try again.
            // It will load the EntryMetadata from FoundationDB.
            // Possible cause: cleanup up filled segments.
            entryMetadataCache.invalidate(key);
            segment = getOrOpenSegmentByName(entryMetadata.segment());
        }

        return segment.get(entryMetadata.position(), entryMetadata.length());
    }

    public ByteBuffer get(Session session, @Nonnull Versionstamp key, boolean useCache) throws IOException {
        EntryMetadata entryMetadata;
        if (useCache) {
            entryMetadata = loadEntryMetadataFromCache(key);
            if (entryMetadata == null) {
                return null;
            }
        } else {
            byte[] value = session.getTransaction().get(packEntryKey(key)).join();
            if (value == null) {
                return null;
            }
            entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(value));
        }
        return getByEntryMetadata(key, entryMetadata);
    }

    public ByteBuffer get(@Nonnull Versionstamp key) throws IOException {
        return get(null, key, true);
    }

    public ByteBuffer get(@Nonnull Session session, @Nonnull Versionstamp key) throws IOException {
        return get(session, key, false);
    }

    public ByteBuffer[] getSegmentRange(String segmentName, SegmentRange[] segmentRanges) throws IOException {
        Segment segment = getOrOpenSegmentByName(segmentName);
        ByteBuffer[] entries = new ByteBuffer[segmentRanges.length];
        for (int i = 0; i < segmentRanges.length; i++) {
            SegmentRange segmentRange = segmentRanges[i];
            entries[i] = segment.get(segmentRange.position(), segmentRange.length());
        }
        return entries;
    }

    public DeleteResult delete(@Nonnull Session session, @Nonnull Versionstamp... keys) {
        Transaction tr = session.getTransaction();
        DeleteResult result = new DeleteResult(keys.length, entryMetadataCache::invalidate);
        int index = 0;
        for (Versionstamp key : keys) {
            byte[] entryKey = packEntryKey(key);
            byte[] encodedEntryMetadata = tr.get(entryKey).join();
            if (encodedEntryMetadata == null) {
                // Already deleted by a previously committed transaction.
                continue;
            }
            tr.clear(entryKey);
            tr.clear(packEntryMetadataKey(encodedEntryMetadata));

            EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(encodedEntryMetadata));
            mutateSegmentCardinality(tr, entryMetadata.segment(), SEGMENT_CARDINALITY_DECREASE_DELTA);
            mutateSegmentUsedBytes(tr, entryMetadata.segment(), encodeSegmentUsedBytes(-1 * entryMetadata.length()));

            appendSegmentLog(session, OperationKind.DELETE, entryMetadata);
            triggerWatchChangesSubscribers(tr);

            result.add(index, key);
            index++;
        }
        return result;
    }

    public UpdateResult update(@Nonnull Session session, @Nonnull KeyEntry... pairs) throws IOException, KeyNotFoundException {
        ByteBuffer[] entries = new ByteBuffer[pairs.length];
        for (int i = 0; i < pairs.length; i++) {
            entries[i] = pairs[i].entry();
        }
        EntryMetadata[] entryMetadataList = appendEntries(entries);
        flushMutatedSegments(entryMetadataList);

        Transaction tr = session.getTransaction();
        int index = 0;
        for (KeyEntry keyEntry : pairs) {
            Versionstamp key = keyEntry.key();
            byte[] packedKey = packEntryKey(key);
            byte[] encodedOldEntryMetadata = tr.get(packedKey).join();
            if (encodedOldEntryMetadata == null) {
                throw new KeyNotFoundException(key);
            }

            EntryMetadata entryMetadata = entryMetadataList[index];
            EntryMetadata oldEntryMetadata = EntryMetadata.decode(ByteBuffer.wrap(encodedOldEntryMetadata));
            if (!oldEntryMetadata.segment().equals(entryMetadata.segment())) {
                mutateSegmentCardinality(tr, oldEntryMetadata.segment(), SEGMENT_CARDINALITY_DECREASE_DELTA);
                mutateSegmentCardinality(tr, entryMetadata.segment(), SEGMENT_CARDINALITY_INCREASE_DELTA);
                mutateSegmentUsedBytes(tr, oldEntryMetadata.segment(), encodeSegmentUsedBytes(-1 * oldEntryMetadata.length()));
            } else {
                mutateSegmentUsedBytes(tr, entryMetadata.segment(), encodeSegmentUsedBytes(-1 * oldEntryMetadata.length()));
            }
            mutateSegmentUsedBytes(tr, entryMetadata.segment(), encodeSegmentUsedBytes(entryMetadata.length()));

            tr.clear(packEntryMetadataKey(encodedOldEntryMetadata));
            tr.set(packedKey, entryMetadata.encode().array());

            appendSegmentLog(session, OperationKind.DELETE, oldEntryMetadata);
            appendSegmentLog(session, OperationKind.APPEND, entryMetadata);
            triggerWatchChangesSubscribers(tr);

            index++;
        }
        return new UpdateResult(pairs, entryMetadataCache::invalidate);
    }

    public void flush(boolean metaData) {
        segmentsLock.readLock().lock();
        try {
            for (Map.Entry<String, Segment> entry : segmentsByName.entrySet()) {
                try {
                    entry.getValue().flush(metaData);
                } catch (IOException e) {
                    LOGGER.error("Failed to flush Segment: {}", entry.getKey(), e);
                }
            }
        } finally {
            segmentsLock.readLock().unlock();
        }
    }

    public void close() {
        isClosed = true;
        segmentsLock.readLock().lock();
        try {
            for (Map.Entry<String, Segment> entry : segmentsByName.entrySet()) {
                Segment segment = entry.getValue();
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

    public boolean isClosed() {
        return isClosed;
    }

    public Iterable<KeyEntry> getRange(@Nonnull Session session) {
        return new VolumeIterable(this, session, null, null);
    }

    public Iterable<KeyEntry> getRange(@Nonnull Session session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        return new VolumeIterable(this, session, begin, end);
    }

    private SegmentAnalysis analyzeSegment(Transaction tr, Segment segment) {
        byte[] segmentCardinalityKey = packSegmentCardinalityKey(segment.getName());
        byte[] segmentCardinalityData = tr.get(segmentCardinalityKey).join();
        int cardinality = decodeSegmentCardinality(segmentCardinalityData);

        byte[] segmentUsedBytesKey = packSegmentUsedBytesKey(segment.getName());
        byte[] segmentUsedBytesData = tr.get(segmentUsedBytesKey).join();
        long usedBytes = decodeSegmentUsedBytes(segmentUsedBytesData);

        return new SegmentAnalysis(segment.getName(), segment.getSize(), usedBytes, segment.getFreeBytes(), cardinality);
    }

    public List<SegmentAnalysis> analyze(Transaction tr) {
        // Create a read-only copy of segments to prevent acquiring segmentsLock for a long time.
        // Read-only access to the segments is not an issue. A segment can only be removed by the Vacuum daemon.
        List<Segment> readOnlySegments;
        List<SegmentAnalysis> result = new ArrayList<>();
        segmentsLock.readLock().lock();
        try {
            if (segments.isEmpty()) {
                return result;
            }
            readOnlySegments = new ArrayList<>(segments);
        } finally {
            segmentsLock.readLock().unlock();
        }
        for (Segment segment : readOnlySegments) {
            result.add(analyzeSegment(tr, segment));
        }
        return result;
    }

    public List<SegmentAnalysis> analyze() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return analyze(tr);
        }
    }

    protected void vacuumSegment(String name, long readVersion) throws IOException {
        Segment segment = getOrOpenSegmentByName(name);
        byte[] begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_PREFIX, segment.getName().getBytes()));
        byte[] end = ByteArrayUtil.strinc(begin);

        while (true) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                tr.setReadVersion(readVersion);

                Range range = new Range(begin, end);
                List<KeyEntry> pairs = new ArrayList<>();
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
                    ByteBuffer buffer = getByEntryMetadata(versionstampedKey, entryMetadata);
                    pairs.add(new KeyEntry(versionstampedKey, buffer));
                    if (pairs.size() >= SEGMENT_VACUUM_BATCH_SIZE) {
                        break;
                    }
                    begin = key;
                }
                if (pairs.isEmpty()) {
                    // End of the segment
                    break;
                }
                UpdateResult updateResult = update(new Session(tr), pairs.toArray(new KeyEntry[0]));
                updateResult.complete();
                tr.commit().join();
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
                begin = config.subspace().pack(Tuple.from(ENTRY_METADATA_PREFIX, segment.getName().getBytes()));
                LOGGER.error("Vacuum on Segment: {} has failed", segment.getName(), e);
            }
        }
    }

    private class EntryMetadataLoader extends CacheLoader<Versionstamp, EntryMetadata> {
        @Override
        public @Nonnull EntryMetadata load(@Nonnull Versionstamp key) {
            // See https://github.com/google/guava/wiki/CachesExplained#when-does-cleanup-happen
            return context.getFoundationDB().run(tr -> {
                byte[] value = tr.get(packEntryKey(key)).join();
                if (value == null) {
                    return null;
                }
                return EntryMetadata.decode(ByteBuffer.wrap(value));
            });
        }
    }
}
