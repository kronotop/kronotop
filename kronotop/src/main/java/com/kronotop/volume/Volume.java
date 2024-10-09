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
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.kronotop.volume.Subspaces.ENTRY_METADATA_SUBSPACE;
import static com.kronotop.volume.Subspaces.VOLUME_WATCH_CHANGES_TRIGGER_SUBSPACE;

public class Volume {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0}; // 1, byte order: little-endian
    private static final byte[] DECREASE_BY_ONE_DELTA = new byte[]{-1, -1, -1, -1}; // -1, byte order: little-endian
    private static final int SEGMENT_VACUUM_BATCH_SIZE = 100;

    private final Context context;
    private final VolumeConfig config;
    private final VolumeSubspace subspace;
    private final ConcurrentHashMap<Long, LoadingCache<Versionstamp, EntryMetadata>> entryMetadataCache;
    private final byte[] watchChangesStageTriggerKey;

    // segmentsLock protects segments map
    private final ReadWriteLock segmentsLock = new ReentrantReadWriteLock();
    private final TreeMap<String, SegmentContainer> segments = new TreeMap<>();

    private volatile boolean isClosed;

    public Volume(Context context, VolumeConfig config) throws IOException {
        this.context = context;
        this.config = config;
        this.subspace = new VolumeSubspace(config.subspace());
        this.entryMetadataCache = new ConcurrentHashMap<>();
        this.watchChangesStageTriggerKey = this.config.subspace().pack(Tuple.from(VOLUME_WATCH_CHANGES_TRIGGER_SUBSPACE));
    }

    protected VolumeSubspace getSubspace() {
        return subspace;
    }

    public VolumeConfig getConfig() {
        return config;
    }

    private void triggerWatchChangesSubscribers(Transaction tr) {
        tr.mutate(MutationType.ADD, watchChangesStageTriggerKey, INCREASE_BY_ONE_DELTA);
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
        SegmentLog segmentLog = new SegmentLog(segment.getName(), config.subspace());
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.getName());
        segments.put(segment.getName(), new SegmentContainer(segment, segmentLog, segmentMetadata));
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
        SegmentLog segmentLog = new SegmentLog(segment.getName(), config.subspace());
        SegmentMetadata segmentMetadata = new SegmentMetadata(subspace, segment.getName());
        segments.put(segment.getName(), new SegmentContainer(segment, segmentLog, segmentMetadata));

        return segment;
    }

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

    private void appendSegmentLog(Session session, OperationKind kind, EntryMetadata entryMetadata) {
        appendSegmentLog(session.transaction(), kind, session.getAndIncrementUserVersion(), entryMetadata);
    }

    private void appendSegmentLog(Transaction tr, OperationKind kind, int userVersion, EntryMetadata entryMetadata) {
        segmentsLock.readLock().lock();
        try {
            SegmentContainer segmentContainer = segments.get(entryMetadata.segment());
            if (segmentContainer == null) {
                throw new IllegalStateException("Segment " + entryMetadata.segment() + " not found");
            }
            SegmentLogValue value = new SegmentLogValue(kind, entryMetadata.position(), entryMetadata.length());
            segmentContainer.log().append(tr, userVersion, value);
        } finally {
            segmentsLock.readLock().unlock();
        }
    }

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
            segmentContainer.metadata().addCardinality(tr, INCREASE_BY_ONE_DELTA);
            segmentContainer.metadata().addUsedBytes(tr, entryMetadata.length());

            appendSegmentLog(tr, OperationKind.APPEND, userVersion, entryMetadata);
            triggerWatchChangesSubscribers(tr);
        }
        return tr.getVersionstamp();
    }

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

    public AppendResult append(@Nonnull Session session, @Nonnull ByteBuffer... entries) throws IOException {
        if (entries.length > UserVersion.MAX_VALUE) {
            throw new TooManyEntriesException();
        }

        EntryMetadata[] entryMetadataList = appendEntries(session.prefix(), entries);

        // Forces any updates to this channel's file to be written to the storage device that contains it.
        flushMutatedSegments(entryMetadataList);

        CompletableFuture<byte[]> future = writeMetadata(session, entryMetadataList);
        return new AppendResult(future, entryMetadataList, getEntryMetadataCache(session.prefix())::put);
    }

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

    private LoadingCache<Versionstamp, EntryMetadata> getEntryMetadataCache(Prefix prefix) {
        return entryMetadataCache.computeIfAbsent(prefix.asLong(), prefixId -> CacheBuilder.
                newBuilder().
                expireAfterAccess(30, TimeUnit.MINUTES).
                build(new EntryMetadataCacheLoader(context, subspace, prefix))
        );
    }

    private EntryMetadata loadEntryMetadataFromCache(Prefix prefix, Versionstamp key) {
        try {
            return getEntryMetadataCache(prefix).get(key);
        } catch (CacheLoader.InvalidCacheLoadException e) {
            // The requested key doesn't exist in this Volume.
            return null;
        } catch (ExecutionException e) {
            throw new KronotopException("Failed to load entry metadata from FoundationDB", e);
        }
    }

    protected ByteBuffer getByEntryMetadata(Prefix prefix, Versionstamp key, EntryMetadata entryMetadata) throws IOException {
        Segment segment;
        try {
            segment = getOrOpenSegmentByName(entryMetadata.segment());
        } catch (SegmentNotFoundException e) {
            // Invalidate the cache and try again.
            // It will load the EntryMetadata from FoundationDB.
            // Possible cause: cleanup up filled segments.
            getEntryMetadataCache(prefix).invalidate(key);
            segment = getOrOpenSegmentByName(entryMetadata.segment());
        }

        return segment.get(entryMetadata.position(), entryMetadata.length());
    }

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
        Transaction tr = session.transaction();

        DeleteResult result = new DeleteResult(keys.length, getEntryMetadataCache(session.prefix())::invalidate);

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
            segmentContainer.metadata().addCardinality(tr, DECREASE_BY_ONE_DELTA);
            segmentContainer.metadata().addUsedBytes(tr, -1 * entryMetadata.length());

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
        EntryMetadata[] entryMetadataList = appendEntries(session.prefix(), entries);
        flushMutatedSegments(entryMetadataList);

        Transaction tr = session.transaction();
        int index = 0;
        for (KeyEntry keyEntry : pairs) {
            Versionstamp key = keyEntry.key();
            byte[] packedKey = subspace.packEntryKey(session.prefix(), key);
            byte[] encodedOldEntryMetadata = tr.get(packedKey).join();
            if (encodedOldEntryMetadata == null) {
                throw new KeyNotFoundException(key);
            }

            EntryMetadata entryMetadata = entryMetadataList[index];
            SegmentContainer segmentContainer = segments.get(entryMetadata.segment());

            EntryMetadata oldEntryMetadata = EntryMetadata.decode(ByteBuffer.wrap(encodedOldEntryMetadata));
            SegmentContainer oldSegmentContainer = segments.get(oldEntryMetadata.segment());

            if (!oldEntryMetadata.segment().equals(entryMetadata.segment())) {
                oldSegmentContainer.metadata().addCardinality(tr, DECREASE_BY_ONE_DELTA);
                segmentContainer.metadata().addCardinality(tr, INCREASE_BY_ONE_DELTA);
                oldSegmentContainer.metadata().addUsedBytes(tr, -1 * oldEntryMetadata.length());
            } else {
                segmentContainer.metadata().addUsedBytes(tr, -1 * entryMetadata.length());
            }
            segmentContainer.metadata().addUsedBytes(tr, entryMetadata.length());

            tr.clear(subspace.packEntryMetadataKey(encodedOldEntryMetadata));
            tr.set(packedKey, entryMetadata.encode().array());

            appendSegmentLog(session, OperationKind.DELETE, oldEntryMetadata);
            appendSegmentLog(session, OperationKind.APPEND, entryMetadata);
            triggerWatchChangesSubscribers(tr);

            index++;
        }
        return new UpdateResult(pairs, getEntryMetadataCache(session.prefix())::invalidate);
    }

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

    public boolean isClosed() {
        return isClosed;
    }

    public Iterable<KeyEntry> getRange(@Nonnull Session session) {
        return new VolumeIterable(this, session, null, null);
    }

    public Iterable<KeyEntry> getRange(@Nonnull Session session, VersionstampedKeySelector begin, VersionstampedKeySelector end) {
        return new VolumeIterable(this, session, begin, end);
    }

    private SegmentAnalysis analyzeSegment(Transaction tr, SegmentContainer segmentContainer) {
        int cardinality = segmentContainer.metadata().getCardinality(tr);
        long usedBytes = segmentContainer.metadata().getUsedBytes(tr);
        Segment segment = segmentContainer.segment();
        return new SegmentAnalysis(segment.getName(), segment.getSize(), usedBytes, segment.getFreeBytes(), cardinality);
    }

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

    public List<SegmentAnalysis> analyze() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return analyze(tr);
        }
    }

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
}
