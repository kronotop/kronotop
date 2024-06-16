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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Volume {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private static final byte ENTRY_PREFIX = 0x01;
    private static final byte ENTRY_METADATA_PREFIX = 0x02;

    private final Context context;
    private final VolumeConfig config;
    private final VolumeMetadata metadata = new VolumeMetadata();
    private final LoadingCache<Versionstamp, EntryMetadata> entryMetadataCache;

    // segmentsLock protects segments array
    private final ReadWriteLock segmentsLock = new ReentrantReadWriteLock();
    private final List<Segment> segments = new ArrayList<>();
    private final HashMap<String, Segment> segmentsByName = new HashMap<>();

    protected Volume(Context context, VolumeConfig volumeConfig) {
        this.context = context;
        this.config = volumeConfig;
        this.entryMetadataCache = CacheBuilder.newBuilder()
                .expireAfterAccess(30, TimeUnit.MINUTES)
                .build(new EntryMetadataLoader());
    }

    private Segment createSegment() throws IOException {
        // Create a new segment and add it to the metadata
        long segmentId = metadata.getAndIncrementSegmentId();
        Segment segment = new Segment(context, segmentId);

        // After this point, the Segment has been created on the physical medium.

        metadata.addSegment(segmentId);
        // Update the volume metadata on FoundationDB
        context.getFoundationDB().run(tr -> {
            try {
                byte[] value = new ObjectMapper().writeValueAsBytes(metadata);
                byte[] metadataKey = config.subspace().pack(Tuple.from("volume-metadata"));
                tr.set(metadataKey, value);
            } catch (JsonProcessingException e) {
                LOGGER.error("Error writing to JSON", e);
                throw new RuntimeException(e); // retry
            }
            return null;
        });

        // Make it available for the rest of the Volume.
        segments.add(segment);
        segmentsByName.put(segment.getName(), segment);
        return segment;
    }

    private Segment getOrCreateLatestSegment(int size) throws IOException {
        segmentsLock.writeLock().lock();
        try {
            if (segments.isEmpty()) {
                return createSegment();
            }
            Segment latest = segments.getLast();
            if (size > latest.getMetadata().getFreeBytes()) {
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
            if (size < latest.getMetadata().getFreeBytes()) {
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
        int size = entry.position();
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

    private byte[] packEntryKeyWithVersionstamp(int userVersion) {
        Tuple key = Tuple.from(ENTRY_PREFIX, Versionstamp.incomplete(userVersion));
        return config.subspace().packWithVersionstamp(key);
    }

    private byte[] packEntryMetadataKey(byte[] data) {
        return config.subspace().pack(Tuple.from(ENTRY_METADATA_PREFIX, data));
    }

    private CompletableFuture<byte[]> writeMetadata(Transaction tr, EntryMetadata[] entryMetadataList) {
        int userVersion = 0;
        for (EntryMetadata entryMetadata : entryMetadataList) {
            byte[] encodedEntryMetadata = entryMetadata.encode().array();
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_KEY,
                    packEntryKeyWithVersionstamp(userVersion),
                    encodedEntryMetadata
            );
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_VALUE,
                    packEntryMetadataKey(encodedEntryMetadata),
                    Tuple.from(Versionstamp.incomplete(userVersion)).packWithVersionstamp()
            );
            userVersion++;
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

    public AppendResult append(@Nonnull Transaction tr, @Nonnull ByteBuffer... entries) throws IOException {
        EntryMetadata[] entryMetadataList = appendEntries(entries);
        CompletableFuture<byte[]> future = writeMetadata(tr, entryMetadataList);
        return new AppendResult(future, entryMetadataList, entryMetadataCache::put);
    }

    private byte[] packEntryKey(Versionstamp key) {
        return config.subspace().pack(Tuple.from(ENTRY_PREFIX, key));
    }

    private Segment getSegmentByName(String name) throws SegmentNotFoundException {
        segmentsLock.readLock().lock();
        try {
            Segment segment = segmentsByName.get(name);
            if (segment == null) {
                throw new SegmentNotFoundException(name);
            }
            return segment;
        } finally {
            segmentsLock.readLock().unlock();
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

    private ByteBuffer getByEntryMetadata(Versionstamp key, EntryMetadata entryMetadata) throws SegmentNotFoundException, IOException {
        Segment segment;
        try {
            segment = getSegmentByName(entryMetadata.segment());
        } catch (SegmentNotFoundException e) {
            // Invalidate the cache and try again.
            // It will load the EntryMetadata from FoundationDB.
            // Possible cause: cleanup up filled segments.
            entryMetadataCache.invalidate(key);
            segment = getSegmentByName(entryMetadata.segment());
        }

        return segment.get(entryMetadata.position(), entryMetadata.length());
    }

    public ByteBuffer get(Transaction tr, @Nonnull Versionstamp key, boolean useCache) throws SegmentNotFoundException, IOException {
        EntryMetadata entryMetadata;
        if (useCache) {
            entryMetadata = loadEntryMetadataFromCache(key);
            if (entryMetadata == null) {
                return null;
            }
        } else {
            byte[] value = tr.get(packEntryKey(key)).join();
            if (value == null) {
                return null;
            }
            entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(value));
        }
        return getByEntryMetadata(key, entryMetadata);
    }

    public ByteBuffer get(@Nonnull Versionstamp key) throws SegmentNotFoundException, IOException {
        return get(null, key, true);
    }

    public ByteBuffer get(@Nonnull Transaction tr, @Nonnull Versionstamp key) throws SegmentNotFoundException, IOException {
        return get(tr, key, false);
    }

    public DeleteResult delete(@Nonnull Transaction tr, @Nonnull Versionstamp... keys) {
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
            result.add(index, key);
            index++;
        }
        return result;
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
