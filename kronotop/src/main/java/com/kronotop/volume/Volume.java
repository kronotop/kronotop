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
import com.kronotop.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Volume {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private static final byte ENTRY_PREFIX = 0x01;
    private final Context context;
    private final VolumeConfig config;
    private final VolumeMetadata metadata = new VolumeMetadata();
    // segmentsLock protects segments array
    private final ReadWriteLock segmentsLock = new ReentrantReadWriteLock();
    private final List<Segment> segments = new ArrayList<>();

    protected Volume(Context context, VolumeConfig volumeConfig) {
        this.context = context;
        this.config = volumeConfig;
    }

    private Segment createSegment() throws IOException {
        // Create a new segment and add it to the metadata
        long segmentId = metadata.getAndIncrementSegmentId();
        Segment segment = new Segment(context, segmentId);
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

    private List<Versionstamp> competeResponse(CompletableFuture<byte[]> versionstamp, int size) {
        byte[] trVersion = versionstamp.join();
        List<Versionstamp> result = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            result.add(Versionstamp.complete(trVersion, i));
        }
        return result;
    }

    private CompletableFuture<byte[]> writeMetadata(Transaction tr, List<EntryMetadata> entryMetadataList) {
        for (int i = 0; i < entryMetadataList.size(); i++) {
            Tuple key = Tuple.from(ENTRY_PREFIX, Versionstamp.incomplete(i));
            EntryMetadata entryMetadata = entryMetadataList.get(i);
            byte[] encodedEntryMetadata = entryMetadata.encode().array();
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_KEY,
                    config.subspace().packWithVersionstamp(key),
                    encodedEntryMetadata
            );
            tr.mutate(
                    MutationType.SET_VERSIONSTAMPED_VALUE,
                    config.subspace().pack(Tuple.from("entry-metadata", encodedEntryMetadata)),
                    Tuple.from(Versionstamp.incomplete(i)).packWithVersionstamp()
            );
        }
        return tr.getVersionstamp();
    }

    private List<EntryMetadata> appendEntries(ByteBuffer[] entries) throws IOException {
        List<EntryMetadata> entryMetadataList = new ArrayList<>();
        for (ByteBuffer entry : entries) {
            EntryMetadata entryMetadata = tryAppend(entry);
            entryMetadataList.add(entryMetadata);
        }
        return entryMetadataList;
    }

    public List<Versionstamp> append(@Nonnull ByteBuffer[] entries) throws IOException {
        List<EntryMetadata> entryMetadataList = appendEntries(entries);
        CompletableFuture<byte[]> versionstamp = context.getFoundationDB().run(tr -> writeMetadata(tr, entryMetadataList));
        return competeResponse(versionstamp, entries.length);
    }

    public byte[] get(@Nonnull Versionstamp key) throws IOException {
        context.getFoundationDB().run(tr -> {
            byte[] value = tr.get(config.subspace().pack(Tuple.from(ENTRY_PREFIX, key))).join();
            System.out.println(EntryMetadata.decode(ByteBuffer.wrap(value)));
            return null;
        });
        return null;
    }
}
