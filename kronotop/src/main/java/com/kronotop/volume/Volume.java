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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Volume {
    private static final Logger LOGGER = LoggerFactory.getLogger(Volume.class);
    private final Context context;
    private final VolumeConfig config;
    private final VolumeMetadata metadata = new VolumeMetadata();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<Segment> segments = new ArrayList<>();

    protected Volume(Context context, VolumeConfig volumeConfig) {
        this.context = context;
        this.config = volumeConfig;
    }

    private Segment createSegment() {
        // Create a new segment and add it to the metadata
        long segmentId = metadata.getAndIncrementSegmentId();
        Segment segment = new Segment(context, segmentId);
        metadata.addSegment(segmentId);

        // Update the volume metadata on FoundationDB
        context.getFoundationDB().run(tr -> {
            try {
                byte[] value = new ObjectMapper().writeValueAsBytes(metadata);
                byte[] metadataKey = config.subspace().pack(Tuple.from("volume"));
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

    private Segment getLatestSegment(int size) {
        try {
            Segment latest = segments.getLast();
            if (size > latest.getMetadata().getFreeBytes()) {
                return createSegment();
            }
            return latest;
        } catch (NoSuchElementException e) {
            return createSegment();
        }
    }

    public List<Versionstamp> append(@Nonnull List<byte[]> values) {
        lock.writeLock().lock();
        try {
            int size = 0;
            for (byte[] value : values) {
                size += value.length;
            }
            Segment segment = getLatestSegment(size);
            System.out.println(segment.getMetadata().getId());
        } finally {
            lock.writeLock().unlock();
        }
        return null;
    }
}
