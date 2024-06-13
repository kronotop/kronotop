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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Volume {
    private final Context context;
    private final Path rootPath;
    private final VolumeConfig config;
    private final Database database;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<Segment> segments = new ArrayList<>();

    protected Volume(Context context, Path rootPath, VolumeConfig volumeConfig) {
        this.context = context;
        this.rootPath = rootPath;
        this.config = volumeConfig;
        this.database = context.getFoundationDB();
    }

    private Segment getLatestSegment(int size) {
        try {
            Segment latest = segments.getLast();
            if (size > latest.getMetadata().getFreeBytes()) {
                Segment newSegment = new Segment(context, rootPath);
                segments.add(newSegment);
                return newSegment;
            }
            return latest;
        } catch (NoSuchElementException e) {
            Segment newSegment = new Segment(context, rootPath);
            segments.add(newSegment);
            return newSegment;
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
        } finally {
            lock.writeLock().unlock();
        }
        return null;
    }
}
