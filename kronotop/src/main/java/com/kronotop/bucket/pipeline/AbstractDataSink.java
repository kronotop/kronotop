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

package com.kronotop.bucket.pipeline;

import org.bson.types.ObjectId;

import java.util.*;

/**
 * Not thread-safe. Designed for single-threaded pipeline execution.
 */
public abstract class AbstractDataSink<V extends DocumentRef> {
    private final int parentNodeId;

    private final List<V> sink = new ArrayList<>();
    private final List<V> unmodifiableView = Collections.unmodifiableList(sink);

    public AbstractDataSink(int parentNodeId) {
        this.parentNodeId = parentNodeId;
    }

    public int parentNodeId() {
        return parentNodeId;
    }

    public void append(V value) {
        sink.add(value);
    }

    public int size() {
        return sink.size();
    }

    public void clear() {
        sink.clear();
    }

    public List<V> entries() {
        return unmodifiableView;
    }

    protected List<V> mutableEntries() {
        return sink;
    }

    public void trimTo(int size) {
        List<V> list = mutableEntries();
        if (list.size() > size) {
            list.subList(size, list.size()).clear();
        }
    }

    /**
     * Removes duplicate entries by ObjectId while preserving the original order.
     * Keeps the first occurrence.
     */
    public void dedupByObjectId() {
        List<V> list = mutableEntries();
        int size = list.size();
        if (size <= 1) {
            return;
        }

        Set<ObjectId> seen = new HashSet<>(size * 2);

        int write = 0;
        for (int i = 0; i < size; i++) {
            V current = list.get(i);
            if (seen.add(current.objectId())) {
                list.set(write++, current);
            }
        }

        if (write < size) {
            list.subList(write, size).clear();
        }
    }
}
