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

package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;

import java.util.concurrent.ConcurrentHashMap;

public class DataSinkRegistry {
    private final ConcurrentHashMap<Integer, DataSink> sinks = new ConcurrentHashMap<>();

    public DataSink load(int parentNodeId) {
        return sinks.get(parentNodeId);
    }

    public DataSink loadOrCreateDocumentLocationSink(int parentNodeId) {
        return sinks.computeIfAbsent(parentNodeId, (ignored) -> new DocumentLocationSink(parentNodeId));
    }

    public DataSink loadOrCreatePersistedEntrySink(int parentNodeId) {
        return sinks.computeIfAbsent(parentNodeId, (ignored) -> new PersistedEntrySink(parentNodeId));
    }

    void writePersistedEntry(DataSink sink, Versionstamp versionstamp, PersistedEntry entry) {
        sink.match(
                buf -> {
                    buf.append(versionstamp, entry);
                    return null;
                },
                doc -> {
                    throw new IllegalStateException("This sink expects ByteBuffer");
                }
        );
    }

    void writeDocumentLocation(DataSink sink, long locationId, DocumentLocation location) {
        sink.match(
                buf -> {
                    throw new IllegalStateException("This sink expects DocumentLocation");
                },
                doc -> {
                    doc.append(locationId, location);
                    return null;
                }
        );
    }
}
