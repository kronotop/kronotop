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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.client.protocol.SegmentRange;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeNames;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Retrieves documents from local or remote storage with batched, locality-optimized fetching.
 * Groups documents by (shardId, segmentId) to minimize disk I/O and network calls.
 */
public class DocumentRetriever {
    private static final int MAX_CONCURRENT_FETCHES = Math.min(8, Runtime.getRuntime().availableProcessors());
    private final Context context;
    private final RoutingService routing;
    private final BucketService service;

    public DocumentRetriever(BucketService service) {
        this.service = service;
        this.context = service.getContext();
        this.routing = service.getContext().getService(RoutingService.NAME);
    }

    /**
     * Groups document locations by (shardId, segmentId) for batched retrieval.
     * Returns a nested map: shardId -> segmentId -> list of original indices.
     */
    private static @NonNull Int2ObjectMap<Long2ObjectMap<IntArrayList>> getGroups(List<DocumentLocation> locations, int size) {
        Int2ObjectMap<Long2ObjectMap<IntArrayList>> groups = new Int2ObjectOpenHashMap<>();
        for (int i = 0; i < size; i++) {
            DocumentLocation loc = locations.get(i);
            int shardId = loc.shardId();
            long segmentId = loc.entryMetadata().segmentId();

            Long2ObjectMap<IntArrayList> bySegment = groups.get(shardId);
            if (bySegment == null) {
                bySegment = new Long2ObjectOpenHashMap<>();
                groups.put(shardId, bySegment);
            }

            IntArrayList indices = bySegment.get(segmentId);
            if (indices == null) {
                indices = new IntArrayList(4);
                bySegment.put(segmentId, indices);
            }
            indices.add(i);
        }
        return groups;
    }

    /**
     * Extracts document location information from a regular index scan entry.
     * For regular indexes, the key structure is [ENTRIES_MAGIC, indexed_value, Versionstamp]
     *
     * @param indexSubspace the index directory subspace
     * @param indexEntry    the key-value pair from the index scan
     * @return document location containing ID, shard, and metadata
     */
    DocumentLocation extractDocumentLocationFromIndexScan(DirectorySubspace indexSubspace, KeyValue indexEntry) {
        // Extract the Versionstamp from the index key
        Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
        Versionstamp documentId;

        // Handle different index structures:
        // Regular indexes: (ENTRIES_MAGIC, indexed_value, Versionstamp) - 3 elements
        // _id index: (ENTRIES_MAGIC, Versionstamp) - 2 elements (Versionstamp is both indexed value and document ID)
        if (indexKeyTuple.size() == 3) {
            documentId = (Versionstamp) indexKeyTuple.get(2); // Regular index: get Versionstamp from position 2
        } else if (indexKeyTuple.size() == 2) {
            documentId = (Versionstamp) indexKeyTuple.get(1); // _id index: get Versionstamp from position 1
        } else {
            throw new IllegalStateException("Unexpected index key tuple size: " + indexKeyTuple.size());
        }

        // Decode the IndexEntry from the value
        IndexEntry indexEntryData = IndexEntry.decode(indexEntry.getValue());
        int shardId = indexEntryData.shardId();
        EntryMetadata entryMetadata = EntryMetadata.decode(indexEntryData.entryMetadata());

        return new DocumentLocation(documentId, shardId, entryMetadata);
    }

    /**
     * Retrieves multiple documents, grouping by shard and segment for locality optimization.
     * Fetches locally or remotely based on routing. Uses single-threaded execution for a single
     * group, otherwise spawns virtual threads with bounded concurrency.
     *
     * @param metadata  bucket metadata containing volume prefix
     * @param locations document locations to retrieve
     * @return documents in the same order as input locations
     */
    List<ByteBuffer> retrieveDocuments(
            BucketMetadata metadata,
            List<DocumentLocation> locations
    ) {
        int size = locations.size();
        if (size == 0) return List.of();

        Int2ObjectMap<Long2ObjectMap<IntArrayList>> groups = getGroups(locations, size);

        ByteBuffer[] result = new ByteBuffer[size];

        // Collect all fetch tasks
        List<Runnable> tasks = new ArrayList<>();

        for (var shardEntry : groups.int2ObjectEntrySet()) {
            int shardId = shardEntry.getIntKey();
            var bySegment = shardEntry.getValue();

            Route route = routing.findRoute(ShardKind.BUCKET, shardId);
            Member primary = route.primary();
            boolean isLocal = primary.equals(context.getMember());

            for (var segmentEntry : bySegment.long2ObjectEntrySet()) {
                long segmentId = segmentEntry.getLongKey();
                IntArrayList indices = segmentEntry.getValue();

                Group group = new Group(shardId, segmentId, indices);

                if (groups.size() == 1 && bySegment.size() == 1) {
                    // switch to the single-thread
                    if (isLocal) {
                        fetchLocal(metadata, locations, group, result);
                    } else {
                        fetchRemote(primary, locations, group, result);
                    }
                } else {
                    if (isLocal) {
                        tasks.add(() -> fetchLocal(metadata, locations, group, result));
                    } else {
                        tasks.add(() -> fetchRemote(primary, locations, group, result));
                    }
                }
            }
        }

        if (!tasks.isEmpty()) {
            // Execute tasks in batches of MAX_CONCURRENT_FETCHES using virtual threads
            executeInBatches(tasks);
        }

        return Arrays.asList(result);
    }

    /**
     * Executes tasks in batches using virtual threads with bounded concurrency.
     * Waits for each batch to complete before starting the next. Propagates exceptions fail-fast.
     */
    private void executeInBatches(List<Runnable> tasks) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < tasks.size(); i += MAX_CONCURRENT_FETCHES) {
                int end = Math.min(i + MAX_CONCURRENT_FETCHES, tasks.size());
                List<Future<?>> futures = new ArrayList<>(end - i);

                for (int j = i; j < end; j++) {
                    futures.add(executor.submit(tasks.get(j)));
                }

                for (Future<?> f : futures) {
                    try {
                        f.get(); // fail-fast
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        cancelAll(futures);
                        throw new KronotopException("Interrupted while fetching documents", e);
                    } catch (ExecutionException e) {
                        cancelAll(futures);
                        Throwable cause = e.getCause();
                        if (cause instanceof RuntimeException re) {
                            throw re;
                        }
                        throw new KronotopException("Error fetching documents", cause);
                    }
                }
            }
        }
    }

    /**
     * Cancels all pending futures in the list.
     */
    private void cancelAll(List<Future<?>> futures) {
        for (Future<?> f : futures) {
            f.cancel(true);
        }
    }

    /**
     * Fetches documents from local volume storage for a given (shard, segment) group.
     */
    private void fetchLocal(
            BucketMetadata metadata,
            List<DocumentLocation> locations,
            Group group,
            ByteBuffer[] result
    ) {
        BucketShard shard = service.getShard(group.shardId);
        if (shard == null) {
            throw new RuntimeException("Shard not found for ID: " + group.shardId);
        }

        for (int i = 0; i < group.indices.size; i++) {
            int idx = group.indices.a[i];
            DocumentLocation loc = locations.get(idx);
            try {
                result[idx] = shard.volume().get(metadata.volumePrefix(), loc.versionstamp(), loc.entryMetadata());
            } catch (IOException e) {
                throw new KronotopException(
                        "Failed to retrieve document with ID: " + loc.versionstamp() + " from shard: " + loc.shardId(), e
                );
            }
        }
    }

    /**
     * Fetches documents from a remote node via SEGMENT.RANGE protocol for a given (shard, segment) group.
     */
    private void fetchRemote(
            Member primary,
            List<DocumentLocation> locations,
            Group group,
            ByteBuffer[] result
    ) {
        String volumeName = VolumeNames.format(ShardKind.BUCKET, group.shardId);

        // Build segment ranges for this group
        List<SegmentRange> ranges = new ArrayList<>(group.indices.size);
        for (int i = 0; i < group.indices.size; i++) {
            int idx = group.indices.a[i];
            DocumentLocation loc = locations.get(idx);
            EntryMetadata em = loc.entryMetadata();
            ranges.add(new SegmentRange(em.position(), em.length()));
        }

        if (Thread.currentThread().isInterrupted()) {
            throw new CancellationException();
        }

        // Fetch from a remote node (use byte array codec to get raw bytes)
        StatefulInternalConnection<byte[], byte[]> conn = context.getInternalClientPool().getByteArray(primary);
        List<Object> chunks = conn.sync().segmentrange(volumeName, group.segmentId, ranges);

        // Place results at correct positions
        for (int i = 0; i < group.indices.size; i++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new CancellationException();
            }
            int idx = group.indices.a[i];
            Object chunk = chunks.get(i);
            if (chunk instanceof byte[] data) {
                result[idx] = ByteBuffer.wrap(data);
            } else {
                throw new KronotopException("SEGMENT.RANGE returned invalid chunk type for index " + i);
            }
        }
    }

    /**
     * Primitive int list to avoid boxing overhead when tracking indices.
     */
    static final class IntArrayList {
        int[] a;
        int size;

        IntArrayList(int cap) {
            a = new int[cap];
        }

        void add(int v) {
            if (size == a.length) {
                a = Arrays.copyOf(a, size << 1);
            }
            a[size++] = v;
        }
    }

    /**
     * Represents a batch of documents sharing the same (shardId, segmentId) for grouped fetching.
     */
    static final class Group {
        final int shardId;
        final long segmentId;
        final IntArrayList indices;

        Group(int shardId, long segmentId, IntArrayList indices) {
            this.shardId = shardId;
            this.segmentId = segmentId;
            this.indices = indices;
        }
    }
}