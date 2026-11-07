/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.vector;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.index.VectorIndexMaintainer;
import com.kronotop.volume.*;
import com.kronotop.volume.blob.BlobChunkWriter;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;


public class VectorSyncMaintainer extends BaseVectorMaintainer<VectorSyncTask> {
    private static final String[] EXTENSIONS = {".index", ".vmeta", ".pqv"};
    private final int chunkSize;
    private final int writeChunksPerTransaction;

    public VectorSyncMaintainer(Context context) {
        super(context, "sync", "kr.vector-sync-maintainer-%d",
                context.getConfig().getLong("bucket.vector.sync_drain_timeout_seconds"));
        this.chunkSize = context.getConfig().getInt("bucket.vector.sync_chunk_size_bytes");
        this.writeChunksPerTransaction = context.getConfig().getInt("bucket.vector.sync_write_chunks_per_transaction");
    }

    @Override
    protected BlockingQueue<VectorSyncTask> queue() {
        return service.getVectorSyncQueue();
    }

    private void clearAndRegisterPrefix(Volume volume, Prefix prefix, PrefixEntry entry) {
        retry().executeRunnable(() -> {
            try (Transaction tr = context.getTransactionManager().create()) {
                volume.clearPrefix(new VolumeSession(tr, prefix));
                PrefixUtil.unregister(context, tr, entry.key(), prefix);
                PrefixUtil.register(context, tr, prefix, entry);
                tr.commit().join();
            }
        });
    }

    private void writeChunks(
            BlobChunkWriter writer,
            int from,
            int to,
            Consumer<Transaction> finalizer
    ) {
        retry().executeRunnable(() -> {
            try (Transaction tr = context.getTransactionManager().create()) {
                for (int i = from; i < to; i++) {
                    writer.write(tr, i);
                }
                if (finalizer != null) {
                    finalizer.accept(tr);
                }
                tr.commit().join();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private void writeFile(
            Volume volume,
            Prefix prefix,
            Path path,
            boolean lastFile,
            Consumer<Transaction> finalizer
    ) throws IOException {
        String name = path.getFileName().toString();
        try (BlobChunkWriter writer = new BlobChunkWriter(volume, prefix, path, name, chunkSize)) {
            int count = writer.chunkCount();
            for (int from = 0; from < count; from += writeChunksPerTransaction) {
                int to = Math.min(from + writeChunksPerTransaction, count);
                boolean finalBatch = lastFile && to == count;
                writeChunks(writer, from, to, finalBatch ? finalizer : null);
            }
        }
    }

    private List<Path> listFiles(Path indexDir, ObjectId indexId) {
        List<Path> files = new ArrayList<>();
        for (String extension : EXTENSIONS) {
            Path path = indexDir.resolve(indexId.toHexString() + extension);
            if (Files.exists(path)) {
                files.add(path);
            }
        }
        return files;
    }

    @Override
    protected void process(VectorSyncTask task) throws IOException {
        VectorGraphIndexGroup group = service.getVectorGraphRegistry().get(
                task.namespace(),
                task.bucket(),
                task.vectorIndexId()
        );
        if (group == null) {
            LOGGER.warn("Vector sync task skipped, no index group for namespace={}, bucket={}, indexId={}",
                    task.namespace(), task.bucket(), task.vectorIndexId());
            return;
        }

        Path indexDir = group.resolveIndexDir(service.getBucketDataDir());

        List<Path> files = listFiles(indexDir, task.onDiskIndexId());
        if (files.isEmpty()) {
            // The files are gone from the local disk. A retry cannot bring them back, so the task is dropped.
            LOGGER.error("Vector sync task dropped, flushed index files are missing: indexId={}, dir={}",
                    task.onDiskIndexId(), indexDir);
            service.getMetrics().incrementVectorSyncFailures();
            return;
        }

        BucketMetadata metadata = group.getBucketMetadata();
        int shardId = VectorGraphIndexGroup.ownerShardId(metadata);

        Volume volume = service.getShard(shardId).volume();
        ObjectId indexId = task.onDiskIndexId();
        Prefix prefix = new Prefix(indexId.toByteArray());
        DirectorySubspace indexSubspace = group.getVectorIndex().subspace();
        PrefixEntry entry = new PrefixEntry(
                VectorIndexMaintainer.graphPrefixBindingKey(indexSubspace, indexId),
                EntryKind.VECTOR_INDEX,
                indexId.toByteArray()
        );

        // The prefix is registered before the first chunk. If the sync stops halfway and the index is
        // dropped, the binding key goes away with the index subspace, the prefix turns stale and the
        // chunks are reclaimed.
        clearAndRegisterPrefix(volume, prefix, entry);

        Consumer<Transaction> finalizer = tr -> VectorIndexMaintainer.markGraphSynced(tr, indexSubspace, indexId);
        for (int i = 0; i < files.size(); i++) {
            writeFile(volume, prefix, files.get(i), i == files.size() - 1, finalizer);
        }
    }

    @Override
    protected void onFailure(VectorSyncTask task, Exception exp) {
        LOGGER.error("Vector sync task failed", exp);
        service.getMetrics().incrementVectorSyncFailures();
        // In drain mode a failed task is dropped, so the drain can finish. The next start syncs it again.
        if (task != null && !drainMode) {
            service.getVectorSyncQueue().add(task);
        }
    }
}
