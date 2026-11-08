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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.VectorIndex;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Volume;
import com.kronotop.volume.blob.BlobChunkReadException;
import com.kronotop.volume.blob.BlobChunkReader;
import com.kronotop.volume.blob.CorruptBlobChunkException;
import com.kronotop.volume.blob.DirectoryBlobChunkSink;
import com.kronotop.volume.blob.IncompleteBlobException;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Restores flushed vector graph index files from Volume. A graph listed under {@code GRAPH_SYNCED} with no
 * {@code .complete} marker on the local disk is read back chunk by chunk, then the marker is created.
 * A graph whose chunks are corrupt, incomplete, missing or unreadable from Volume is logged, counted as a
 * restore failure and skipped. A skipped graph has no marker, so the next start reads it again. Any other
 * error, such as a local disk write failure or a FoundationDB error, stops the restore of the whole index.
 */
public final class VectorIndexVolumeRestore {
    static final int PAGE_SIZE = 1000;
    private static final Logger LOGGER = LoggerFactory.getLogger(VectorIndexVolumeRestore.class);

    private VectorIndexVolumeRestore() {
    }

    /**
     * Reads back from Volume every synced graph of the index that has no {@code .complete} marker on disk.
     *
     * @param context                  server context
     * @param metadata                 metadata of the bucket that owns the index
     * @param vectorIndex              the vector index
     * @param bucketDataDir            root of the bucket data directory
     * @param readChunksPerTransaction number of chunks read in one transaction
     * @throws IOException if the index directory cannot be created or a file cannot be written
     */
    public static void restore(
            Context context,
            BucketMetadata metadata,
            VectorIndex vectorIndex,
            Path bucketDataDir,
            int readChunksPerTransaction
    ) throws IOException {
        BucketService service = context.getService(BucketService.NAME);
        int shardId = VectorGraphIndexGroup.ownerShardId(metadata);
        Path indexDir = VectorGraphIndexGroup.resolveIndexDir(bucketDataDir, metadata, vectorIndex.definition().id());
        DirectorySubspace indexSubspace = vectorIndex.subspace();

        // (GRAPH_SYNCED, graphId) keys, one per graph that is fully written to Volume.
        byte[] syncedGraphsRange = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.GRAPH_SYNCED.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(syncedGraphsRange);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(syncedGraphsRange));
        Volume volume = null;
        while (true) {
            List<KeyValue> page;
            try (Transaction tr = context.getTransactionManager().create()) {
                page = tr.getRange(begin, end, PAGE_SIZE).asList().join();
            }
            for (KeyValue kv : page) {
                ObjectId graphId = new ObjectId(indexSubspace.unpack(kv.getKey()).getBytes(1));
                if (Files.exists(VectorGraphFiles.completeMarker(indexDir, graphId.toHexString()))) {
                    continue;
                }
                if (volume == null) {
                    Files.createDirectories(indexDir);
                    volume = service.getShard(shardId).volume();
                }
                restoreGraph(context, service, metadata, vectorIndex.definition().id(), volume, indexDir, graphId,
                        readChunksPerTransaction);
            }
            if (page.size() < PAGE_SIZE) {
                return;
            }
            begin = KeySelector.firstGreaterThan(page.getLast().getKey());
        }
    }

    private static void restoreGraph(
            Context context,
            BucketService service,
            BucketMetadata metadata,
            long indexId,
            Volume volume,
            Path indexDir,
            ObjectId graphId,
            int readChunksPerTransaction
    ) throws IOException {
        String baseName = graphId.toHexString();
        // A previous restore may have stopped halfway. Remove what it left behind.
        VectorGraphFiles.deleteFiles(indexDir, baseName);
        BlobChunkReader reader = new BlobChunkReader(volume, new Prefix(graphId.toByteArray()));
        try (DirectoryBlobChunkSink sink = new DirectoryBlobChunkSink(indexDir)) {
            Versionstamp cursor = null;
            do {
                try (Transaction tr = context.getTransactionManager().create()) {
                    cursor = reader.read(tr, cursor, readChunksPerTransaction, sink);
                }
            } while (cursor != null);
        } catch (CorruptBlobChunkException exp) {
            skip(service, metadata, indexId, graphId, indexDir, "chunk is corrupt", exp);
            return;
        } catch (IncompleteBlobException exp) {
            skip(service, metadata, indexId, graphId, indexDir, "chunks are incomplete", exp);
            return;
        } catch (BlobChunkReadException exp) {
            skip(service, metadata, indexId, graphId, indexDir, "chunks could not be read from Volume", exp);
            return;
        }

        if (!Files.exists(indexDir.resolve(baseName + VectorGraphFiles.INDEX_SUFFIX))
                || !Files.exists(indexDir.resolve(baseName + VectorGraphFiles.VMETA_SUFFIX))) {
            skip(service, metadata, indexId, graphId, indexDir, "chunks are missing", null);
            return;
        }
        Files.createFile(VectorGraphFiles.completeMarker(indexDir, baseName));
    }

    private static void skip(
            BucketService service,
            BucketMetadata metadata,
            long indexId,
            ObjectId graphId,
            Path indexDir,
            String reason,
            Exception exp
    ) throws IOException {
        LOGGER.error("Vector graph restore skipped, {}: namespace={}, bucket={}, indexId={}, graphId={}, dir={}",
                reason, metadata.namespace(), metadata.name(), indexId, graphId, indexDir, exp);
        service.getMetrics().incrementVectorRestoreFailures();
        // The sink is already closed. Remove the partial files so the next start reads the graph again.
        VectorGraphFiles.deleteFiles(indexDir, graphId.toHexString());
    }
}
