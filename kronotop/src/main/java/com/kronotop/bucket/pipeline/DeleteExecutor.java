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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.Context;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.index.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executor responsible for performing document deletion operations in the Kronotop Cluster.
 *
 * <p>The DeleteExecutor processes a query pipeline to identify documents to be deleted,
 * then executes the actual deletion operations across multiple shards.
 */
public final class DeleteExecutor extends BaseExecutor implements Executor<List<ObjectId>> {
    private final Context context;
    private final PipelineExecutor executor;
    private final VolumeFacade volumeFacade;
    private final Map<Integer, VolumeSubspace> subspaces = new ConcurrentHashMap<>();

    public DeleteExecutor(Context context, PipelineExecutor executor) {
        this.context = context;
        this.executor = executor;
        this.volumeFacade = new VolumeFacade(context);
    }

    @Override
    public List<ObjectId> execute(Transaction tr, QueryContext ctx) {
        executor.execute(tr, ctx);

        PipelineNode head = findHeadNode(ctx.plan());

        if (head == null) {
            return List.of();
        }

        DataSink sink = ctx.sinks().load(head.id());
        if (sink == null) {
            return List.of();
        }

        try {
            Map<Integer, List<DocumentRef>> byShardId = accumulateDocumentRefsByShardId(ctx, sink);

            Index primaryIndex = ctx.metadata().indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);

            List<ObjectId> deletedIds = new ArrayList<>();
            List<Versionstamp> versionstamps = new ArrayList<>();
            for (Map.Entry<Integer, List<DocumentRef>> item : byShardId.entrySet()) {
                int shardId = item.getKey();

                BucketShard shard = ctx.env().bucketService().getShard(shardId);
                VolumeSession session = new VolumeSession(tr, ctx.metadata().prefix());

                List<DocumentRef> docRefs = item.getValue();
                VersionstampedEntry[] entries = new VersionstampedEntry[docRefs.size()];
                for (int i = 0; i < docRefs.size(); i++) {
                    DocumentRef docRef = docRefs.get(i);
                    Versionstamp versionstamp = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, docRef.objectId().toByteArray(), ctx.metadata());
                    entries[i] = new VersionstampedEntry(versionstamp, docRef.entryMetadata());
                    deletedIds.add(docRef.objectId());
                    versionstamps.add(versionstamp);
                }

                if (shard != null) {
                    DeleteResult deleteResult = shard.volume().deleteByVersionstampedEntry(session, entries);
                    PostCommitHook postCommitHook = new PostCommitHook(deleteResult);
                    TransactionUtil.addPostCommitHook(postCommitHook, ctx.getSession());
                } else {
                    VolumeSubspace subspace = subspaces.computeIfAbsent(shardId, (ignored) -> {
                        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.BUCKET, shardId);
                        return new VolumeSubspace(generator.openVolumeSubspace());
                    });
                    volumeFacade.deleteByVersionstampedEntry(subspace, session, entries);
                }
            }

            // Drop index entries for all deleted documents
            for (Index index : ctx.metadata().indexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                // Skip the default ID index as it'll be handled separately
                if (PrimaryIndex.isPrimary(index.definition())) {
                    continue;
                }
                for (ObjectId objectId : deletedIds) {
                    SingleFieldIndexMaintainer.dropEntry(
                            tr, objectId.toByteArray(), index.definition(), index.subspace(), ctx.metadata().subspace()
                    );
                }
            }

            // Drop compound index entries for all deleted documents
            for (CompoundIndex compoundIndex : ctx.metadata().compoundIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                for (ObjectId objectId : deletedIds) {
                    CompoundIndexMaintainer.dropEntry(
                            tr,
                            objectId.toByteArray(),
                            compoundIndex.definition(),
                            compoundIndex.subspace(),
                            ctx.metadata().subspace()
                    );
                }
            }

            // Drop vector index entries for all deleted documents
            for (VectorIndex vectorIndex : ctx.metadata().vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                List<DeletedVector> deletedVectors = new ArrayList<>();
                for (ObjectId objectId : deletedIds) {
                    VectorIndexMaintainer.dropEntry(tr, objectId.toByteArray(), vectorIndex, ctx.metadata());
                    int userVersion = ctx.getAndIncrementUserVersion();
                    VectorIndexMaintainer.deleteMutationLog(
                            tr,
                            vectorIndex.subspace(),
                            objectId.toByteArray(),
                            userVersion
                    );
                    deletedVectors.add(new DeletedVector(objectId, userVersion));
                }
                // Schedule on-heap graph node deletion after commit
                TransactionUtil.addPostCommitHook(new VectorNodeDeleteHook(
                        ctx.env().bucketService(),
                        ctx.metadata(),
                        vectorIndex.definition().id(),
                        deletedVectors,
                        tr.getVersionstamp()
                ), ctx.getSession());
            }

            for (int i = 0; i < deletedIds.size(); i++) {
                ObjectId objectId = deletedIds.get(i);
                Versionstamp versionstamp = versionstamps.get(i);
                PrimaryIndexMaintainer.dropEntry(tr, objectId.toByteArray(), versionstamp, primaryIndex, ctx.metadata());
            }

            return deletedIds;
        } finally {
            sink.clear();
        }
    }

    private record PostCommitHook(DeleteResult deleteResult) implements CommitHook {
        @Override
        public void run() {
            deleteResult.complete();
        }
    }

}
