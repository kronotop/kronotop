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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.IndexTypeMismatchException;
import com.kronotop.bucket.index.*;
import com.kronotop.volume.*;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Executes document update operations on buckets, applying both SET and UNSET operations
 * while maintaining index consistency across affected and unaffected indexes.
 * <p>
 * The UpdateExecutor performs the following workflow:
 * <ol>
 *   <li>Executes the pipeline to identify matching documents</li>
 *   <li>Groups documents by shard ID for efficient batch processing</li>
 *   <li>Applies SET operations (field updates/additions) and UNSET operations (field removals)</li>
 *   <li>Updates documents in the Volume storage layer</li>
 *   <li>Updates affected indexes with new/removed field values</li>
 *   <li>Updates unaffected indexes with new metadata only</li>
 * </ol>
 * <p>
 * Index consistency is maintained by:
 * <ul>
 *   <li><b>Affected indexes</b>: Indexes on fields being SET or UNSET - entries are dropped and recreated</li>
 *   <li><b>Unaffected indexes</b>: Indexes on unchanged fields - only metadata is updated</li>
 * </ul>
 */
public final class UpdateExecutor extends BaseExecutor implements Executor<List<Versionstamp>> {
    private final boolean strictTypes;
    private final PipelineExecutor executor;

    public UpdateExecutor(Context context, PipelineExecutor executor) {
        this.strictTypes = context.getConfig().getBoolean("bucket.index.strict_types");
        this.executor = executor;
    }

    /**
     * Executes document update operations within the provided transaction context.
     * <p>
     * This method orchestrates the entire update process by:
     * <ol>
     *   <li>Running the pipeline execution to find matching documents</li>
     *   <li>Grouping found documents by shard for batch processing</li>
     *   <li>Applying SET and UNSET operations from the query context</li>
     *   <li>Updating documents in the volume storage</li>
     *   <li>Maintaining index consistency for both affected and unaffected indexes</li>
     * </ol>
     *
     * @param tr  the FoundationDB transaction for atomic operations
     * @param ctx the query context containing the pipeline plan, update operations (SET/UNSET),
     *            and bucket metadata including index definitions
     * @return a list of versionstamps representing the successfully updated documents
     * @throws UncheckedIOException if volume I/O operations fail
     * @throws KronotopException    if document retrieval fails or update consistency is violated
     */
    @Override
    public List<Versionstamp> execute(Transaction tr, QueryContext ctx) {
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
            Map<Integer, List<Versionstamp>> byShardId = accumulateAndGroupVersionstampsByShardId(sink);

            Map<Versionstamp, UpdateResultContainer> updateResultContainers = new LinkedHashMap<>();

            for (Map.Entry<Integer, List<Versionstamp>> versionstampGroupByShard : byShardId.entrySet()) {
                int shardId = versionstampGroupByShard.getKey();
                BucketShard shard = ctx.env().bucketService().getShard(shardId);

                VolumeSession session = new VolumeSession(tr, ctx.metadata().volumePrefix());

                List<KeyEntry> updatedEntries = new ArrayList<>();
                for (Versionstamp versionstamp : versionstampGroupByShard.getValue()) {
                    ByteBuffer document = shard.volume().get(session, versionstamp);
                    if (document == null) {
                        throw new KeyNotFoundException(versionstamp);
                    }

                    BSONUpdateUtil.DocumentUpdateResult setResult = BSONUpdateUtil.applyUpdateOperations(
                            document,
                            ctx.options().update().setOps(),
                            ctx.options().update().unsetOps()
                    );
                    KeyEntry updatedEntry = new KeyEntry(versionstamp, setResult.document());
                    updatedEntries.add(updatedEntry);
                    updateResultContainers.put(versionstamp, new UpdateResultContainer(shardId, setResult));
                }

                // Update the documents at Volume level
                UpdateResult updateResult = shard.volume().update(session, updatedEntries.toArray(new KeyEntry[0]));
                ctx.registerPostCommitHook(new PostCommitHook(updateResult));

                setUpdatedEntryMetadata(updateResultContainers, updateResult.entries());
            }

            updateAffectedIndexes(ctx, tr, updateResultContainers);
            updateUnaffectedIndexes(ctx, tr, updateResultContainers);

            return updateResultContainers.keySet().stream().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (KeyNotFoundException e) {
            throw new KronotopException("Failed to update bucket entry", e);
        } finally {
            sink.clear();
        }
    }

    /**
     * Updates the metadata for each updated entry in the provided array by using the corresponding update result container.
     * This method ensures that each updated entry is properly reflected in the update result containers.
     *
     * @param updateResultContainers a map containing the association between version stamps and update result containers,
     *                               where each container holds metadata and update-related information
     * @param updatedEntries         an array of updated entry objects containing version stamps and encoded metadata
     *                               to be used for metadata update
     * @throws IllegalStateException if no corresponding update result container is found for a given updated entry
     */
    private void setUpdatedEntryMetadata(Map<Versionstamp, UpdateResultContainer> updateResultContainers, UpdatedEntry[] updatedEntries) {
        for (UpdatedEntry updatedEntry : updatedEntries) {
            UpdateResultContainer container = updateResultContainers.get(updatedEntry.versionstamp());
            if (container == null) {
                throw new IllegalStateException("Update result could not be found");
            }
            container.setEntryMetadata(updatedEntry.encodedMetadata());
        }
    }

    /**
     * Updates metadata for indexes that are unaffected by the current set of update operations
     * in the query context. For each unaffected index, entry metadata is updated based on the
     * provided transaction and update result containers.
     * <p>
     * Iterates over index selectors from the query context metadata to identify indexes that are
     * not targeted by the update operations. For these indexes, metadata entries are updated using
     * the defined index subspace and the result containers. This ensures that unaffected indexes
     * maintain consistency without altering their main data structure.
     *
     * @param ctx                    the query context containing metadata, execution state, and configuration options
     * @param tr                     the current transaction object facilitating the atomic updates within a single transaction scope
     * @param updateResultContainers a map of version stamps to update result containers used to
     *                               generate index entries and update metadata for unaffected indexes
     */
    private void updateUnaffectedIndexes(QueryContext ctx, Transaction tr, Map<Versionstamp, UpdateResultContainer> updateResultContainers) {
        for (Index index : ctx.metadata().indexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
            // Skip the default ID index as it'll be handled separately
            if (index.definition().equals(DefaultIndexDefinition.ID)) {
                continue;
            }
            String selector = index.definition().selector();
            if (ctx.options().update().setOps().containsKey(selector) || ctx.options().update().unsetOps().contains(selector)) {
                continue;
            }
            for (Map.Entry<Versionstamp, UpdateResultContainer> entry : updateResultContainers.entrySet()) {
                DirectorySubspace unaffectedIndexSubspace = index.subspace();
                Versionstamp versionstamp = entry.getKey();
                UpdateResultContainer updateResultContainer = entry.getValue();
                IndexEntry indexEntry = new IndexEntry(updateResultContainer.getShardId(), updateResultContainer.getEntryMetadata());
                IndexBuilder.updateEntryMetadata(tr, versionstamp, indexEntry.encode(), unaffectedIndexSubspace);
            }
        }
    }

    /**
     * Updates the indexes that are affected by the current set of update operations
     * in the query context. For each affected index, entries are updated or dropped
     * based on the provided transaction and update result containers.
     * <p>
     * The method processes each entry in the update result containers and updates
     * the primary index, removes stale entries, and sets new index entries for the
     * affected indexes. This ensures that the indexes remain consistent with the
     * latest update operations applied to the data.
     *
     * @param ctx                    the query context containing metadata, execution state, and configuration options
     * @param tr                     the current transaction object facilitating the atomic updates within a single transaction scope
     * @param updateResultContainers a map of version stamps to update result containers used to
     *                               update or remove index entries and ensure consistency of affected indexes
     */
    private void updateAffectedIndexes(QueryContext ctx, Transaction tr, Map<Versionstamp, UpdateResultContainer> updateResultContainers) {
        for (Map.Entry<Versionstamp, UpdateResultContainer> updatedEntry : updateResultContainers.entrySet()) {
            Versionstamp versionstamp = updatedEntry.getKey();
            UpdateResultContainer updateResultContainer = updatedEntry.getValue();
            IndexBuilder.updatePrimaryIndex(
                    tr,
                    versionstamp,
                    ctx.metadata(),
                    updateResultContainer.getShardId(),
                    updateResultContainer.getEntryMetadata()
            );
            dropStaleEntriesOnAffectedIndexes(ctx, tr, versionstamp, updateResultContainer);
            setIndexEntriesOnAffectedIndexes(ctx, tr, versionstamp, updateResultContainer);
        }
    }

    /**
     * Sets new index entries for the indexes affected by the current update operation
     * in the specified query context. For each affected index, the method generates
     * and updates the index entry based on the provided transaction, versionstamp,
     * and update result container.
     * <p>
     * This method processes the updated values, identifies affected indexes, and
     * builds the corresponding index entry. The entries are then set in the transaction
     * to ensure consistency with the current update operation on the primary data.
     *
     * @param ctx                   the query context containing metadata, execution state, and configuration options
     * @param tr                    the current transaction object facilitating atomic updates within a single transaction scope
     * @param versionstamp          the versionstamp representing the unique version of the current transactional operation
     * @param updateResultContainer the container holding the results of the update operation, including new and old values,
     *                              entry metadata, and shard information
     */
    private void setIndexEntriesOnAffectedIndexes(QueryContext ctx, Transaction tr, Versionstamp versionstamp, UpdateResultContainer updateResultContainer) {
        Map<String, BsonValue> newValues = updateResultContainer.getDocumentUpdateResult().newValues();
        for (Map.Entry<String, BsonValue> newValue : newValues.entrySet()) {
            String selector = newValue.getKey();
            Index index = ctx.metadata().indexes().getIndex(selector, IndexSelectionPolicy.READ);
            if (index == null) {
                continue;
            }
            DirectorySubspace affectedIndex = index.subspace();
            IndexDefinition indexDefinition = index.definition();

            BsonValue bsonValue = updateResultContainer.getDocumentUpdateResult().newValues().get(selector);
            if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                Object indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                if (indexValue == null) {
                    if (!strictTypes) {
                        // Type mismatch, drop the value and continue
                        IndexBuilder.dropIndexEntry(tr, versionstamp, indexDefinition, affectedIndex, ctx.metadata().subspace());
                        continue;
                    }
                    throw new IndexTypeMismatchException(index.definition(), bsonValue);
                }
                IndexEntryContainer indexEntryContainer = new IndexEntryContainer(
                        ctx.metadata(),
                        indexValue,
                        indexDefinition,
                        affectedIndex,
                        updateResultContainer.getShardId(),
                        updateResultContainer.getEntryMetadata()
                );
                IndexBuilder.setIndexEntryByVersionstamp(tr, versionstamp, indexEntryContainer);
            }
        }
    }

    /**
     * Removes stale index entries from the indexes affected by the current update operation
     * in the provided query context. This method identifies the relevant indexes based on
     * the updated values in the update result container and removes outdated entries.
     *
     * @param ctx                   the query context containing metadata, execution state, and configuration options
     * @param tr                    the current transaction object facilitating atomic updates within a single transaction scope
     * @param versionstamp          the versionstamp representing the unique version of the current transactional operation
     * @param updateResultContainer the container holding the results of the update operation, including new and old values,
     *                              entry metadata, and shard information
     */
    private void dropStaleEntriesOnAffectedIndexes(QueryContext ctx, Transaction tr, Versionstamp versionstamp, UpdateResultContainer updateResultContainer) {
        Set<String> matchedSelectors = new HashSet<>();
        matchedSelectors.addAll(updateResultContainer.getDocumentUpdateResult().newValues().keySet());
        matchedSelectors.addAll(updateResultContainer.getDocumentUpdateResult().droppedSelectors());
        for (String selector : matchedSelectors) {
            Index index = ctx.metadata().indexes().getIndex(selector, IndexSelectionPolicy.READ);
            if (index == null) {
                continue;
            }
            DirectorySubspace affectedIndex = index.subspace();
            IndexDefinition indexDefinition = index.definition();
            IndexBuilder.dropIndexEntry(tr, versionstamp, indexDefinition, affectedIndex, ctx.metadata().subspace());
        }
    }

    /**
     * A container class for holding the results of update operations performed on documents.
     * The container encapsulates the shard identifier, the result of the document update operation,
     * and metadata associated with the update operation.
     */
    private static final class UpdateResultContainer {
        private final int shardId;
        private final BSONUpdateUtil.DocumentUpdateResult documentUpdateResult;
        private byte[] entryMetadata;

        UpdateResultContainer(int shardId, BSONUpdateUtil.DocumentUpdateResult documentUpdateResult) {
            this.documentUpdateResult = documentUpdateResult;
            this.shardId = shardId;
        }

        public BSONUpdateUtil.DocumentUpdateResult getDocumentUpdateResult() {
            return documentUpdateResult;
        }

        public byte[] getEntryMetadata() {
            return entryMetadata;
        }

        public void setEntryMetadata(byte[] entryMetadata) {
            this.entryMetadata = entryMetadata;
        }

        public int getShardId() {
            return shardId;
        }
    }

    private record PostCommitHook(UpdateResult updateResult) implements CommitHook {
        @Override
        public void run() {
            updateResult.complete();
        }
    }
}
