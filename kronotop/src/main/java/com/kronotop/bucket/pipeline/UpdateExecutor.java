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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ast.BqlAnd;
import com.kronotop.bucket.bql.ast.BqlEq;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.vector.CollectedVector;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.*;
import org.bson.*;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes document update operations on buckets using ObjectId as the document identifier.
 */
public final class UpdateExecutor extends BaseExecutor implements Executor<List<ObjectId>> {
    private final Context context;
    private final boolean strictTypes;
    private final PipelineExecutor executor;
    private final VolumeFacade volumeFacade;
    private final Map<Integer, VolumeSubspace> subspaces = new ConcurrentHashMap<>();

    public UpdateExecutor(Context context, PipelineExecutor executor) {
        this.context = context;
        this.strictTypes = context.getConfig().getBoolean("bucket.index.strict_types");
        this.executor = executor;
        this.volumeFacade = new VolumeFacade(context);
    }

    /**
     * Collects the document paths modified by the update, with positional operators normalized
     * to match index selectors.
     */
    private static Set<String> collectAffectedPaths(QueryContext ctx) {
        Set<String> affectedPaths = new HashSet<>();
        for (String key : ctx.options().update().setOps().keySet()) {
            affectedPaths.add(BSONUpdateUtil.normalizeSelector(key));
        }
        for (String key : ctx.options().update().unsetOps()) {
            affectedPaths.add(BSONUpdateUtil.normalizeSelector(key));
        }
        return affectedPaths;
    }

    /**
     * Checks whether an op path and an index selector touch overlapping document subtrees.
     */
    private static boolean pathsOverlap(String opPath, String selector) {
        return opPath.equals(selector)
                || selector.startsWith(opPath + ".")
                || opPath.startsWith(selector + ".");
    }

    private static boolean isSelectorAffected(Set<String> affectedPaths, String selector) {
        for (String opPath : affectedPaths) {
            if (pathsOverlap(opPath, selector)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isVectorIndexAffected(VectorIndex vectorIndex, Set<String> affectedPaths) {
        return isSelectorAffected(affectedPaths, vectorIndex.definition().selector());
    }

    private static boolean isCompoundIndexAffected(CompoundIndex compoundIndex, Set<String> affectedPaths) {
        for (CompoundIndexField field : compoundIndex.definition().fields()) {
            if (isSelectorAffected(affectedPaths, field.selector())) {
                return true;
            }
        }
        return false;
    }

    private void handleNoMatch(Transaction tr, QueryContext ctx) {
        if (ctx.options().update().upsert()) {
            try {
                executeUpsert(tr, ctx);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public List<ObjectId> execute(Transaction tr, QueryContext ctx) {
        executor.execute(tr, ctx);

        PipelineNode head = findHeadNode(ctx.plan());

        if (head == null) {
            handleNoMatch(tr, ctx);
            return List.of();
        }

        DataSink sink = ctx.sinks().load(head.id());
        if (sink == null) {
            handleNoMatch(tr, ctx);
            return List.of();
        }

        try {
            Map<Integer, List<DocumentRef>> byShardId = accumulateDocumentRefsByShardId(ctx, sink);

            if (byShardId.isEmpty()) {
                handleNoMatch(tr, ctx);
                return List.of();
            }

            // Pre-parse query for $ positional operator
            BqlExpr parsedQuery = null;
            Set<String> positionalFields = ctx.options().update().positionalFields();
            if (!positionalFields.isEmpty()) {
                if (ctx.queryBytes() == null) {
                    throw new IllegalArgumentException("Positional operator $ requires a query filter");
                }
                parsedQuery = BqlParser.parse(ctx.queryBytes());
            }

            Map<ObjectId, UpdateResultContainer> updateResultContainers = new LinkedHashMap<>();

            // Shared across all shard groups, so two documents updated to the same unique value in one
            // command conflict even when they live on different shards (their index entries are not
            // written yet, so only the in-memory batch set can see the collision).
            UniquenessEnforcer uniquenessEnforcer =
                    new UniquenessEnforcer(ctx.metadata(), ctx.env().collatorCache(), strictTypes);

            for (Map.Entry<Integer, List<DocumentRef>> documentRefGroupByShard : byShardId.entrySet()) {
                int shardId = documentRefGroupByShard.getKey();
                BucketShard shard = ctx.env().bucketService().getShard(shardId);

                if (shard != null) {
                    handleLocalUpdate(
                            tr, ctx, shard, shardId,
                            documentRefGroupByShard.getValue(), parsedQuery, positionalFields,
                            updateResultContainers, uniquenessEnforcer
                    );
                } else {
                    handleRemoteUpdate(tr, ctx, shardId,
                            documentRefGroupByShard.getValue(), parsedQuery, positionalFields,
                            updateResultContainers, uniquenessEnforcer
                    );
                }
            }

            Set<String> affectedPaths = collectAffectedPaths(ctx);
            updateAffectedIndexes(ctx, tr, updateResultContainers, affectedPaths);
            updateUnaffectedIndexes(ctx, tr, updateResultContainers, affectedPaths);

            return new ArrayList<>(updateResultContainers.keySet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (KeyNotFoundException | VersionstampAlreadyExistsException e) {
            throw new KronotopException("Failed to update bucket entry", e);
        } finally {
            sink.clear();
        }
    }

    /**
     * Resolves positional operators against the query and applies the update's set/unset operations
     * to a single document. Shared by the local and remote update paths.
     *
     * @throws IllegalStateException if a positional field has no array element matching the query
     */
    private BSONUpdateUtil.DocumentUpdateResult applyUpdateOperations(QueryContext ctx, ByteBuffer document,
                                                                      BqlExpr parsedQuery, Set<String> positionalFields) {
        Map<String, Integer> matchedPositions = Map.of();
        if (parsedQuery != null) {
            matchedPositions = PositionalMatchFinder.findMatchedPositions(document, parsedQuery, positionalFields);
            for (String field : positionalFields) {
                if (!matchedPositions.containsKey(field)) {
                    throw new IllegalStateException(
                            "No array element in '" + field + "' matches the query condition");
                }
            }
        }

        return BSONUpdateUtil.applyUpdateOperations(
                document,
                ctx.options().update().setOps(),
                ctx.options().update().unsetOps(),
                ctx.options().update().arrayFilters(),
                matchedPositions
        );
    }

    /**
     * Updates documents residing on a shard owned by this node. Applies update operations in-place
     * via {@code Volume.updateByVersionstampedEntryUpdate} without changing the document's shard assignment.
     */
    private void handleLocalUpdate(
            Transaction tr,
            QueryContext ctx,
            BucketShard shard,
            int shardId,
            List<DocumentRef> docRefs,
            BqlExpr parsedQuery,
            Set<String> positionalFields,
            Map<ObjectId, UpdateResultContainer> updateResultContainers,
            UniquenessEnforcer uniquenessEnforcer
    ) throws IOException, KeyNotFoundException {
        SingleFieldIndex primaryIndex = ctx.metadata().singleFieldIndexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
        VolumeSession session = new VolumeSession(tr, ctx.metadata().prefix());
        UniquenessChecker uniquenessChecker = new UniquenessChecker(tr);
        List<VersionstampedEntryUpdate> updatedEntries = new ArrayList<>();
        List<ObjectId> objectIds = new ArrayList<>();

        for (DocumentRef docRef : docRefs) {
            ByteBuffer document = shard.volume().getByEntryMetadata(docRef.entryMetadata());
            if (document == null) {
                throw new KeyNotFoundException(docRef.entryMetadata());
            }

            BSONUpdateUtil.DocumentUpdateResult setResult = applyUpdateOperations(ctx, document, parsedQuery, positionalFields);
            if (!setResult.changed()) {
                continue;
            }
            uniquenessEnforcer.enqueue(uniquenessChecker, docRef.objectId().toByteArray(), setResult.document());
            Versionstamp versionstamp = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, docRef.objectId().toByteArray(), ctx.metadata());
            VersionstampedEntryUpdate updatedEntry = new VersionstampedEntryUpdate(versionstamp, docRef.entryMetadata(), setResult.document());
            updatedEntries.add(updatedEntry);
            objectIds.add(docRef.objectId());
            updateResultContainers.put(docRef.objectId(), new UpdateResultContainer(shardId, setResult));
        }

        if (updatedEntries.isEmpty()) {
            return;
        }

        // Drain all uniqueness reads before touching the volume so a violation leaves no appended bytes.
        uniquenessChecker.await();

        UpdateResult updateResult = shard.volume().updateByVersionstampedEntryUpdate(session, updatedEntries.toArray(new VersionstampedEntryUpdate[0]));
        TransactionUtil.addPostCommitHook(new PostCommitHook(updateResult), ctx.getSession());

        for (VersionstampedEntryUpdate entry : updatedEntries) {
            entry.entry().rewind();
        }

        setUpdatedEntryMetadata(objectIds, updateResultContainers, updateResult.entries());
    }

    /**
     * Updates documents that live on a remote shard. The updated documents are inserted into a
     * local shard and the originals are deleted from the remote volume, so the update also migrates
     * them to this node. Each result container carries the new local shard metadata so index entries
     * can be rewritten afterward.
     */
    private void handleRemoteUpdate(
            Transaction tr,
            QueryContext ctx,
            int remoteShardId,
            List<DocumentRef> docRefs,
            BqlExpr parsedQuery,
            Set<String> positionalFields,
            Map<ObjectId, UpdateResultContainer> updateResultContainers,
            UniquenessEnforcer uniquenessEnforcer
    ) throws IOException, VersionstampAlreadyExistsException {
        SingleFieldIndex primaryIndex = ctx.metadata().singleFieldIndexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);

        // Fetch remote document bodies via DocumentRetriever
        List<DocumentLocation> locations = new ArrayList<>(docRefs.size());
        for (DocumentRef docRef : docRefs) {
            locations.add(new DocumentLocation(docRef.objectId(), remoteShardId, docRef.entryMetadata()));
        }
        List<ByteBuffer> documents = ctx.env().documentRetriever().retrieveDocuments(locations);

        VolumeSubspace remoteSubspace = subspaces.computeIfAbsent(remoteShardId, (ignored) -> {
            VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.BUCKET, remoteShardId);
            return new VolumeSubspace(generator.openVolumeSubspace());
        });

        // Pick a local shard from the bucket's shard list
        BucketShard localShard = findLocalBucketShard(ctx);

        VolumeSession session = new VolumeSession(tr, ctx.metadata().prefix());
        UniquenessChecker uniquenessChecker = new UniquenessChecker(tr);

        // First pass: apply the update operations and enqueue uniqueness checks. No volume writes
        // happen yet, so the reads pipeline in parallel, and a violation aborts before any insert.
        record ChangedDocument(DocumentRef docRef, BSONUpdateUtil.DocumentUpdateResult setResult) {
        }
        List<ChangedDocument> changed = new ArrayList<>();
        for (int i = 0; i < docRefs.size(); i++) {
            DocumentRef docRef = docRefs.get(i);
            ByteBuffer document = documents.get(i);

            BSONUpdateUtil.DocumentUpdateResult setResult = applyUpdateOperations(ctx, document, parsedQuery, positionalFields);
            if (!setResult.changed()) {
                continue;
            }

            uniquenessEnforcer.enqueue(uniquenessChecker, docRef.objectId().toByteArray(), setResult.document());
            changed.add(new ChangedDocument(docRef, setResult));
        }

        // Drain all uniqueness reads once before touching any volume.
        uniquenessChecker.await();

        // Second pass: insert into the local shard and delete the remote original.
        for (ChangedDocument entry : changed) {
            DocumentRef docRef = entry.docRef();
            BSONUpdateUtil.DocumentUpdateResult setResult = entry.setResult();

            Versionstamp versionstamp = PrimaryIndexMaintainer.getVolumePointer(tr, primaryIndex, docRef.objectId().toByteArray(), ctx.metadata());
            KeyEntry keyEntry = new KeyEntry(versionstamp, setResult.document());
            InsertResult insertResult = localShard.volume().insert(session, keyEntry);
            TransactionUtil.addPostCommitHook(new InsertPostCommitHook(insertResult), ctx.getSession());

            // Delete from remote volume
            VersionstampedEntry versionstampedEntry = new VersionstampedEntry(versionstamp, docRef.entryMetadata());
            volumeFacade.deleteByVersionstampedEntry(remoteSubspace, session, versionstampedEntry);

            // Populate UpdateResultContainer with the local shard's metadata
            InsertedEntry inserted = insertResult.entries()[0];
            UpdateResultContainer container = new UpdateResultContainer(localShard.id(), setResult);
            container.setEntryMetadata(inserted.encodedMetadata());
            container.setVersionstamp(inserted.versionstamp());
            updateResultContainers.put(docRef.objectId(), container);
        }
    }

    private BucketShard findLocalBucketShard(QueryContext ctx) {
        for (int shardId : ctx.metadata().shards()) {
            BucketShard shard = ctx.env().bucketService().getShard(shardId);
            if (shard != null) {
                return shard;
            }
        }
        throw new IllegalStateException("No local shard found in the bucket's shard list");
    }

    private void setUpdatedEntryMetadata(List<ObjectId> objectIds, Map<ObjectId, UpdateResultContainer> updateResultContainers, UpdatedEntry[] updatedEntries) {
        for (int i = 0; i < updatedEntries.length; i++) {
            ObjectId objectId = objectIds.get(i);
            UpdateResultContainer container = updateResultContainers.get(objectId);
            if (container == null) {
                throw new IllegalStateException("Update result could not be found");
            }
            container.setEntryMetadata(updatedEntries[i].encodedMetadata());
            container.setVersionstamp(updatedEntries[i].versionstamp());
        }
    }

    private void updateUnaffectedIndexes(QueryContext ctx, Transaction tr, Map<ObjectId, UpdateResultContainer> updateResultContainers, Set<String> affectedPaths) {
        for (SingleFieldIndex index : ctx.metadata().singleFieldIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (PrimaryIndex.isPrimary(index.definition())) {
                continue;
            }
            if (isSelectorAffected(affectedPaths, index.definition().selector())) {
                continue;
            }
            for (Map.Entry<ObjectId, UpdateResultContainer> entry : updateResultContainers.entrySet()) {
                DirectorySubspace unaffectedIndexSubspace = index.subspace();
                ObjectId objectId = entry.getKey();
                UpdateResultContainer updateResultContainer = entry.getValue();
                IndexEntry indexEntry = new IndexEntry(updateResultContainer.getShardId(), updateResultContainer.getEntryMetadata());
                SingleFieldIndexMaintainer.updateIndexEntry(
                        tr,
                        objectId.toByteArray(),
                        indexEntry.encode(),
                        unaffectedIndexSubspace
                );
            }
        }

        // Update unaffected compound indexes
        for (CompoundIndex compoundIndex : ctx.metadata().compoundIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (isCompoundIndexAffected(compoundIndex, affectedPaths)) {
                continue;
            }
            for (Map.Entry<ObjectId, UpdateResultContainer> entry : updateResultContainers.entrySet()) {
                ObjectId objectId = entry.getKey();
                UpdateResultContainer container = entry.getValue();
                IndexEntry indexEntry = new IndexEntry(container.getShardId(), container.getEntryMetadata());
                CompoundIndexMaintainer.updateIndexEntry(
                        tr,
                        objectId.toByteArray(),
                        indexEntry.encode(),
                        compoundIndex.subspace(),
                        compoundIndex.definition().fields().size()
                );
            }
        }

        // Update unaffected vector indexes (metadata only, preserve vector)
        for (VectorIndex vectorIndex : ctx.metadata().vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (isVectorIndexAffected(vectorIndex, affectedPaths)) {
                continue;
            }
            for (Map.Entry<ObjectId, UpdateResultContainer> entry : updateResultContainers.entrySet()) {
                ObjectId objectId = entry.getKey();
                UpdateResultContainer container = entry.getValue();
                VectorIndexMaintainer.updateIndexEntry(
                        tr,
                        objectId.toByteArray(),
                        vectorIndex,
                        container.getShardId(),
                        container.getEntryMetadata()
                );
            }
        }
    }

    private void updateAffectedIndexes(QueryContext ctx, Transaction tr, Map<ObjectId, UpdateResultContainer> updateResultContainers, Set<String> affectedPaths) {
        // Validate vector fields that were modified
        for (VectorIndex vi : ctx.metadata().vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (!isVectorIndexAffected(vi, affectedPaths)) {
                continue;
            }
            for (UpdateResultContainer container : updateResultContainers.values()) {
                BsonValue newValue = SelectorMatcher.match(vi.definition().selector(), container.getDocumentUpdateResult().document());
                if (newValue != null) {
                    VectorIndexUtil.validateVectorField(vi.definition(), newValue);
                }
            }
        }

        SingleFieldIndex primaryIndex = ctx.metadata().singleFieldIndexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
        for (Map.Entry<ObjectId, UpdateResultContainer> updatedEntry : updateResultContainers.entrySet()) {
            ObjectId objectId = updatedEntry.getKey();
            byte[] objectIdBytes = objectId.toByteArray();
            UpdateResultContainer container = updatedEntry.getValue();
            PrimaryIndexMaintainer.updateIndexEntry(
                    tr,
                    objectIdBytes,
                    primaryIndex,
                    container.getShardId(),
                    container.getEntryMetadata()
            );
            for (SingleFieldIndex index : ctx.metadata().singleFieldIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                if (PrimaryIndex.isPrimary(index.definition())) {
                    continue;
                }
                if (!isSelectorAffected(affectedPaths, index.definition().selector())) {
                    continue;
                }
                refreshSingleFieldIndexEntry(ctx, tr, objectIdBytes, container, index);
            }
        }

        // Handle affected compound indexes: drop old entries and set new ones
        for (CompoundIndex compoundIndex : ctx.metadata().compoundIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (!isCompoundIndexAffected(compoundIndex, affectedPaths)) {
                continue;
            }
            for (Map.Entry<ObjectId, UpdateResultContainer> updatedEntry : updateResultContainers.entrySet()) {
                ObjectId objectId = updatedEntry.getKey();
                byte[] objectIdBytes = objectId.toByteArray();
                UpdateResultContainer container = updatedEntry.getValue();

                // Drop old entries
                CompoundIndexMaintainer.dropEntry(
                        tr, objectIdBytes, compoundIndex.definition(),
                        compoundIndex.subspace(), ctx.metadata().subspace()
                );

                // Extract new values from the updated document and set new entries
                ByteBuffer updatedDoc = container.getDocumentUpdateResult().document();
                List<List<Object>> valueCombinations = CompoundIndexMaintainer.extractFieldValues(compoundIndex, updatedDoc, strictTypes);
                for (List<Object> fieldValues : valueCombinations) {
                    CompoundIndexMaintainer.insertEntry(
                            tr, compoundIndex, ctx.metadata(),
                            objectIdBytes,
                            fieldValues, container.getShardId(), container.getEntryMetadata(),
                            ctx.env().collatorCache()
                    );
                }
            }
        }

        // Handle affected vector indexes: drop old entries and re-set with a new vector
        List<CollectedVector> collectedVectors = new ArrayList<>();
        for (VectorIndex vectorIndex : ctx.metadata().vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (!isVectorIndexAffected(vectorIndex, affectedPaths)) {
                continue;
            }
            List<DeletedVector> deletedVectors = new ArrayList<>();
            for (Map.Entry<ObjectId, UpdateResultContainer> updatedEntry : updateResultContainers.entrySet()) {
                ObjectId objectId = updatedEntry.getKey();
                byte[] objectIdBytes = objectId.toByteArray();
                UpdateResultContainer container = updatedEntry.getValue();

                // Drop old entry
                VectorIndexMaintainer.dropEntry(tr, objectIdBytes, vectorIndex, ctx.metadata());
                int deleteUserVersion = ctx.getAndIncrementUserVersion();
                VectorIndexMaintainer.deleteMutationLog(
                        tr, vectorIndex.subspace(), objectIdBytes, deleteUserVersion
                );
                deletedVectors.add(new DeletedVector(objectId, deleteUserVersion));

                // Extract the new vector from the updated document and re-set
                BsonValue newValue = SelectorMatcher.match(vectorIndex.definition().selector(), container.getDocumentUpdateResult().document());
                float[] vector = VectorIndexMaintainer.parseVector(newValue);
                if (vector != null) {
                    VectorIndexMaintainer.insertEntry(
                            tr, vectorIndex, ctx.metadata(),
                            objectIdBytes,
                            container.getShardId(), container.getEntryMetadata(), vector
                    );

                    // Write mutation log entry for crash recovery
                    int userVersion = ctx.getAndIncrementUserVersion();
                    VectorIndexMaintainer.setMutationLog(
                            tr, vectorIndex.subspace(), MutationLogMarker.UPDATE,
                            objectIdBytes,
                            new IndexEntry(container.getShardId(), container.getEntryMetadata()).encode(),
                            vector,
                            userVersion
                    );

                    // Collect for on-heap graph addition
                    collectedVectors.add(new CollectedVector(
                            objectId,
                            container.getShardId(),
                            EntryMetadata.decode(container.getEntryMetadata()),
                            vector,
                            vectorIndex.definition().id(),
                            vectorIndex.definition(),
                            userVersion
                    ));
                }
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

        // Schedule on-heap graph node addition for all updated vectors
        if (!collectedVectors.isEmpty()) {
            CompletableFuture<byte[]> trVersionFuture = tr.getVersionstamp();
            TransactionUtil.addPostCommitHook(
                    new VectorNodeAddHook(ctx.env().bucketService(), ctx.metadata(), collectedVectors, trVersionFuture),
                    ctx.getSession()
            );
        }
    }

    /**
     * Brings a single-field index entry in sync with the updated document. Drops all existing
     * entries for the document and re-creates them from the document's current content, matching
     * the state a fresh insert of the same document would produce.
     */
    private void refreshSingleFieldIndexEntry(QueryContext ctx, Transaction tr, byte[] objectIdBytes, UpdateResultContainer container, SingleFieldIndex index) {
        DirectorySubspace indexSubspace = index.subspace();
        SingleFieldIndexDefinition indexDefinition = index.definition();

        SingleFieldIndexMaintainer.dropEntry(tr, objectIdBytes, indexDefinition, indexSubspace, ctx.metadata().subspace());

        BsonValue bsonValue = SelectorMatcher.match(indexDefinition.selector(), container.getDocumentUpdateResult().document());

        if (bsonValue instanceof BsonArray bsonArray) {
            Set<Object> uniqueIndexValues = extractUniqueIndexValues(bsonArray, indexDefinition);
            for (Object indexValue : uniqueIndexValues) {
                setSingleFieldIndexEntry(ctx, tr, objectIdBytes, container, indexDefinition, indexSubspace, indexValue);
            }
        } else if (bsonValue == null || bsonValue.equals(BsonNull.VALUE)) {
            setSingleFieldIndexEntry(ctx, tr, objectIdBytes, container, indexDefinition, indexSubspace, null);
        } else {
            Object indexValue = BSONUtil.toObject(bsonValue, indexDefinition.bsonType());
            if (indexValue == null) {
                if (!strictTypes) {
                    return;
                }
                throw new IndexTypeMismatchException(indexDefinition, bsonValue);
            }
            setSingleFieldIndexEntry(ctx, tr, objectIdBytes, container, indexDefinition, indexSubspace, indexValue);
        }
    }

    private void setSingleFieldIndexEntry(QueryContext ctx, Transaction tr, byte[] objectIdBytes, UpdateResultContainer container,
                                          SingleFieldIndexDefinition indexDefinition, DirectorySubspace indexSubspace, Object indexValue) {
        IndexEntryContainer indexEntryContainer = new IndexEntryContainer(
                ctx.metadata(),
                indexValue,
                indexDefinition,
                indexSubspace,
                container.getShardId(),
                container.getEntryMetadata(),
                container.getVersionstamp()
        );
        SingleFieldIndexMaintainer.setEntryByObjectId(tr, objectIdBytes, indexEntryContainer, ctx.env().collatorCache());
    }

    private void executeUpsert(Transaction tr, QueryContext ctx) throws IOException {
        BsonDocument upsertDoc = buildUpsertDocument(ctx);

        UniquenessChecker uniquenessChecker = new UniquenessChecker(tr);

        ObjectId objectId;
        BsonValue existingId = upsertDoc.get(ReservedFieldName.ID.getValue());
        if (existingId != null) {
            if (existingId.getBsonType() != BsonType.OBJECT_ID) {
                throw new InvalidBsonTypeException(ReservedFieldName.ID.getValue(), BsonType.OBJECT_ID);
            }
            objectId = existingId.asObjectId().getValue();
            SingleFieldIndex primaryIndex = ctx.metadata().singleFieldIndexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
            uniquenessChecker.checkPrimaryKey(primaryIndex.subspace(), objectId);
        } else {
            objectId = new ObjectId();
            upsertDoc.put(ReservedFieldName.ID.getValue(), new BsonObjectId(objectId));
        }

        // Enforce unique single field indexes before the append so a violation leaves no bytes behind.
        UniquenessEnforcer uniquenessEnforcer =
                new UniquenessEnforcer(ctx.metadata(), ctx.env().collatorCache(), strictTypes);
        uniquenessEnforcer.enqueue(uniquenessChecker, objectId.toByteArray(), upsertDoc);
        uniquenessChecker.await();

        ByteBuffer docBuffer = BSONUtil.toByteBuffer(upsertDoc);

        BucketShard shard = findLocalBucketShard(ctx);
        VolumeSession session = new VolumeSession(tr, ctx.metadata().prefix());

        AppendResult appendResult = shard.volume().append(session, docBuffer);

        AppendedEntry[] appendedEntries = appendResult.getAppendedEntries();
        byte[] objectIdBytes = objectId.toByteArray();
        byte[] encodedIndexEntry = new IndexEntry(shard.id(), appendedEntries[0].metadataBytes()).encode();
        SingleFieldIndex primaryIndex = ctx.metadata().singleFieldIndexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
        PrimaryIndexMaintainer.setEntry(tr, primaryIndex, ctx.metadata(), objectIdBytes, encodedIndexEntry, appendedEntries[0].userVersion());

        // Set single field indexes
        setSecondaryIndexesForUpsert(tr, ctx, objectIdBytes, encodedIndexEntry, appendedEntries[0].userVersion(), upsertDoc);

        // Schedule on-heap graph node addition for upserted vectors
        List<CollectedVector> collectedVectors = new ArrayList<>();
        for (VectorIndex vectorIndex : ctx.metadata().vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            float[] vector = VectorIndexMaintainer.extractVector(vectorIndex.definition(), upsertDoc);
            if (vector == null) {
                continue;
            }
            collectedVectors.add(new CollectedVector(
                    objectId,
                    shard.id(),
                    appendedEntries[0].metadata(),
                    vector,
                    vectorIndex.definition().id(),
                    vectorIndex.definition(),
                    appendedEntries[0].userVersion()
            ));
        }
        if (!collectedVectors.isEmpty()) {
            CompletableFuture<byte[]> trVersionFuture = tr.getVersionstamp();
            TransactionUtil.addPostCommitHook(
                    new VectorNodeAddHook(ctx.env().bucketService(), ctx.metadata(), collectedVectors, trVersionFuture),
                    ctx.getSession()
            );
        }

        // Store the upsert result for ObjectId resolution after commit
        ctx.setUpsertResult(new UpsertResult(objectId));
    }

    private void setSecondaryIndexesForUpsert(
            Transaction tr,
            QueryContext ctx,
            byte[] objectIdBytes,
            byte[] encodedIndexEntry,
            int userVersion,
            BsonDocument document
    ) {
        for (SingleFieldIndex index : ctx.metadata().singleFieldIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            if (PrimaryIndex.isPrimary(index.definition())) {
                continue;
            }

            BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), document);

            if (bsonValue instanceof BsonArray bsonArray) {
                Set<Object> uniqueIndexValues = extractUniqueIndexValues(bsonArray, index.definition());
                for (Object indexValue : uniqueIndexValues) {
                    SingleFieldIndexMaintainer.setEntry(tr, index, ctx.metadata(), indexValue, objectIdBytes, encodedIndexEntry, ctx.env().collatorCache());
                }
            } else {
                Object indexValue = null;
                if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                    indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                    if (indexValue == null) {
                        if (!strictTypes) {
                            continue;
                        }
                        throw new IndexTypeMismatchException(index.definition(), bsonValue);
                    }
                }
                SingleFieldIndexMaintainer.setEntry(tr, index, ctx.metadata(), indexValue, objectIdBytes, encodedIndexEntry, ctx.env().collatorCache());
            }
        }

        // Set compound index entries for upsert
        for (CompoundIndex compoundIndex : ctx.metadata().compoundIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            List<List<Object>> valueCombinations = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, strictTypes);
            for (List<Object> fieldValues : valueCombinations) {
                CompoundIndexMaintainer.setEntry(
                        tr, compoundIndex, ctx.metadata(), fieldValues,
                        objectIdBytes, encodedIndexEntry,
                        ctx.env().collatorCache()
                );
            }
        }

        // Set vector index entries for upsert
        for (VectorIndex vectorIndex : ctx.metadata().vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
            float[] vector = VectorIndexMaintainer.extractVector(vectorIndex.definition(), document);
            if (vector == null) {
                continue;
            }
            VectorIndexMaintainer.setEntry(
                    tr, vectorIndex, ctx.metadata(), objectIdBytes,
                    encodedIndexEntry, vector
            );
            VectorIndexMaintainer.setMutationLog(
                    tr, vectorIndex.subspace(), MutationLogMarker.INSERT,
                    objectIdBytes, encodedIndexEntry, vector, userVersion
            );
        }
    }

    private Set<Object> extractUniqueIndexValues(BsonArray bsonArray, SingleFieldIndexDefinition indexDefinition) {
        Set<Object> uniqueIndexValues = new HashSet<>();
        boolean hasNull = false;
        for (BsonValue element : bsonArray) {
            if (element == null || element.equals(BsonNull.VALUE)) {
                hasNull = true;
                continue;
            }
            Object indexValue = BSONUtil.toObject(element, indexDefinition.bsonType());
            if (indexValue == null) {
                if (strictTypes) {
                    throw new IndexTypeMismatchException(indexDefinition, element);
                }
                continue;
            }
            uniqueIndexValues.add(indexValue);
        }
        if (hasNull) {
            uniqueIndexValues.add(null);
        }
        return uniqueIndexValues;
    }

    private BsonDocument buildUpsertDocument(QueryContext ctx) {
        BsonDocument doc = new BsonDocument();

        if (ctx.queryBytes() != null) {
            BqlExpr expr = BqlParser.parse(ctx.queryBytes());
            extractEqualityConditions(expr, doc);
        }

        for (Map.Entry<String, BsonValue> entry : ctx.options().update().setOps().entrySet()) {
            BSONUtil.setNestedField(doc, entry.getKey(), entry.getValue());
        }

        return doc;
    }

    private void extractEqualityConditions(BqlExpr expr, BsonDocument doc) {
        switch (expr) {
            case BqlEq eq -> {
                BsonValue bsonValue = BSONUtil.bqlValueToBsonValue(eq.value());
                if (bsonValue != null) {
                    BSONUtil.setNestedField(doc, eq.selector(), bsonValue);
                }
            }
            case BqlAnd and -> {
                for (BqlExpr child : and.children()) {
                    extractEqualityConditions(child, doc);
                }
            }
            default -> {
            }
        }
    }

    private static final class UpdateResultContainer {
        private final int shardId;
        private final BSONUpdateUtil.DocumentUpdateResult documentUpdateResult;
        private byte[] entryMetadata;
        private Versionstamp versionstamp;

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

        public Versionstamp getVersionstamp() {
            return versionstamp;
        }

        public void setVersionstamp(Versionstamp versionstamp) {
            this.versionstamp = versionstamp;
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

    private record InsertPostCommitHook(InsertResult insertResult) implements CommitHook {
        @Override
        public void run() {
            insertResult.complete();
        }
    }

}
