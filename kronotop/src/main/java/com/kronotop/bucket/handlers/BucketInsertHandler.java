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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.bucket.pipeline.VectorNodeAddHook;
import com.kronotop.bucket.vector.CollectedVector;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeSession;
import org.bson.*;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketInsertMessage.COMMAND)
@MinimumParameterCount(BucketInsertMessage.MINIMUM_PARAMETER_COUNT)
public class BucketInsertHandler extends AbstractBucketHandler implements Handler {
    private static final int OBJECTID_BSON_OVERHEAD = 17; // 1 type + 4 name ("_id\0") + 12 value
    private final boolean strictTypes = context.getConfig().getBoolean("bucket.index.strict_types");

    public BucketInsertHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETINSERT).set(new BucketInsertMessage(request));
    }

    /**
     * Serializes documents to BSON ByteBuffer format based on the session's input type.
     * Generates and injects ObjectId for each document.
     *
     * @param request the request containing session configuration
     * @param message the insert message containing document data
     * @return array of SerializedDocument containing ObjectId and BSON content
     */
    private SerializedDocument[] serializeDocuments(Request request, BucketInsertMessage message) {
        InputType inputType = getInputType(request);
        int count = message.getDocumentCount();
        SerializedDocument[] documents = new SerializedDocument[count];

        for (int index = 0; index < count; index++) {
            byte[] data = message.getDocument(index);

            BsonDocument document;
            int bufferSize = 0;
            if (inputType == InputType.BSON) {
                document = BSONUtil.toBsonDocument(data);
                bufferSize = data.length;
            } else if (inputType == InputType.JSON) {
                document = BSONUtil.fromJson(data);
            } else {
                throw new KronotopException("Invalid input type: " + inputType);
            }

            // Use user-provided _id if present, otherwise generate one
            ObjectId objectId;
            boolean userProvidedId = false;
            BsonValue existingId = document.get(ReservedFieldName.ID.getValue());
            if (existingId != null) {
                if (existingId.getBsonType() != BsonType.OBJECT_ID) {
                    throw new InvalidBsonTypeException(ReservedFieldName.ID.getValue(), BsonType.OBJECT_ID);
                }
                objectId = existingId.asObjectId().getValue();
                userProvidedId = true;
            } else {
                objectId = new ObjectId();
                document.put(ReservedFieldName.ID.getValue(), new BsonObjectId(objectId));
                bufferSize += OBJECTID_BSON_OVERHEAD;
            }

            ByteBuffer content = bufferSize > 0
                    ? BSONUtil.toByteBuffer(document, bufferSize)
                    : BSONUtil.toByteBuffer(document);
            documents[index] = new SerializedDocument(objectId, content, document, userProvidedId);
        }
        return documents;
    }

    /**
     * Creates single field index entries for the appended documents. Handles both single-value
     * and multi-key (array) indexes, including type validation and statistics tracking.
     *
     * @param tr                  the FoundationDB transaction
     * @param metadata            bucket metadata containing index definitions
     * @param appendResult        volume append result with entry metadata
     * @param documents           serialized documents with ObjectIds
     * @param objectIdBytesArray  pre-computed ObjectId bytes per document
     * @param encodedIndexEntries pre-computed encoded IndexEntry bytes per document
     */
    private void setSecondaryIndexes(Transaction tr,
                                     BucketMetadata metadata,
                                     AppendResult appendResult,
                                     SerializedDocument[] documents,
                                     byte[][] objectIdBytesArray,
                                     byte[][] encodedIndexEntries
    ) {
        for (int i = 0; i < appendResult.getAppendedEntries().length; i++) {
            AppendedEntry appendedEntry = appendResult.getAppendedEntries()[i];
            SerializedDocument serializedDocument = documents[i];
            BsonDocument document = serializedDocument.document();
            byte[] objectIdBytes = objectIdBytesArray[i];
            byte[] encodedIndexEntry = encodedIndexEntries[i];

            for (Index index : metadata.indexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                // Skip the default ID index as it's already handled above
                if (PrimaryIndex.isPrimary(index.definition())) {
                    continue;
                }

                // Every insert produces an index entry. Missing values and explicit nulls are both
                // represented as null in the index. Non-null values are converted to the target type;
                // if conversion fails due to a type mismatch, the entry is either skipped or an exception has been thrown.
                BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), document);

                if (bsonValue instanceof BsonArray bsonArray) {
                    // Multikey index: create an index entry for each unique value in the array
                    Set<Object> uniqueIndexValues = new HashSet<>();
                    List<BsonValue> uniqueBsonValues = new ArrayList<>();
                    boolean hasNull = false;
                    for (BsonValue element : bsonArray) {
                        if (element == null || element.equals(BsonNull.VALUE)) {
                            hasNull = true;
                            continue;
                        }
                        Object indexValue = BSONUtil.toObject(element, index.definition().bsonType());
                        if (indexValue == null) {
                            if (strictTypes) {
                                throw new IndexTypeMismatchException(index.definition(), element);
                            }
                            continue;
                        }
                        if (uniqueIndexValues.add(indexValue)) {
                            uniqueBsonValues.add(element);
                        }
                    }
                    // Index null elements (deduplicated) for consistent semantics with single-value indexes
                    if (hasNull && uniqueIndexValues.add(null)) {
                        uniqueBsonValues.add(BsonNull.VALUE);
                    }
                    for (Object indexValue : uniqueIndexValues) {
                        SingleFieldIndexMaintainer.setEntry(
                                tr,
                                index,
                                metadata,
                                indexValue,
                                objectIdBytes,
                                encodedIndexEntry,
                                service.getCollatorCache()
                        );
                    }
                    // Track stats for each unique element, not the array itself
                    for (BsonValue element : uniqueBsonValues) {
                        IndexStatsBuilder.setHintForStats(tr, index, objectIdBytes, element);
                    }
                } else {
                    // Single value index
                    Object indexValue = null;
                    if (bsonValue != null && !bsonValue.equals(BsonNull.VALUE)) {
                        indexValue = BSONUtil.toObject(bsonValue, index.definition().bsonType());
                        if (indexValue == null) {
                            if (!strictTypes) {
                                // Type mismatch, continue
                                continue;
                            }
                            throw new IndexTypeMismatchException(index.definition(), bsonValue);
                        }
                    }

                    SingleFieldIndexMaintainer.setEntry(
                            tr,
                            index,
                            metadata,
                            indexValue,
                            objectIdBytes,
                            encodedIndexEntry,
                            service.getCollatorCache()
                    );
                    IndexStatsBuilder.setHintForStats(tr, index, objectIdBytes, bsonValue);
                }
            }
        }
    }

    /**
     * Creates vector index entries for the appended documents. For each vector index,
     * extracts the vector from the document and stores it alongside the index entry.
     * Registers a post-commit hook to add vectors to the in-memory graph after commit.
     */
    private void setVectorIndexes(Transaction tr,
                                  BucketMetadata metadata,
                                  AppendResult appendResult,
                                  SerializedDocument[] documents,
                                  byte[][] objectIdBytesArray,
                                  byte[][] encodedIndexEntries,
                                  int shardId,
                                  Session session
    ) {
        List<CollectedVector> collectedVectors = new ArrayList<>();

        for (int i = 0; i < appendResult.getAppendedEntries().length; i++) {
            AppendedEntry appendedEntry = appendResult.getAppendedEntries()[i];
            SerializedDocument serializedDocument = documents[i];
            BsonDocument document = serializedDocument.document();
            byte[] objectIdBytes = objectIdBytesArray[i];
            byte[] encodedIndexEntry = encodedIndexEntries[i];

            for (VectorIndex vectorIndex : metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                float[] vector = VectorIndexMaintainer.extractVector(vectorIndex.definition(), document);
                if (vector == null) {
                    continue;
                }
                VectorIndexMaintainer.setEntry(
                        tr,
                        vectorIndex,
                        metadata,
                        objectIdBytes,
                        encodedIndexEntry,
                        vector
                );
                VectorIndexMaintainer.setMutationLog(
                        tr,
                        vectorIndex.subspace(),
                        MutationLogMarker.INSERT,
                        objectIdBytes,
                        encodedIndexEntry,
                        vector,
                        appendedEntry.userVersion()
                );

                collectedVectors.add(new CollectedVector(
                        serializedDocument.objectId(),
                        shardId,
                        appendedEntry.metadata(),
                        vector,
                        vectorIndex.definition().id(),
                        vectorIndex.definition(),
                        appendedEntry.userVersion()
                ));
            }
        }

        if (!collectedVectors.isEmpty()) {
            CompletableFuture<byte[]> trVersionFuture = tr.getVersionstamp();
            TransactionUtil.addPostCommitHook(
                    new VectorNodeAddHook(service, metadata, collectedVectors, trVersionFuture),
                    session
            );
        }
    }

    /**
     * Creates compound index entries for the appended documents. For each compound index,
     * extracts all field values and produces one entry per value combination (expanding
     * multi-key array fields).
     */
    private void setCompoundIndexes(Transaction tr,
                                    BucketMetadata metadata,
                                    AppendResult appendResult,
                                    SerializedDocument[] documents,
                                    byte[][] objectIdBytesArray,
                                    byte[][] encodedIndexEntries
    ) {
        for (int i = 0; i < appendResult.getAppendedEntries().length; i++) {
            AppendedEntry appendedEntry = appendResult.getAppendedEntries()[i];
            SerializedDocument serializedDocument = documents[i];
            BsonDocument document = serializedDocument.document();
            byte[] objectIdBytes = objectIdBytesArray[i];
            byte[] encodedIndexEntry = encodedIndexEntries[i];

            for (CompoundIndex compoundIndex : metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.WRITABLE)) {
                List<List<Object>> valueCombinations = CompoundIndexMaintainer.extractFieldValues(compoundIndex, document, strictTypes);
                for (List<Object> fieldValues : valueCombinations) {
                    CompoundIndexMaintainer.setEntry(
                            tr,
                            compoundIndex,
                            metadata,
                            fieldValues,
                            objectIdBytes,
                            encodedIndexEntry,
                            service.getCollatorCache()
                    );
                }
                IndexStatsBuilder.setHintForStatsIfSelected(tr, compoundIndex, objectIdBytes);
            }
        }
    }

    /**
     * Checks the primary index for existing _id entries and throws if any duplicate is found.
     */
    private void checkDuplicateIds(Transaction tr, BucketMetadata metadata, Set<ObjectId> objectIds) {
        Index index = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
        DirectorySubspace indexSubspace = index.subspace();
        for (ObjectId objectId : objectIds) {
            byte[] objectIdBytes = objectId.toByteArray();
            Tuple tuple = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), objectIdBytes);
            byte[] key = indexSubspace.pack(tuple);
            byte[] existing = tr.get(key).join();
            if (existing != null) {
                throw new DuplicateKeyException(objectId);
            }
        }
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketInsertMessage message = request.attr(MessageTypes.BUCKETINSERT).get();
            if (message.getDocumentCount() == 0) {
                throw new KronotopException("No documents provided");
            }
            Session session = request.getSession();

            Transaction tr = TransactionUtil.getOrCreateTransaction(context, session);
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());

            Index primaryIndex = metadata.indexes().getIndex(PrimaryIndex.SELECTOR, IndexSelectionPolicy.READWRITE);
            if (primaryIndex == null) {
                throw new KronotopException("Primary index is missing from metadata.");
            }

            BucketShard shard = findLocalShard(metadata);

            // Extract ByteBuffers for volume append
            SerializedDocument[] documents = serializeDocuments(request, message);

            // Check for duplicate _id values (only for user-provided ones)
            Set<ObjectId> userProvidedIds = null;
            for (SerializedDocument doc : documents) {
                if (doc.userProvidedId()) {
                    if (userProvidedIds == null) {
                        userProvidedIds = new HashSet<>();
                    }
                    if (!userProvidedIds.add(doc.objectId())) {
                        throw new DuplicateKeyException(doc.objectId());
                    }
                }
            }
            if (userProvidedIds != null && !userProvidedIds.isEmpty()) {
                checkDuplicateIds(tr, metadata, userProvidedIds);
            }

            if (!metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE).isEmpty()) {
                checkVectorIndexRecoveryState(metadata);
                for (SerializedDocument doc : documents) {
                    VectorIndexUtil.validateVectorFields(metadata, doc.document());
                }
            }

            ByteBuffer[] entries = new ByteBuffer[documents.length];
            for (int i = 0; i < documents.length; i++) {
                entries[i] = documents[i].content();
            }

            boolean autoCommit = TransactionUtil.getAutoCommit(session);
            VolumeSession volumeSession;
            if (autoCommit) {
                volumeSession = new VolumeSession(tr, metadata.prefix());
            } else {
                volumeSession = new VolumeSession(tr, metadata.prefix(),
                        () -> TransactionUtil.getUserVersion(session));
            }

            AppendResult appendResult;
            try {
                appendResult = shard.volume().append(volumeSession, entries);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            // Pre-compute objectIdBytes and encodedIndexEntry per document
            byte[][] objectIdBytesArray = new byte[documents.length][];
            byte[][] encodedIndexEntries = new byte[documents.length][];
            for (int i = 0; i < documents.length; i++) {
                objectIdBytesArray[i] = documents[i].objectId().toByteArray();
                AppendedEntry ae = appendResult.getAppendedEntries()[i];
                encodedIndexEntries[i] = new IndexEntry(shard.id(), ae.metadataBytes()).encode();
            }

            // Set primary index entries with ObjectIds
            for (int i = 0; i < documents.length; i++) {
                AppendedEntry ae = appendResult.getAppendedEntries()[i];
                PrimaryIndexMaintainer.setEntry(
                        tr,
                        primaryIndex,
                        metadata,
                        objectIdBytesArray[i],
                        encodedIndexEntries[i],
                        ae.userVersion()
                );
                SerializedDocument serialized = documents[i];
                BsonValue id = serialized.document.get(ReservedFieldName.ID.getValue());
                IndexStatsBuilder.setHintForStats(tr, primaryIndex, objectIdBytesArray[i], id);
            }

            // Index creation for all user-defined indexes
            // Minimum number of indexes is 1. The primary index is the default one.
            if (metadata.indexes().getIndexes(IndexSelectionPolicy.WRITABLE).size() > 1) {
                setSecondaryIndexes(tr, metadata, appendResult, documents, objectIdBytesArray, encodedIndexEntries);
            }

            if (!metadata.compoundIndexes().getIndexes(IndexSelectionPolicy.WRITABLE).isEmpty()) {
                setCompoundIndexes(tr, metadata, appendResult, documents, objectIdBytesArray, encodedIndexEntries);
            }

            if (!metadata.vectorIndexes().getIndexes(IndexSelectionPolicy.WRITABLE).isEmpty()) {
                setVectorIndexes(tr, metadata, appendResult, documents, objectIdBytesArray, encodedIndexEntries, shard.id(), session);
            }

            TransactionUtil.commitIfAutoCommitEnabled(tr, session);

            // Return ObjectIds
            ObjectIdFormat format = request.getSession().attr(SessionAttributes.OBJECT_ID_FORMAT).get();
            List<RedisMessage> children = new ArrayList<>(documents.length);
            for (SerializedDocument document : documents) {
                children.add(encodeObjectId(format, document.objectId()));
            }
            return children;
        }, response::writeArray);
    }

    /**
     * Container for a serialized document with its ObjectId.
     */
    private record SerializedDocument(ObjectId objectId, ByteBuffer content, BsonDocument document,
                                      boolean userProvidedId) {
    }
}
