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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.SelectorMatcher;
import com.kronotop.bucket.index.statistics.IndexStatsBuilder;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.VolumeSession;
import org.bson.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketInsertMessage.COMMAND)
@MinimumParameterCount(BucketInsertMessage.MINIMUM_PARAMETER_COUNT)
public class BucketInsertHandler extends AbstractBucketHandler implements Handler {
    private final boolean strictTypes = context.getConfig().getBoolean("bucket.index.strict_types");

    public BucketInsertHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETINSERT).set(new BucketInsertMessage(request));
    }

    /**
     * Prepares entries for insertion into a bucket. This method processes and validates
     * the documents contained in the {@code BucketInsertMessage}, converting them
     * into the appropriate format and wrapping them in an {@code EntriesPack}.
     *
     * @param request the request object containing session information and parameters
     * @param message the bucket insert a message containing the documents to be prepared
     * @return an {@code EntriesPack} containing the serialized entries and parsed document objects
     */
    private EntriesPack prepareEntries(Request request, BucketInsertMessage message) {
        InputType inputType = getInputType(request);
        BsonDocument[] documents = new BsonDocument[message.getDocuments().size()];
        ByteBuffer[] entries = new ByteBuffer[message.getDocuments().size()];
        for (int index = 0; index < message.getDocuments().size(); index++) {
            byte[] data = message.getDocuments().get(index);

            // Parsing also validates the document
            BsonDocument document = parseDocument(inputType, data);
            documents[index] = document;

            if (inputType.equals(InputType.BSON)) {
                entries[index] = ByteBuffer.wrap(data);
            } else {
                entries[index] = BSONUtil.toByteBuffer(document);
            }
        }
        return new EntriesPack(entries, documents);
    }

    /**
     * Processes and sets user-defined indexes for appended entries.
     * This method iterates through appended entries and associated user-defined indexes,
     * extracts values based on index definitions, and updates the indexing structure.
     * It skips the default ID index as it is handled separately.
     *
     * @param tr           the transaction object used for writing data to the indexed structure
     * @param metadata     metadata associated with the bucket, which includes index definitions
     * @param shard        the shard to which the bucket belongs, utilized for indexing operations
     * @param appendResult the result of the append operation containing appended entries
     * @param pack         an {@code EntriesPack} containing the serialized entries and corresponding documents
     */
    private void setUserDefinedIndexes(Transaction tr,
                                       BucketMetadata metadata,
                                       BucketShard shard,
                                       AppendResult appendResult,
                                       EntriesPack pack
    ) {
        for (int i = 0; i < appendResult.getAppendedEntries().length; i++) {
            AppendedEntry appendedEntry = appendResult.getAppendedEntries()[i];
            ByteBuffer entry = pack.entries[i];
            entry.rewind(); // ready to read the document again

            for (Index index : metadata.indexes().getIndexes(IndexSelectionPolicy.READWRITE)) {
                // Skip the default ID index as it's already handled above
                if (index.definition().id() == DefaultIndexDefinition.ID.id()) {
                    continue;
                }

                // Every insert produces an index entry. Missing values and explicit nulls are both
                // represented as null in the index. Non-null values are converted to the target type;
                // if conversion fails due to a type mismatch, the entry is either skipped or an exception has been thrown.
                BsonValue bsonValue = SelectorMatcher.match(index.definition().selector(), entry);

                if (bsonValue instanceof BsonArray bsonArray) {
                    // Multikey index: create an index entry for each unique value in the array
                    Set<Object> uniqueIndexValues = new HashSet<>();
                    List<BsonValue> uniqueBsonValues = new ArrayList<>();
                    for (BsonValue element : bsonArray) {
                        if (element == null || element.equals(BsonNull.VALUE)) {
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
                    for (Object indexValue : uniqueIndexValues) {
                        IndexBuilder.setIndexEntry(
                                tr,
                                index.definition(),
                                shard.id(),
                                metadata,
                                indexValue,
                                appendedEntry
                        );
                    }
                    // Track stats for each unique element, not the array itself
                    for (BsonValue element : uniqueBsonValues) {
                        IndexStatsBuilder.setHintForStats(tr, appendedEntry.userVersion(), index, element);
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

                    IndexBuilder.setIndexEntry(
                            tr,
                            index.definition(),
                            shard.id(),
                            metadata,
                            indexValue,
                            appendedEntry
                    );
                    IndexStatsBuilder.setHintForStats(tr, appendedEntry.userVersion(), index, bsonValue);
                }
            }
        }
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketInsertMessage message = request.attr(MessageTypes.BUCKETINSERT).get();
            if (message.getDocuments().isEmpty()) {
                throw new KronotopException("No documents provided");
            }
            Session session = request.getSession();
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, message.getBucket());
            BucketShard shard = getOrSelectBucketShardId(message.getArguments().shard());

            EntriesPack pack = prepareEntries(request, message);

            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);

            VolumeSession volumeSession = new VolumeSession(tr, metadata.volumePrefix());

            AppendResult appendResult;
            try {
                appendResult = shard.volume().append(volumeSession, pack.entries());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            IndexBuilder.setPrimaryIndexEntry(tr, shard.id(), metadata, appendResult.getAppendedEntries());

            // Index creation for all user-defined indexes
            // Minimum number of indexes is 1. The primary index is the default one.
            if (metadata.indexes().getIndexes(IndexSelectionPolicy.READWRITE).size() > 1) {
                setUserDefinedIndexes(tr, metadata, shard, appendResult, pack);
            }

            boolean autoCommit = TransactionUtils.getAutoCommit(request.getSession());
            PostCommitHook postCommitHook = new PostCommitHook(appendResult);
            TransactionUtils.addPostCommitHook(postCommitHook, request.getSession());
            TransactionUtils.commitIfAutoCommitEnabled(tr, request.getSession());

            if (autoCommit) {
                Versionstamp[] versionstamps = postCommitHook.getVersionstamps();
                List<RedisMessage> children = new ArrayList<>();
                for (Versionstamp versionstamp : versionstamps) {
                    children.add(new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(versionstamp)));
                }
                tr.close();
                return children;
            } else {
                List<RedisMessage> userVersions = new LinkedList<>();
                for (AppendedEntry appendedEntry : appendResult.getAppendedEntries()) {
                    userVersions.add(new IntegerRedisMessage(appendedEntry.userVersion()));
                    request.getSession().attr(SessionAttributes.ASYNC_RETURNING).get().add(appendedEntry.userVersion());
                }
                // Return userVersions to track the versionstamps in the COMMIT response
                return userVersions;
            }
        }, response::writeArray);
    }

    record EntriesPack(ByteBuffer[] entries, BsonDocument[] documents) {
    }

    private static class PostCommitHook implements CommitHook {
        private final AppendResult appendResult;
        private Versionstamp[] versionstamps;

        PostCommitHook(AppendResult appendResult) {
            this.appendResult = appendResult;
        }

        @Override
        public void run() {
            versionstamps = appendResult.getVersionstampedKeys();
        }

        public Versionstamp[] getVersionstamps() {
            return versionstamps;
        }
    }
}
