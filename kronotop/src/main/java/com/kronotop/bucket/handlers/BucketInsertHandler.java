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
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketInsertMessage.COMMAND)
@MinimumParameterCount(BucketInsertMessage.MINIMUM_PARAMETER_COUNT)
public class BucketInsertHandler extends AbstractBucketHandler implements Handler {

    public BucketInsertHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETINSERT).set(new BucketInsertMessage(request));
    }

    /**
     * Extracts a field value from a BSON document and converts it to the appropriate Java object
     * for index storage. Returns null if the field is not found or if the BSON type doesn't
     * match the expected IndexDefinition type.
     *
     * @param entry            the ByteBuffer containing the BSON document
     * @param selector         the field name to extract
     * @param expectedBsonType the expected BSON type from IndexDefinition
     * @return the extracted field value as a Java object, or null if not found/type mismatch
     */
    private Object extractFieldValueFromBsonDocument(ByteBuffer entry, String selector, BsonType expectedBsonType) {
        entry.rewind();
        try (BsonReader reader = new BsonBinaryReader(entry)) {
            reader.readStartDocument();

            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();

                if (fieldName.equals(selector)) {
                    BsonType actualBsonType = reader.getCurrentBsonType();

                    // Check if the actual BSON type matches the expected type from IndexDefinition
                    if (actualBsonType != expectedBsonType) {
                        // Int64 covers Int32 values
                        if (!(expectedBsonType.equals(BsonType.INT64) && actualBsonType.equals(BsonType.INT32))) {
                            throw new KronotopException("Type mismatch for '" + selector + "'. Expected BsonType=" + expectedBsonType);
                        }
                    }

                    // Extract the value based on the BSON type
                    return switch (actualBsonType) {
                        case STRING -> reader.readString();
                        case INT32 -> (long) reader.readInt32(); // FoundationDB stores as Long
                        case INT64 -> reader.readInt64();
                        case DOUBLE -> reader.readDouble();
                        case BOOLEAN -> reader.readBoolean();
                        case BINARY -> reader.readBinaryData().getData();
                        case DATE_TIME -> reader.readDateTime();
                        case DECIMAL128 -> reader.readDecimal128().bigDecimalValue();
                        case NULL -> {
                            reader.readNull();
                            yield null;
                        }
                        default -> {
                            reader.skipValue(); // Skip unsupported types
                            yield null;
                        }
                    };
                } else {
                    reader.skipValue(); // Skip fields that don't match our selector
                }
            }

            return null; // Field not found in document
        } catch (Exception e) {
            // Log and ignore parsing errors - missing field is OK
            return null;
        }
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
        Document[] documents = new Document[message.getDocuments().size()];
        ByteBuffer[] entries = new ByteBuffer[message.getDocuments().size()];
        for (int index = 0; index < message.getDocuments().size(); index++) {
            byte[] data = message.getDocuments().get(index);

            // Parsing also validates the document
            Document document = parseDocument(inputType, data);
            documents[index] = document;

            if (inputType.equals(InputType.BSON)) {
                entries[index] = ByteBuffer.wrap(data);
            } else {
                entries[index] = ByteBuffer.wrap(BSONUtil.toBytes(document));
            }
        }
        return new EntriesPack(entries, documents);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketInsertMessage message = request.attr(MessageTypes.BUCKETINSERT).get();
            if (message.getDocuments().isEmpty()) {
                throw new KronotopException("No documents provided");
            }
            BucketShard shard = getOrSelectBucketShardId(message.getArguments().shard());

            EntriesPack pack = prepareEntries(request, message);

            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(context, session);

            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, message.getBucket());
            VolumeSession volumeSession = new VolumeSession(tr, metadata.volumePrefix());

            AppendResult appendResult;
            try {
                appendResult = shard.volume().append(volumeSession, pack.entries());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            IndexBuilder.setPrimaryIndexEntry(tr, shard.id(), metadata, appendResult.getAppendedEntries());

            // Index creation for all user-defined indexes
            for (int i = 0; i < appendResult.getAppendedEntries().length; i++) {
                AppendedEntry appendedEntry = appendResult.getAppendedEntries()[i];
                ByteBuffer entry = pack.entries[i];
                entry.rewind();

                for (Index index : metadata.indexes().getIndexes()) {
                    // Skip the default ID index as it's already handled above
                    if (index.definition().equals(DefaultIndexDefinition.ID)) {
                        continue;
                    }

                    Object indexValue = extractFieldValueFromBsonDocument(
                            entry,
                            index.definition().selector(),
                            index.definition().bsonType()
                    );
                    IndexBuilder.setIndexEntry(
                            tr,
                            index.definition(),
                            shard.id(),
                            metadata,
                            indexValue,
                            appendedEntry
                    );
                }
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

    record EntriesPack(ByteBuffer[] entries, Document[] documents) {
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
