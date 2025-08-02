// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.CommitHook;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
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
public class BucketInsertHandler extends BaseBucketHandler implements Handler {

    public BucketInsertHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETINSERT).set(new BucketInsertMessage(request));
    }

    private Document parseDocument(InputType inputType, byte[] data) {
        if (inputType.equals(InputType.JSON)) {
            return BSONUtil.fromJson(data);
        } else if (inputType.equals(InputType.BSON)) {
            return BSONUtil.fromBson(data);
        } else {
            throw new KronotopException("Invalid input type: " + inputType);
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

            IndexBuilder.setIDIndex(tr, shard.id(), metadata, appendResult.getAppendedEntries());

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
