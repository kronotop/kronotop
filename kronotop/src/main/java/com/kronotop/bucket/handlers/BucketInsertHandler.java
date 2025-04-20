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
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.VersionstampUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.AppendedEntry;
import com.kronotop.volume.Prefix;
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

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketInsertMessage message = request.attr(MessageTypes.BUCKETINSERT).get();

            // TODO: Distribute the requests among shards in a round robin fashion.
            int shardId = 1;

            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
            BucketSubspace subspace = BucketSubspaceUtils.open(context, request.getSession(), tr);

            Prefix prefix = BucketPrefix.getOrSetBucketPrefix(context, tr, subspace, message.getBucket());

            List<RedisMessage> userVersions = new LinkedList<>();
            boolean autoCommitEnabled = TransactionUtils.getAutoCommit(request.getSession());

            Document[] documents = new Document[message.getDocuments().size()];
            ByteBuffer[] entries = new ByteBuffer[message.getDocuments().size()];
            for (int index = 0; index < message.getDocuments().size(); index++) {
                byte[] data = message.getDocuments().get(index);
                Document document = Document.parse(new String(data));
                entries[index] = ByteBuffer.wrap(BSONUtils.toBytes(document));
                documents[index] = document;
            }

            VolumeSession volumeSession = new VolumeSession(tr, prefix);
            BucketShard shard = service.getShard(shardId);
            AppendResult appendResult;
            try {
                appendResult = shard.volume().append(volumeSession, entries);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            // Set indexes
            for (AppendedEntry appendedEntry : appendResult.getAppendedEntries()) {
                // Set the default ID index
                IndexBuilder.setIndex(tr, subspace, shardId, prefix, appendedEntry.userVersion(), DefaultIndex.ID, appendedEntry.encodedMetadata());
                if (!autoCommitEnabled) {
                    userVersions.add(new IntegerRedisMessage(appendedEntry.userVersion()));
                    request.getSession().attr(SessionAttributes.ASYNC_RETURNING).get().add(appendedEntry.userVersion());
                }
            }

            PostCommitHook postCommitHook = new PostCommitHook(appendResult);
            TransactionUtils.addPostCommitHook(postCommitHook, request.getSession());
            TransactionUtils.commitIfAutoCommitEnabled(tr, request.getSession());

            if (autoCommitEnabled) {
                Versionstamp[] versionstamps = postCommitHook.getVersionstamps();
                List<RedisMessage> children = new ArrayList<>();
                for (Versionstamp versionstamp : versionstamps) {
                    children.add(new SimpleStringRedisMessage(VersionstampUtils.base32HexEncode(versionstamp)));
                }
                tr.close();
                return children;
            } else {
                tr.close();
                // Return userVersions to track the versionstamps in the COMMIT response
                return userVersions;
            }
        }, response::writeArray);
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
