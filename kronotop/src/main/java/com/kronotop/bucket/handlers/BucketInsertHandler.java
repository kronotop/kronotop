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
import com.kronotop.NamespaceUtils;
import com.kronotop.TransactionUtils;
import com.kronotop.VersionstampUtils;
import com.kronotop.bucket.BSONUtils;
import com.kronotop.bucket.BucketPrefix;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Session;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
        BucketInsertMessage message = request.attr(MessageTypes.BUCKETINSERT).get();

        // TODO: Distribute the requests among shards in a round robin fashion.
        int shardId = 1;

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());
        Namespace namespace = NamespaceUtils.open(service.getContext(), request.getChannelContext(), tr);
        Prefix prefix = BucketPrefix.getOrSetBucketPrefix(context, tr, namespace, message.getBucket());

        ByteBuffer[] entries = new ByteBuffer[message.getDocuments().size()];
        for (int index = 0; index < message.getDocuments().size(); index++) {
            byte[] data = message.getDocuments().get(index);
            Document document = Document.parse(new String(data));
            entries[index] = ByteBuffer.wrap(BSONUtils.toBytes(document));
            IndexBuilder.setIdIndex(tr, namespace, shardId, prefix, index);
        }

        Session session = new Session(tr, prefix);
        BucketShard shard = service.getShard(shardId);
        AppendResult appendResult = shard.volume().append(session, entries);

        PostCommitHook postCommitHook = new PostCommitHook(appendResult);
        TransactionUtils.addPostCommitHook(postCommitHook, request.getChannelContext());
        TransactionUtils.commitIfAutoCommitEnabled(tr, request.getChannelContext());

        if (postCommitHook.isCommitted()) {
            Versionstamp[] versionstamps = postCommitHook.getVersionstamps();
            List<RedisMessage> children = new ArrayList<>();
            for (Versionstamp versionstamp : versionstamps) {
                children.add(new SimpleStringRedisMessage(VersionstampUtils.base32HexEncode(versionstamp)));
            }
            response.writeArray(children);
        } else {
            // Empty response
            response.writeArray(List.of());
        }
    }

    private static class PostCommitHook implements CommitHook {
        private final AppendResult appendResult;
        private Versionstamp[] versionstamps;
        private volatile boolean committed;

        PostCommitHook(AppendResult appendResult) {
            this.appendResult = appendResult;
        }

        @Override
        public void run() {
            versionstamps = appendResult.getVersionstampedKeys();
            committed = true;
        }

        public Versionstamp[] getVersionstamps() {
            return versionstamps;
        }

        public boolean isCommitted() {
            return committed;
        }
    }
}
