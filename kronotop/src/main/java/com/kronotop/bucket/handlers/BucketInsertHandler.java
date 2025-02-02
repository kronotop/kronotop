// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.hash.HashCode;
import com.kronotop.CommitHook;
import com.kronotop.NamespaceUtils;
import com.kronotop.TransactionUtils;
import com.kronotop.bucket.BSONUtils;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.directory.Kronotop;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Session;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

import static com.google.common.hash.Hashing.sipHash24;

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

        ByteBuffer[] entries = new ByteBuffer[message.getDocuments().size()];
        for (int i = 0; i < message.getDocuments().size(); i++) {
            byte[] data = message.getDocuments().get(i);
            Document document = Document.parse(new String(data));
            entries[i] = ByteBuffer.wrap(BSONUtils.toBytes(document));
        }

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());

        Namespace namespace = NamespaceUtils.open(service.getContext(), request.getChannelContext(), tr);
        Prefix prefix = service.getOrSetBucketPrefix(tr, namespace);
        Session session = new Session(tr, prefix);

        // TODO: Distribute the requests among shards in a round robin fashion.
        BucketShard shard = service.getShard(1);
        AppendResult appendResult = shard.volume().append(session, entries);
        PostCommitHook postCommitHook = new PostCommitHook(appendResult);
        TransactionUtils.addPostCommitHook(postCommitHook, request.getChannelContext());
        TransactionUtils.commitIfAutoCommitEnabled(tr, request.getChannelContext());
        response.writeOK();
    }

    private static class PostCommitHook implements CommitHook {
        private final AppendResult appendResult;

        public PostCommitHook(AppendResult appendResult) {
            this.appendResult = appendResult;
        }

        @Override
        public void run() {
            System.out.println(Arrays.toString(appendResult.getVersionstampedKeys()));
        }
    }
}
