// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.CommitHook;
import com.kronotop.TransactionUtils;
import com.kronotop.bucket.BSONUtils;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.bucket.handlers.protocol.BucketInsertMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Session;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.util.Arrays;

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
        Prefix prefix = getBucketSubspace(request, message.getBucket());

        ByteBuffer[] entries = new ByteBuffer[message.getDocuments().size()];
        for (int i = 0; i < message.getDocuments().size(); i++) {
            byte[] data = message.getDocuments().get(i);
            Document document = Document.parse(new String(data));
            entries[i] = ByteBuffer.wrap(BSONUtils.toBytes(document));
        }

        BucketShard shard = service.getShard(1);
        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());
        Session session = new Session(tr, prefix);
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
