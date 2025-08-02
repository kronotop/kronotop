// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.handlers.protocol.BucketDropIndexMessage;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(BucketDropIndexMessage.COMMAND)
@MinimumParameterCount(BucketDropIndexMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketDropIndexMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketDropIndexHandler extends BaseBucketHandler implements Handler {
    public BucketDropIndexHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETDROPINDEX).set(new BucketDropIndexMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            BucketDropIndexMessage message = request.attr(MessageTypes.BUCKETDROPINDEX).get();
            if (message.getIndex().equals(DefaultIndexDefinition.ID.name())) {
                throw new IllegalArgumentException("Cannot drop the default index");
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), message.getBucket());
                IndexUtil.drop(tr, metadata.subspace(), message.getIndex());
                tr.commit().join();
            }
        }, response::writeOK);
    }
}
