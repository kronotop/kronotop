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
import com.kronotop.bucket.handlers.protocol.BucketCreateIndexMessage;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexNameGenerator;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.runAsync;


@Command(BucketCreateIndexMessage.COMMAND)
@MinimumParameterCount(BucketCreateIndexMessage.MINIMUM_PARAMETER_COUNT)
public class BucketCreateIndexHandler extends BaseBucketHandler implements Handler {
    public BucketCreateIndexHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETCREATEINDEX).set(new BucketCreateIndexMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            BucketCreateIndexMessage message = request.attr(MessageTypes.BUCKETCREATEINDEX).get();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), message.getBucket());
                for (Map.Entry<String, BucketCreateIndexMessage.IndexDefinition> entry : message.getDefinitions().entrySet()) {
                    BucketCreateIndexMessage.IndexDefinition definition = entry.getValue();
                    String name = definition.getName();
                    if (name == null) {
                        name = IndexNameGenerator.generate(entry.getKey(), definition.getBsonType(), definition.getSortOrder());
                    }
                    IndexDefinition indexDefinition = IndexDefinition.create(
                            name,
                            entry.getKey(),
                            definition.getBsonType(),
                            definition.getSortOrder()
                    );
                    IndexUtil.create(tr, metadata.subspace(), indexDefinition);
                }
                tr.commit().join();
            }
        }, response::writeOK);
    }
}
