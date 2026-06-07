/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketCreateMessage;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.cluster.Route;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;

import java.util.ArrayList;
import java.util.List;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(BucketCreateMessage.COMMAND)
@MinimumParameterCount(BucketCreateMessage.MINIMUM_PARAMETER_COUNT)
public class BucketCreateHandler extends AbstractBucketHandler implements Handler {

    public BucketCreateHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETCREATE).set(new BucketCreateMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketCreateMessage message = request.attr(MessageTypes.BUCKETCREATE).get();
        String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
        runAsync(context, response, () -> {
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                List<Integer> shards = new ArrayList<>(message.getShards());
                if (message.getShards().isEmpty()) {
                    BucketShard shard = service.getShardSelector().next();
                    assert shard != null;
                    shards.add(shard.id());
                }

                // Validate shard routes
                for (int shardId : shards) {
                    Route route = service.findRoute(shardId);
                    if (route == null) {
                        throw new KronotopException("No route found for Bucket shard: " + shardId);
                    }
                }

                Collation collation = null;
                if (message.getCollation() != null) {
                    collation = CollationHelper.deserializeAndValidate(message.getCollation());
                }

                BucketMetadata metadata;
                try {
                    metadata = BucketMetadataUtil.create(context, tr, request.getSession(), message.getBucket(), shards, collation);
                } catch (BucketAlreadyExistsException e) {
                    if (message.isIfNotExists()) {
                        return;
                    }
                    throw e;
                }

                if (message.getIndexes() != null) {
                    IndexSchemaPayload payload = IndexCreationHelper.deserializeAndValidate(message.getIndexes());
                    TransactionalContext tx = new TransactionalContext(context, tr);
                    IndexCreationHelper.createIndexes(tx, metadata, payload, IndexStatus.READY);
                }

                tr.commit().join();
                context.getBucketMetadataCache().set(namespace, message.getBucket(), metadata);
            }
        }, response::writeOK);
    }
}
