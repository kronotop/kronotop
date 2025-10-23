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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.handlers.protocol.BucketDropIndexMessage;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Collection;
import java.util.Objects;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(BucketDropIndexMessage.COMMAND)
@MinimumParameterCount(BucketDropIndexMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketDropIndexMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketDropIndexHandler extends AbstractBucketHandler implements Handler {
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
                TransactionalContext tx = new TransactionalContext(context, tr);
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), message.getBucket());
                IndexUtil.drop(tx, metadata, message.getIndex());

                int userVersion = tx.getUserVersion();
                if (!Objects.equals(message.getIndex(), DefaultIndexDefinition.ID.name())) {
                    Collection<Index> indexes = metadata.indexes().getIndexes(IndexSelectionPolicy.ALL);
                    for (Index index : indexes) {
                        if (!index.definition().name().equals(message.getIndex())) {
                            continue;
                        }
                        byte[] taskId = index.subspace().packWithVersionstamp(
                                Tuple.from(IndexSubspaceMagic.TASKS, Versionstamp.incomplete(userVersion))
                        );
                        tr.set(taskId, NULL_BYTES);
                    }
                }

                tr.commit().join();
            }
        }, response::writeOK);
    }
}
