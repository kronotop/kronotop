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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketCreateIndexMessage;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.runAsync;


@Command(BucketCreateIndexMessage.COMMAND)
@MinimumParameterCount(BucketCreateIndexMessage.MINIMUM_PARAMETER_COUNT)
public class BucketCreateIndexHandler extends AbstractBucketHandler implements Handler {
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
                        name = IndexNameGenerator.generate(entry.getKey(), definition.getBsonType());
                    }
                    IndexDefinition indexDefinition = IndexDefinition.create(
                            name,
                            entry.getKey(),
                            definition.getBsonType()
                    );
                    IndexUtil.create(tr, metadata.subspace(), indexDefinition);

                    String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
                    IndexBuilderTask task = new IndexBuilderTask(namespace, message.getBucket(), indexDefinition.id());
                    byte[] encodedTask = JSONUtil.writeValueAsBytes(task);
                    for (int shardId = 0; shardId < service.getNumberOfShards(); shardId++) {
                        DirectorySubspace taskSubspace = IndexTaskUtil.createOrOpenTasksSubspace(context, shardId);
                        TaskStorage.create(tr, taskSubspace, encodedTask);
                    }
                }
                tr.commit().join();
            }
        }, response::writeOK);
    }
}
