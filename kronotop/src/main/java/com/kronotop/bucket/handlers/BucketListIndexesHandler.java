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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketListIndexesMessage;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketListIndexesMessage.COMMAND)
@MinimumParameterCount(BucketListIndexesMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketListIndexesMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketListIndexesHandler extends BaseBucketHandler implements Handler {
    public BucketListIndexesHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETLISTINDEXES).set(new BucketListIndexesMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketListIndexesMessage message = request.attr(MessageTypes.BUCKETLISTINDEXES).get();
            Session session = request.getSession();
            List<RedisMessage> children = new ArrayList<>();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());
                List<String> names = IndexUtil.list(tr, metadata.subspace());
                for (String name : names) {
                    Map<RedisMessage, RedisMessage> item = new LinkedHashMap<>();
                    item.put(new SimpleStringRedisMessage("name"), new SimpleStringRedisMessage(name));
                    children.add(new MapRedisMessage(item));
                }
            }
            return children;
        }, response::writeArray);
    }
}
