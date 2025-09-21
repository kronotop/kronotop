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
import com.kronotop.bucket.handlers.protocol.BucketDescribeIndexMessage;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketDescribeIndexMessage.COMMAND)
@MinimumParameterCount(BucketDescribeIndexMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(BucketDescribeIndexMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketDescribeIndexHandler extends AbstractBucketHandler implements Handler {
    public BucketDescribeIndexHandler(BucketService service) {
        super(service);
    }

    private static Map<RedisMessage, RedisMessage> getRedisMessageRedisMessageMap(IndexDefinition definition, IndexStatistics statistics) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(
                new SimpleStringRedisMessage("selector"),
                new SimpleStringRedisMessage(definition.selector())
        );
        description.put(
                new SimpleStringRedisMessage("bson_type"),
                new SimpleStringRedisMessage(definition.bsonType().name())
        );

        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(new SimpleStringRedisMessage("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        description.put(new SimpleStringRedisMessage("statistics"), new MapRedisMessage(stats));

        return description;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETDESCRIBEINDEX).set(new BucketDescribeIndexMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketDescribeIndexMessage message = request.attr(MessageTypes.BUCKETDESCRIBEINDEX).get();
            Session session = request.getSession();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());

                DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), message.getIndex());
                IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, indexSubspace, definition.id());
                return getRedisMessageRedisMessageMap(definition, statistics);
            }
        }, response::writeMap);
    }
}
