/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

class BucketIndexDescribeSubcommand implements SubcommandHandler {
    private final Context context;

    BucketIndexDescribeSubcommand(Context context) {
        this.context = context;
    }

    private static Map<RedisMessage, RedisMessage> getRedisMessageRedisMessageMap(IndexDefinition definition, IndexStatistics statistics) {
        Map<RedisMessage, RedisMessage> description = new LinkedHashMap<>();
        description.put(
                new SimpleStringRedisMessage("id"),
                new IntegerRedisMessage(definition.id())
        );
        description.put(
                new SimpleStringRedisMessage("selector"),
                new SimpleStringRedisMessage(definition.selector())
        );
        description.put(
                new SimpleStringRedisMessage("bson_type"),
                new SimpleStringRedisMessage(definition.bsonType().name())
        );
        description.put(
                new SimpleStringRedisMessage("status"),
                new SimpleStringRedisMessage(definition.status().name())
        );

        Map<RedisMessage, RedisMessage> stats = new LinkedHashMap<>();
        stats.put(new SimpleStringRedisMessage("cardinality"), new IntegerRedisMessage(statistics.cardinality()));
        description.put(new SimpleStringRedisMessage("statistics"), new MapRedisMessage(stats));

        return description;
    }

    @Override
    public void execute(Request request, Response response) {
        DescribeParameters parameters = new DescribeParameters(request.getParams());
        supplyAsync(context, response, () -> {
            Session session = request.getSession();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, parameters.bucket);

                DirectorySubspace indexSubspace = IndexUtil.open(tr, metadata.subspace(), parameters.index);
                IndexDefinition definition = IndexUtil.loadIndexDefinition(tr, indexSubspace);
                IndexStatistics statistics = BucketMetadataUtil.readIndexStatistics(tr, metadata.subspace(), definition.id());
                return getRedisMessageRedisMessageMap(definition, statistics);
            }
        }, response::writeMap);
    }

    private static class DescribeParameters {
        private final String bucket;
        private final String index;

        DescribeParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
            index = ProtocolMessageUtil.readAsString(params.get(2));
        }
    }
}
