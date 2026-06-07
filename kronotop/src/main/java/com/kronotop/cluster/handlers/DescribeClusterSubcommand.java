/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.MetadataVersion;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.transaction.TransactionUtil;

import java.util.LinkedHashMap;
import java.util.Map;

class DescribeClusterSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    DescribeClusterSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                String version = MetadataVersion.read(context, tr);
                result.put(bulkString("metadata_version"), bulkString(version));
                result.put(bulkString("cluster_name"), bulkString(context.getClusterName()));
                for (ShardKind kind : context.getShardRegistry().getShardKinds()) {
                    Map<RedisMessage, RedisMessage> shardsByKind = new LinkedHashMap<>();
                    for (int shardId : getShardIds(kind)) {
                        Map<RedisMessage, RedisMessage> shard = describeShard(tr, kind, shardId);
                        shardsByKind.put(new IntegerRedisMessage(shardId), new MapRedisMessage(shard));
                    }
                    result.put(bulkString(kind.toString().toLowerCase()), new MapRedisMessage(shardsByKind));
                }
            }
            return result;
        }, response::writeMap);
    }
}
