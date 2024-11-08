/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.ClusterNotInitializedException;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.LinkedHashMap;
import java.util.Map;

class DescribeClusterSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    DescribeClusterSubcommand(MembershipService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            if (!isClusterInitialized(tr)) {
                throw new ClusterNotInitializedException();
            }
            for (ShardKind kind : ShardKind.values()) {
                Map<RedisMessage, RedisMessage> shardsByKind = new LinkedHashMap<>();
                int numberOfShards = getNumberOfShards(kind);
                for (int shardId = 0; shardId < numberOfShards; shardId++) {
                    DirectorySubspace subspace = context.getDirectorySubspaceCache().get(kind, shardId);
                    Map<RedisMessage, RedisMessage> shard = describeShard(tr, subspace);
                    shardsByKind.put(new IntegerRedisMessage(shardId), new MapRedisMessage(shard));
                }
                result.put(new SimpleStringRedisMessage(kind.toString().toLowerCase()), new MapRedisMessage(shardsByKind));
            }
        }
        response.writeMap(result);
    }
}