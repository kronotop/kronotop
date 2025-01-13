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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Map;

class DescribeShardSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    public DescribeShardSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        DescribeShardParameters parameters = new DescribeShardParameters(request.getParams());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Map<RedisMessage, RedisMessage> shard = describeShard(tr, parameters.kind, parameters.shardId);
            response.writeMap(shard);
        }
    }

    private class DescribeShardParameters {
        private final ShardKind kind;
        private final int shardId;

        private DescribeShardParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            kind = readShardKind(params.get(1));
            shardId = readShardId(kind, params.get(2));
        }
    }
}
