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
import com.kronotop.cluster.ShardUtils;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.internal.ByteBufUtils;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;


class SetShardStatusSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    SetShardStatusSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        SetShardStatusParameters parameters = new SetShardStatusParameters(request.getParams());

        CompletableFuture.runAsync(() -> {
            try (Transaction tr = membership.getContext().getFoundationDB().createTransaction()) {
                if (parameters.allShards) {
                    int numberOfShards = getNumberOfShards(parameters.shardKind);
                    for (int shardId = 0; shardId < numberOfShards; shardId++) {
                        ShardUtils.setShardStatus(context, tr, parameters.shardKind, parameters.shardStatus, shardId);
                    }
                } else {
                    ShardUtils.setShardStatus(context, tr, parameters.shardKind, parameters.shardStatus, parameters.shardId);
                }
                membership.triggerClusterTopologyWatcher(tr);
                tr.commit().join();
            }
        }, context.getVirtualThreadPerTaskExecutor()).thenRunAsync(response::writeOK, response.getCtx().executor()).exceptionally(ex -> {
            response.writeError(ex);
            return null;
        });
    }

    private class SetShardStatusParameters {
        private final ShardKind shardKind;
        private final boolean allShards;
        private final int shardId;
        private final ShardStatus shardStatus;

        private SetShardStatusParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 4) {
                throw new InvalidNumberOfParametersException();
            }

            shardKind = readShardKind(params.get(1));

            String rawShardId = ByteBufUtils.readAsString(params.get(2));
            allShards = rawShardId.equals("*");
            if (!allShards) {
                shardId = readShardId(shardKind, rawShardId);
            } else {
                shardId = -1; // dummy assignment due to final declaration
            }

            shardStatus = readShardStatus(params.get(3));
        }
    }
}
