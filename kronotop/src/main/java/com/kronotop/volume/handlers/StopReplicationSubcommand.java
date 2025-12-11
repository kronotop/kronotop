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

package com.kronotop.volume.handlers;

import com.kronotop.KronotopException;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.ReplicationService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class StopReplicationSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    public StopReplicationSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        RoutingService routing = service.getContext().getService(RoutingService.NAME);
        ReplicationService replications = service.getContext().getService(ReplicationService.NAME);
        StopReplicationParameters parameters = new StopReplicationParameters(request.getParams());

        runAsync(context, response, () -> {
            Route route = routing.findRoute(parameters.shardKind, parameters.shardId);
            if (route == null) {
                throw new KronotopException(
                        String.format("No route found for %s-%d", parameters.shardKind, parameters.shardId)
                );
            }
            if (!route.standbys().contains(service.getContext().getMember())) {
                throw new KronotopException(
                        String.format("This node is not a standby for %s-%d", parameters.shardKind, parameters.shardId)
                );
            }
            replications.stopReplication(parameters.shardKind, parameters.shardId, true);
        }, response::writeOK);
    }

    private class StopReplicationParameters {
        private final ShardKind shardKind;
        private final int shardId;

        private StopReplicationParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }

            shardKind = ProtocolMessageUtil.readShardKind(params.get(1));
            shardId = ProtocolMessageUtil.readShardId(service.getContext().getConfig(), shardKind, params.get(2));
        }
    }
}