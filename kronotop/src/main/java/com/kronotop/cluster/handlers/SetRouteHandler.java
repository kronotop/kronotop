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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class SetRouteHandler extends BaseKrAdminSubcommandHandler implements SubcommandHandler {
    public SetRouteHandler(MembershipService service) {
        super(service);
    }

    private void setPrimaryRoute(Transaction tr, Route route, DirectorySubspace subspace, SetRouteParameters parameters) {
        if (route == null) {
            // Setting the route first time
            byte[] key = subspace.pack(Tuple.from(MembershipConstants.ROUTE_PRIMARY_KEY));
            tr.set(key, parameters.memberId.getBytes());
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void execute(Request request, Response response) {
        SetRouteParameters parameters = new SetRouteParameters(request.getParams());

        DirectorySubspace metadata = MembershipUtils.createOrOpenClusterMetadataSubspace(service.getContext());
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            if (!isClusterInitialized(tr, metadata)) {
                throw new ClusterNotInitializedException();
            }

            if (!service.isMemberRegistered(parameters.memberId)) {
                throw new KronotopException("Member is not registered");
            }

            DirectorySubspace subspace = openShardSubspace(tr, metadata, parameters.shardKind, parameters.shardId);
            Route route = loadRoute(tr, subspace);
            if (parameters.routeKind.equals(RouteKind.PRIMARY)) {
                setPrimaryRoute(tr, route, metadata, parameters);
            } else if (parameters.routeKind.equals(RouteKind.STANDBY)) {
                //
            } else {
                // This should be impossible!
                throw new KronotopException("Unknown route kind: " + parameters.routeKind);
            }
            tr.commit().join();
        }
        response.writeOK();
    }

    private class SetRouteParameters {
        private final RouteKind routeKind;
        private final ShardKind shardKind;
        private final int shardId;
        private final String memberId;

        SetRouteParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 5) {
                throw new KronotopException("Invalid number of parameters");
            }

            String rawRouteKind = readAsString(params.get(1));
            try {
                routeKind = RouteKind.valueOf(rawRouteKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid route kind: " + rawRouteKind);
            }

            String rawShardKind = readAsString(params.get(2));
            try {
                shardKind = ShardKind.valueOf(rawShardKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid shard kind: " + rawShardKind);
            }

            String rawShardId = readAsString(params.get(3));
            try {
                shardId = Integer.parseInt(rawShardId);
            } catch (NumberFormatException e) {
                throw new KronotopException("Invalid shard id: " + rawShardId);
            }

            memberId = readMemberId(params.get(4));
        }
    }
}
