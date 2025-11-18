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

package com.kronotop.volume.segrep;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.client.InternalClient;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.sharding.ShardKind;
import io.lettuce.core.RedisClient;
import io.lettuce.core.codec.ByteArrayCodec;

public class ReplicationClient {
    private final Context context;
    private final ShardKind shardKind;
    private final int shardId;

    private volatile Member server;
    private volatile RedisClient client;
    private volatile StatefulInternalConnection<byte[], byte[]> connection;

    private volatile boolean connected;
    private volatile boolean shutdown;

    ReplicationClient(Context context, ShardKind shardKind, int shardId) {
        this.shardKind = shardKind;
        this.shardId = shardId;
        this.context = context;
    }

    void connect() {
        RoutingService routing = context.getService(RoutingService.NAME);
        Route route = routing.findRoute(shardKind, shardId);
        if (route == null) {
            throw new IllegalArgumentException("Route not found: " + shardKind + " " + shardId);
        }

        Member member = route.primary();
        if (member.equals(server)) {
            // Already connected to the server. Lettuce has its own reconnection logic.
            return;
        }
        connected = false;
        String address = String.format("redis://%s:%d", member.getInternalAddress().getHost(), member.getInternalAddress().getPort());
        RedisClient redisClient = RedisClient.create(address);
        if (client != null) {
            client.shutdown();
        }
        client = redisClient;
        connection = InternalClient.connect(redisClient, ByteArrayCodec.INSTANCE);
        connected = true;
        server = member;
    }

    StatefulInternalConnection<byte[], byte[]> connection() {
        if (shutdown) {
            throw new KronotopException("Replication client has been shutdown");
        }
        if (!connected) {
            throw new KronotopException("Replication client has connected to the primary yet");
        }
        return connection;
    }

    void shutdown() {
        shutdown = true;
        client.shutdown();
    }
}
