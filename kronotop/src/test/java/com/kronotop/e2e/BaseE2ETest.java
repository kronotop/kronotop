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

package com.kronotop.e2e;

import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.sharding.ShardKind;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;

class BaseE2ETest extends BaseClusterTestWithTCPServer {

    protected Cluster setupCluster() {
        KronotopTestInstance primary = getInstances().getFirst();
        KronotopTestInstance standby = addNewInstance();

        int shards = primary.getContext().getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < shards; shardId++) {
            E2ETestUtil.setRoute(primary.getChannel(), "SET", RouteKind.STANDBY, ShardKind.REDIS, shardId, standby);
        }
        await().atMost(Duration.ofSeconds(5)).until(() -> E2ETestUtil.checkReplicationSlots(standby));

        return new Cluster(primary, List.of(standby));
    }
}
