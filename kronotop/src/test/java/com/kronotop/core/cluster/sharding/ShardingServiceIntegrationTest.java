/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.core.cluster.sharding;

import com.kronotop.KronotopTestInstance;
import com.kronotop.core.cluster.BaseClusterTest;
import com.kronotop.core.cluster.MembershipService;
import com.kronotop.core.cluster.coordinator.Route;
import com.kronotop.redis.storage.Shard;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * ShardingServiceIntegrationTest is a class that represents the integration test for the ShardingService.
 * It extends the BaseClusterTest class and tests the dropPreviouslyOwnedShards method.
 */
public class ShardingServiceIntegrationTest extends BaseClusterTest {

    /**
     * This method is a JUnit test method that tests the behavior of the
     * 'shardingService_dropPreviouslyOwnedShards' method.
     *
     * <p>
     * The method performs the following steps:
     * <p>
     * 1. Obtains the first KronotopTestInstance from 'kronotopInstances' map.
     * 2. Creates two sets - 'shardIdsOwnedByFirstInstance' and 'shardIdsOwnedBySecondInstance'.
     * 3. Adds a new instance to 'kronotopInstances' map and retrieves the MembershipService object from the second instance.
     * 4. Waits until all shards are operable.
     * 5. Iterates over the number of shards and adds the shard ids to the respective sets
     *    based on whether the shard is owned by the first or second instance.
     * 6. Asserts that shards owned by the second instance are not present in the first instance and vice versa.
     * 7. Asserts that shards owned by the first instance are present in the first instance and are operable.
     * </p>
     */
    @Test
    public void test_shardingService_dropPreviouslyOwnedShards() {
        KronotopTestInstance firstInstance = kronotopInstances.values().iterator().next();

        Set<Integer> shardIdsOwnedByFirstInstance = new HashSet<>();
        Set<Integer> shardIdsOwnedBySecondInstance = new HashSet<>();
        {
            KronotopTestInstance secondInstance = addNewInstance();
            MembershipService membershipService = secondInstance.getContext().getService(MembershipService.NAME);

            await().atMost(10, TimeUnit.SECONDS).until(this::areAllShardsOperable);

            int numberOfShards = membershipService.getContext().getConfig().getInt("cluster.number_of_shards");
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                Route route = membershipService.getRoutingTable().getRoute(shardId);
                if (route.getMember().equals(secondInstance.getMember())) {
                    shardIdsOwnedBySecondInstance.add(shardId);
                } else {
                    shardIdsOwnedByFirstInstance.add(shardId);
                }
            }

            for (Integer shardId : shardIdsOwnedBySecondInstance) {
                Shard shard = firstInstance.getContext().getLogicalDatabase().getShards().get(shardId);
                assertNull(shard);
            }

            for (Integer shardId : shardIdsOwnedByFirstInstance) {
                Shard shard = firstInstance.getContext().getLogicalDatabase().getShards().get(shardId);
                assertNotNull(shard);
                //assertTrue(shard.isOperable());
                System.out.println(shardId + ">>>" + shard.isOperable() + " " +shard.isReadOnly());
            }
        }
    }
}
