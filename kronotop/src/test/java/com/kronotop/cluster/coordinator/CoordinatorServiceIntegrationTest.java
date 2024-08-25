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

package com.kronotop.cluster.coordinator;

import com.kronotop.KronotopTestInstance;
import com.kronotop.ServiceContext;
import com.kronotop.cluster.BaseClusterTest;
import com.kronotop.cluster.MembershipService;
import com.kronotop.instance.KronotopInstance;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.storage.RedisShard;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The {@code CoordinatorServiceIntegrationTest} class is responsible for testing the coordination service integration in
 * the Kronotop application.
 * <p>
 * It extends the {@link BaseClusterTest} class to set up the test environment and tear it down after the tests are
 * executed.
 * <p>
 * This class contains two test methods:
 * <ul>
 *     <li>{@link #test_singleKronotopInstance()}: Test case to verify the behavior of a single Kronotop instance in the
 *     cluster. It checks the shards' properties and states.</li>
 *     <li>{@link #test_kronotopClusterWithTwoInstances()}: Test case to verify the behavior of a Kronotop cluster with
 *     two instances. It checks the routing table, shards' properties and states, and shard ownership.</li>
 * </ul>
 * <p>
 * The test class uses the {@link KronotopTestInstance} class to create and manage the Kronotop test instances. Each
 * instance is associated with a configuration and represents a standalone Kronotop instance for testing purposes. It
 * extends the {@link KronotopInstance} class and adds additional functionality to support testing.
 * <p>
 * The {@link KronotopTestInstance} class also contains a private inner class {@code CheckClusterStatus} that is used
 * to check the status of the cluster by iterating through each shard and performing necessary checks. If an issue is
 * found, the execution is scheduled to retry after a certain time period. Once all the shards have been checked and no
 * issues are found, it notifies the clusterOperable object.
 */
public class CoordinatorServiceIntegrationTest extends BaseClusterTest {

    /**
     * This method tests a single Kronotop instance.
     * It retrieves the first KronotopTestInstance from the kronotopInstances map.
     * It then retrieves the number of shards from the instance's context configuration.
     * It iterates through each shard and performs the necessary checks:
     * - Asserts that the shard is not null
     * - Asserts that the shard is not read-only
     * - Asserts that the shard is operable
     *
     * @throws AssertionError if any of the shard checks fail
     */
    @Test
    public void test_singleKronotopInstance() {
        KronotopTestInstance kronotopTestInstance = kronotopInstances.values().iterator().next();
        int numberOfShards = kronotopTestInstance.getContext().getConfig().getInt("cluster.number_of_shards");
        ServiceContext<RedisShard> redisContext = kronotopTestInstance.getContext().getServiceContext(RedisService.NAME);
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            RedisShard shard = redisContext.shards().get(shardId);
            assertNotNull(shard);
            assertFalse(shard.isReadOnly());
            assertTrue(shard.isOperable());
        }
    }


    /**
     * This method tests the functionality of the Kronotop cluster when two instances are created.
     * <p>
     * The initial state of the cluster is checked by iterating through each shard of the first Kronotop instance.
     * - Asserts that the shard is not null
     * - Asserts that the shard is not read-only
     * - Asserts that the shard is operable
     * <p>
     * Then, a new Kronotop instance is added to the cluster. The routing table, shards, shard ownership, and their statuses are checked.
     * - For each shard, it retrieves the route from the routing table.
     * - If the route's member is the new instance, asserts that the shard is not read-only and is operable.
     * - Otherwise, asserts that the route's member is the first instance's member.
     *
     * @throws AssertionError if any of the checks fail
     */
    @Test
    public void test_kronotopClusterWithTwoInstances() {
        // Check the initial state
        {
            KronotopTestInstance kronotopTestInstance = kronotopInstances.values().iterator().next();
            int numberOfShards = kronotopTestInstance.getContext().getConfig().getInt("cluster.number_of_shards");
            ServiceContext<RedisShard> redisContext = kronotopTestInstance.getContext().getServiceContext(RedisService.NAME);
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                RedisShard shard = redisContext.shards().get(shardId);
                assertNotNull(shard);
                assertFalse(shard.isReadOnly());
                assertTrue(shard.isOperable());
            }
        }

        // Start a new Kronotop instance and check routing table, shards, shard ownership and their statuses.
        {
            KronotopTestInstance firstInstance = kronotopInstances.values().iterator().next();

            KronotopTestInstance kronotopTestInstance = addNewInstance();
            MembershipService membershipService = kronotopTestInstance.getContext().getService(MembershipService.NAME);
            RoutingTable routingTable = membershipService.getRoutingTable();

            int numberOfShards = kronotopTestInstance.getContext().getConfig().getInt("cluster.number_of_shards");
            ServiceContext<RedisShard> redisContext = kronotopTestInstance.getContext().getServiceContext(RedisService.NAME);
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                Route route = routingTable.getRoute(shardId);
                assertNotNull(route);
                if (route.getMember().equals(kronotopTestInstance.getMember())) {
                    RedisShard shard = redisContext.shards().get(shardId);
                    assertFalse(shard.isReadOnly());
                    assertTrue(shard.isOperable());
                } else {
                    assertEquals(firstInstance.getContext().getMember(), route.getMember());
                }
            }
        }
    }
}
