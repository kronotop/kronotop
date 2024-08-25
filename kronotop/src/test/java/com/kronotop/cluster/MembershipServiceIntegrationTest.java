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

package com.kronotop.cluster;

import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.coordinator.Route;
import com.kronotop.cluster.coordinator.RoutingTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The MembershipServiceIntegrationTest class contains integration tests
 * for the MembershipService class.
 *
 * <p>
 * This class tests various methods of the MembershipService class, including
 * the getKnownCoordinator, getMembers, and getRoutingTable methods.
 * It also tests the behavior of the MembershipService class in different
 * scenarios such as a single Kronotop instance and a Kronotop cluster
 * with two instances.
 * </p>
 *
 * <p>
 * The test cases in this class use a BaseClusterTest class as a base class
 * to set up and tear down the Kronotop instances for testing.
 * </p>
 */
public class MembershipServiceIntegrationTest extends BaseClusterTest {

    @Test
    public void test_singleKronotopInstance_getKnownCoordinator() {
        // In the beginning, we already have a running Kronotop instance.
        KronotopTestInstance kronotopTestInstance = kronotopInstances.values().iterator().next();

        // Check the known coordinator
        MembershipService membershipService = kronotopTestInstance.getContext().getService(MembershipService.NAME);
        assertEquals(kronotopTestInstance.getContext().getMember(), membershipService.getKnownCoordinator());
    }

    @Test
    public void test_singleKronotopInstance_getMembers() {
        KronotopTestInstance kronotopTestInstance = kronotopInstances.values().iterator().next();
        MembershipService membershipService = kronotopTestInstance.getContext().getService(MembershipService.NAME);

        List<String> members = membershipService.getMembers();
        assertEquals(1, members.size());

        String hostPort = members.get(0);
        assertEquals(membershipService.getContext().getMember().getAddress().toString(), hostPort);
    }

    @Test
    @Disabled("CLUSTERING-REFACTOR")
    public void test_singleKronotopInstance_getRoutingTable() {
        KronotopTestInstance kronotopTestInstance = kronotopInstances.values().iterator().next();
        MembershipService membershipService = kronotopTestInstance.getContext().getService(MembershipService.NAME);

        int numberOfShards = membershipService.getContext().getConfig().getInt("cluster.number_of_shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            Route route = membershipService.getRoutingTable().getRoute(shardId);
            assertNotNull(route);
            assertEquals(membershipService.getContext().getMember(), route.getMember());
        }
        assertEquals(membershipService.getKnownCoordinator(), membershipService.getRoutingTable().getCoordinator());
        assertNotEquals(0, membershipService.getRoutingTable().getVersion());
    }

    @Test
    public void test_kronotopClusterWithTwoInstances_check_known_coordinators() {
        KronotopTestInstance instanceOne = kronotopInstances.values().iterator().next();
        KronotopTestInstance instanceTwo = addNewInstance();

        // Check the known coordinator
        {
            MembershipService membershipServiceOne = instanceOne.getContext().getService(MembershipService.NAME);
            MembershipService membershipServiceTwo = instanceTwo.getContext().getService(MembershipService.NAME);
            assertEquals(membershipServiceOne.getKnownCoordinator(), membershipServiceTwo.getKnownCoordinator());
        }

        {
            MembershipService membershipService = instanceOne.getContext().getService(MembershipService.NAME);
            assertEquals(instanceOne.getContext().getMember(), membershipService.getKnownCoordinator());
        }
    }

    @Test
    public void test_kronotopClusterWithTwoInstances_getMembers() {
        KronotopTestInstance instanceOne = kronotopInstances.values().iterator().next();
        KronotopTestInstance instanceTwo = addNewInstance();

        MembershipService membershipService = instanceOne.getContext().getService(MembershipService.NAME);
        Set<String> addresses = new HashSet<>();
        addresses.add(instanceOne.getContext().getMember().getAddress().toString());
        addresses.add(instanceTwo.getContext().getMember().getAddress().toString());

        List<String> members = membershipService.getMembers();
        assertEquals(2, members.size());
        for (String member : members) {
            assertTrue(addresses.contains(member));
        }
    }

    @Test
    @Disabled("CLUSTERING-REFACTOR")
    public void test_kronotopClusterWithTwoInstances_getRoutingTable() {
        KronotopTestInstance instanceOne = kronotopInstances.values().iterator().next();
        KronotopTestInstance instanceTwo = addNewInstance();

        MembershipService membershipService = instanceOne.getContext().getService(MembershipService.NAME);
        RoutingTable routingTable = membershipService.getRoutingTable();

        assertEquals(membershipService.getKnownCoordinator(), routingTable.getCoordinator());

        Set<String> addresses = new HashSet<>();
        addresses.add(instanceOne.getContext().getMember().getAddress().toString());
        addresses.add(instanceTwo.getContext().getMember().getAddress().toString());

        int numberOfShards = membershipService.getContext().getConfig().getInt("cluster.number_of_shards");
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            Route route = membershipService.getRoutingTable().getRoute(shardId);
            assertNotNull(route);
            assertTrue(addresses.contains(route.getMember().getAddress().toString()));
        }
    }
}
