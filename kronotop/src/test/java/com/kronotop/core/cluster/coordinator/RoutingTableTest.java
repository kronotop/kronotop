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

package com.kronotop.core.cluster.coordinator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.MockProcessIdGeneratorImpl;
import com.kronotop.core.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoutingTableTest {
    private final MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();

    private Route newRoute() throws UnknownHostException {
        Member member = new Member(Address.parseString("localhost:3320"), processIdGenerator.getProcessID());
        return new Route(member);
    }

    @Test
    public void test_setRoute() throws UnknownHostException {
        RoutingTable routingTable = new RoutingTable();
        Route route = newRoute();

        assertDoesNotThrow(() -> routingTable.setRoute(1, route));
    }

    @Test
    public void test_getRoute() throws UnknownHostException {
        RoutingTable routingTable = new RoutingTable();
        Route route = newRoute();

        routingTable.setRoute(1, route);
        Route receivedRoute = routingTable.getRoute(1);
        assertEquals(route, receivedRoute);
    }

    @Test
    public void test_getVersion() throws UnknownHostException {
        RoutingTable routingTable = new RoutingTable();
        Route route = newRoute();

        routingTable.setRoute(1, route);
        assertEquals(1, routingTable.getVersion());
    }

    @Test
    public void test_setCoordinator() throws UnknownHostException {
        RoutingTable routingTable = new RoutingTable();

        Member member = new Member(Address.parseString("localhost:3320"), processIdGenerator.getProcessID());
        assertDoesNotThrow(() -> routingTable.updateCoordinator(member));
    }

    @Test
    public void test_getCoordinator() throws UnknownHostException {
        RoutingTable routingTable = new RoutingTable();

        Member member = new Member(Address.parseString("localhost:3320"), processIdGenerator.getProcessID());
        routingTable.updateCoordinator(member);

        assertEquals(member, routingTable.getCoordinator());
    }

    @Test
    public void test_encode() throws UnknownHostException, JsonProcessingException {
        RoutingTable routingTable = new RoutingTable();
        Route route = newRoute();
        routingTable.updateCoordinator(route.getMember());
        routingTable.setRoute(1, route);
        routingTable.setRoute(2, route);

        ObjectMapper objectMapper = new ObjectMapper();
        String data = objectMapper.writeValueAsString(routingTable);

        RoutingTable decoded = objectMapper.readValue(data, RoutingTable.class);
        assertEquals(routingTable, decoded);
        assertEquals(routingTable.getVersion(), decoded.getVersion());

        assertEquals(routingTable.getRoute(1), decoded.getRoute(1));
        assertEquals(routingTable.getRoute(2), decoded.getRoute(2));
    }
}
