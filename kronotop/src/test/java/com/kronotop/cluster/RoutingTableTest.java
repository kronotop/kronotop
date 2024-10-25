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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoutingTableTest {
    private final MockProcessIdGeneratorImpl processIdGenerator = new MockProcessIdGeneratorImpl();

    private RouteLegacy newRoute() throws UnknownHostException {
        Member member = new Member(
                UUID.randomUUID().toString(),
                Address.parseString("localhost:3320"),
                Address.parseString("localhost:3321"),
                processIdGenerator.getProcessID()
        );
        return new RouteLegacy(member);
    }

    @Test
    public void test_setRoute() throws UnknownHostException {
        RoutingTableLegacy routingTable = new RoutingTableLegacy();
        RouteLegacy route = newRoute();

        assertDoesNotThrow(() -> routingTable.setRoute(1, route));
    }

    @Test
    public void test_getRoute() throws UnknownHostException {
        RoutingTableLegacy routingTable = new RoutingTableLegacy();
        RouteLegacy route = newRoute();

        routingTable.setRoute(1, route);
        RouteLegacy receivedRoute = routingTable.getRoute(1);
        assertEquals(route, receivedRoute);
    }

    @Test
    public void test_getVersion() throws UnknownHostException {
        RoutingTableLegacy routingTable = new RoutingTableLegacy();
        RouteLegacy route = newRoute();

        routingTable.setRoute(1, route);
        assertEquals(1, routingTable.getVersion());
    }

    @Test
    public void test_encode() throws UnknownHostException, JsonProcessingException {
        RoutingTableLegacy routingTable = new RoutingTableLegacy();
        RouteLegacy route = newRoute();
        routingTable.setRoute(1, route);
        routingTable.setRoute(2, route);

        ObjectMapper objectMapper = new ObjectMapper();
        String data = objectMapper.writeValueAsString(routingTable);

        RoutingTableLegacy decoded = objectMapper.readValue(data, RoutingTableLegacy.class);
        assertEquals(routingTable, decoded);
        assertEquals(routingTable.getVersion(), decoded.getVersion());

        assertEquals(routingTable.getRoute(1), decoded.getRoute(1));
        assertEquals(routingTable.getRoute(2), decoded.getRoute(2));
    }
}
