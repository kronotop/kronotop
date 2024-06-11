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

package com.kronotop.network;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class AddressTest {
    @Test
    public void testAddress() throws UnknownHostException {
        Address addr = new Address("localhost", 5484);
        assertEquals("localhost", addr.getHost());
        assertEquals(5484, addr.getPort());
    }

    @Test
    public void testFindAvailablePort() throws UnknownHostException {
        Address addr = new Address("localhost", 0);
        assertNotEquals(0, addr.getPort());
    }

    @Test
    public void testHashCode() throws UnknownHostException {
        Address addr = new Address("localhost", 0);
        assertNotEquals(0, addr.hashCode());
    }

    @Test
    public void testEquals() throws UnknownHostException {
        Address one = new Address("localhost", 5484);
        Address two = new Address("localhost", 5484);
        assertEquals(one, two);
    }

    @Test
    public void testResolve() {
        InetSocketAddress sockAddr = new InetSocketAddress(5484);
        Address addr = new Address(sockAddr);
        assertEquals(5484, addr.getPort());
    }
}
