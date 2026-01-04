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

package com.kronotop.cluster.client;

import com.kronotop.BaseClusterTestWithTCPServer;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Member;
import com.kronotop.server.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InternalClientPoolTest extends BaseClusterTestWithTCPServer {
    private InternalClientPool pool;

    @BeforeEach
    void setupPool() {
        pool = new InternalClientPool();
    }

    @AfterEach
    void teardownPool() {
        if (pool != null) {
            pool.shutdown();
        }
    }

    @Test
    void shouldGetStringConnection() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<String, String> connection = pool.get(member);

        assertNotNull(connection);
        assertEquals(Response.PONG, connection.sync().ping());
    }

    @Test
    void shouldGetByteArrayConnection() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<byte[], byte[]> connection = pool.getByteArray(member);

        assertNotNull(connection);
        assertEquals(Response.PONG, connection.sync().ping());
    }

    @Test
    void shouldReturnSameConnectionForSameMember() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<String, String> first = pool.get(member);
        StatefulInternalConnection<String, String> second = pool.get(member);

        assertSame(first, second);
    }

    @Test
    void shouldReturnSameByteArrayConnectionForSameMember() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<byte[], byte[]> first = pool.getByteArray(member);
        StatefulInternalConnection<byte[], byte[]> second = pool.getByteArray(member);

        assertSame(first, second);
    }

    @Test
    void shouldReturnDifferentConnectionsForDifferentCodecs() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<String, String> stringConn = pool.get(member);
        StatefulInternalConnection<byte[], byte[]> byteArrayConn = pool.getByteArray(member);

        assertNotNull(stringConn);
        assertNotNull(byteArrayConn);
        // Both should work independently
        assertEquals(Response.PONG, stringConn.sync().ping());
        assertEquals(Response.PONG, byteArrayConn.sync().ping());
    }

    @Test
    void shouldEvictConnection() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<String, String> first = pool.get(member);
        assertNotNull(first);

        pool.evict(member);

        // After eviction, should get a new connection
        StatefulInternalConnection<String, String> second = pool.get(member);
        assertNotNull(second);
        assertNotSame(first, second);
    }

    @Test
    void shouldEvictBothCodecConnectionsForMember() {
        KronotopTestInstance instance = getInstances().getFirst();
        Member member = instance.getMember();

        StatefulInternalConnection<String, String> stringFirst = pool.get(member);
        StatefulInternalConnection<byte[], byte[]> byteArrayFirst = pool.getByteArray(member);

        pool.evict(member);

        StatefulInternalConnection<String, String> stringSecond = pool.get(member);
        StatefulInternalConnection<byte[], byte[]> byteArraySecond = pool.getByteArray(member);

        assertNotSame(stringFirst, stringSecond);
        assertNotSame(byteArrayFirst, byteArraySecond);
    }

    @Test
    void shouldReturnDifferentConnectionsForDifferentMembers() {
        KronotopTestInstance firstInstance = getInstances().getFirst();
        KronotopTestInstance secondInstance = addNewInstance(true);

        Member firstMember = firstInstance.getMember();
        Member secondMember = secondInstance.getMember();

        StatefulInternalConnection<String, String> firstConn = pool.get(firstMember);
        StatefulInternalConnection<String, String> secondConn = pool.get(secondMember);

        assertNotSame(firstConn, secondConn);
        // Both should be functional
        assertEquals(Response.PONG, firstConn.sync().ping());
        assertEquals(Response.PONG, secondConn.sync().ping());
    }
}
