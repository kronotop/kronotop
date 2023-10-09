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

package com.kronotop.core.cluster.consistent;

import com.kronotop.ConfigTestUtil;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.network.Address;
import com.typesafe.config.Config;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ConsistentTest {
    protected Config config = ConfigTestUtil.load("test.conf");

    @Test
    public void testAdd() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address address = new Address("localhost", 0);
        Member member = new Member(address, Instant.now().toEpochMilli());
        consistent.addMember(member);

        Member targetMember = consistent.locate("foobar");
        assertEquals(member.getId(), targetMember.getId());
    }

    @Test
    public void testGetMembers() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address addressOne = new Address("localhost", 0);
        Member memberOne = new Member(addressOne, Instant.now().toEpochMilli());
        consistent.addMember(memberOne);

        Address addressTwo = new Address("localhost", 0);
        Member memberTwo = new Member(addressTwo, Instant.now().toEpochMilli());
        consistent.addMember(memberTwo);

        List<Member> expectedMembers = new ArrayList<>();
        expectedMembers.add(memberOne);
        expectedMembers.add(memberTwo);

        List<Member> members = consistent.getMembers();
        assertEquals(2, members.size());

        assertTrue(expectedMembers.containsAll(members));
    }

    @Test
    public void testAverageLoad() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address address = new Address("localhost", 0);
        Member member = new Member(address, Instant.now().toEpochMilli());
        consistent.addMember(member);

        assertTrue(consistent.averageLoad() > 0);
    }

    @Test
    public void testGetPartitionOwner() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address addressOne = new Address("localhost", 0);
        Member memberOne = new Member(addressOne, Instant.now().toEpochMilli());
        consistent.addMember(memberOne);

        Address addressTwo = new Address("localhost", 0);
        Member memberTwo = new Member(addressTwo, Instant.now().toEpochMilli());
        consistent.addMember(memberTwo);

        List<Member> expectedMembers = new ArrayList<>();
        expectedMembers.add(memberOne);
        expectedMembers.add(memberTwo);

        double partitionCount = config.getDouble("cluster.partition_count");

        List<Member> result = new ArrayList<>();
        for (int partID = 0; partID < partitionCount; partID++) {
            Member owner = consistent.getPartitionOwner(partID);
            result.add(owner);
        }
        assertTrue(expectedMembers.containsAll(result));
    }

    @Test
    public void testLocate_EmptyHashRing() {
        Consistent consistent = new Consistent(config);
        NoPartitionOwnerFoundException exception = assertThrows(
                NoPartitionOwnerFoundException.class,
                () -> consistent.locate("foobar")
        );
        assertNotNull(exception);
    }

    @Test
    public void testLocate() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address addressOne = new Address("localhost", 0);
        Member memberOne = new Member(addressOne, Instant.now().toEpochMilli());
        consistent.addMember(memberOne);

        Address addressTwo = new Address("localhost", 0);
        Member memberTwo = new Member(addressTwo, Instant.now().toEpochMilli());
        consistent.addMember(memberTwo);

        Set<String> members = new HashSet<>();
        members.add(memberOne.getId());
        members.add(memberTwo.getId());

        Member owner = consistent.locate("foobar");
        assertTrue(members.contains(owner.getId()));
    }

    @Test
    public void testLoadDistribution() {
        Consistent consistent = new Consistent(config);

        for (int i = 1; i <= 10; i++) {
            try {
                Address address = new Address("localhost", 0);
                Member m = new Member(address, Instant.now().toEpochMilli());
                consistent.addMember(m);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        double averageLoad = consistent.averageLoad();
        HashMap<Member, Double> loads = consistent.loadDistribution();
        for (Double load : loads.values()) {
            assertTrue(load <= averageLoad);
        }
    }

    @Test
    public void testRemoveMember_EmptyHashRing() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address address = new Address("localhost", 0);
        Member member = new Member(address, Instant.now().toEpochMilli());

        assertDoesNotThrow(() -> consistent.removeMember(member));
    }

    @Test
    public void testRemove() throws UnknownHostException {
        Consistent consistent = new Consistent(config);

        Address addressOne = new Address("localhost", 0);
        Member memberOne = new Member(addressOne, Instant.now().toEpochMilli());
        consistent.addMember(memberOne);

        Address addressTwo = new Address("localhost", 0);
        Member memberTwo = new Member(addressTwo, Instant.now().toEpochMilli());
        consistent.addMember(memberTwo);

        consistent.removeMember(memberTwo);

        Member owner = consistent.locate("foobar");
        assertEquals(memberOne, owner);
    }

    @Test
    public void testConsistentWithInitialMembers() {
        List<Member> members = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            try {
                Address address = new Address("localhost", 0);
                Member m = new Member(address, Instant.now().toEpochMilli());
                members.add(m);
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }

        Consistent consistent = new Consistent(config, members);
        List<Member> currentMembers = consistent.getMembers();
        assertEquals(10, members.size());
        assertTrue(currentMembers.containsAll(members));
    }
}
