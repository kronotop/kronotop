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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseMetadataStoreTest;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

public class MemberRegistryTest extends BaseMetadataStoreTest {

    @Test
    public void test_add() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        DirectorySubspace subspace = assertDoesNotThrow(() -> registry.add(member));
        assertNotNull(subspace);
    }

    @Test
    public void test_add_throws_MemberAlreadyRegisteredException() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));
        assertThrows(MemberAlreadyRegisteredException.class, () -> registry.add(member));
    }

    @Test
    public void test_remove() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));
        assertDoesNotThrow(() -> registry.remove(member.getId()));
    }

    @Test
    public void test_delete_throws_MemberNotRegisteredException() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertThrows(MemberNotRegisteredException.class, () -> registry.remove(member.getId()));
    }

    @Test
    public void test_isAdded_false() {
        MemberRegistry members = new MemberRegistry(context);
        assertFalse(members.isAdded(MemberIdGenerator.generateId()));
    }

    @Test
    public void test_isAdded_true() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));
        assertTrue(registry.isAdded(member.getId()));
    }

    @Test
    public void test_setStatus() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));

        Member updatedMember = registry.setStatus(member.getId(), MemberStatus.RUNNING);
        assertEquals(MemberStatus.RUNNING, updatedMember.getStatus());
    }

    @Test
    public void test_findMember() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));

        assertEquals(member, registry.findMember(member.getId()));
    }

    @Test
    public void test_findMember_MemberNotRegisteredException() {
        MemberRegistry registry = new MemberRegistry(context);
        assertThrows(MemberNotRegisteredException.class, () -> registry.findMember(MemberIdGenerator.generateId()));
    }

    @Test
    public void test_listMembers() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member one = createMemberWithEphemeralPort();
        Member two = createMemberWithEphemeralPort();
        registry.add(one);
        registry.add(two);

        TreeSet<Member> members = registry.listMembers();
        assertEquals(2, members.size());
        assertTrue(members.contains(one));
        assertTrue(members.contains(two));
    }
}
