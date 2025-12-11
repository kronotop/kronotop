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

package com.kronotop.cluster;

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

class MemberRegistryTest extends BaseStandaloneInstanceTest {

    @Test
    void shouldAddMember() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        DirectorySubspace subspace = assertDoesNotThrow(() -> registry.add(member));
        assertNotNull(subspace);
    }

    @Test
    void shouldThrowMemberAlreadyRegisteredExceptionWhenAddingDuplicate() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));
        assertThrows(MemberAlreadyRegisteredException.class, () -> registry.add(member));
    }

    @Test
    void shouldRemoveMember() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));
        assertDoesNotThrow(() -> registry.remove(member.getId()));
    }

    @Test
    void shouldThrowMemberNotRegisteredExceptionWhenRemovingUnknown() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertThrows(MemberNotRegisteredException.class, () -> registry.remove(member.getId()));
    }

    @Test
    void shouldReturnFalseWhenMemberNotAdded() {
        MemberRegistry members = new MemberRegistry(context);
        assertFalse(members.isAdded(MemberIdGenerator.generateId()));
    }

    @Test
    void shouldReturnTrueWhenMemberAdded() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));
        assertTrue(registry.isAdded(member.getId()));
    }

    @Test
    void shouldSetMemberStatus() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));

        Member updatedMember = registry.setStatus(member.getId(), MemberStatus.RUNNING);
        assertEquals(MemberStatus.RUNNING, updatedMember.getStatus());
    }

    @Test
    void shouldFindMember() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> registry.add(member));

        assertEquals(member, registry.findMember(member.getId()));
    }

    @Test
    void shouldThrowMemberNotRegisteredExceptionWhenFindingUnknown() {
        MemberRegistry registry = new MemberRegistry(context);
        assertThrows(MemberNotRegisteredException.class, () -> registry.findMember(MemberIdGenerator.generateId()));
    }

    @Test
    void shouldListMembers() throws UnknownHostException {
        MemberRegistry registry = new MemberRegistry(context);
        Member one = createMemberWithEphemeralPort();
        Member two = createMemberWithEphemeralPort();
        registry.add(one);
        registry.add(two);

        TreeSet<Member> members = registry.listMembers();
        assertEquals(3, members.size());
        assertTrue(members.contains(context.getMember()));
        assertTrue(members.contains(one));
        assertTrue(members.contains(two));
    }
}
