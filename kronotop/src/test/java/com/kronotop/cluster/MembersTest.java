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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class MembersTest extends BaseMetadataStoreTest {

    @Test
    public void test_register() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        DirectorySubspace subspace = assertDoesNotThrow(() -> members.register(member));
        assertNotNull(subspace);
    }

    @Test
    public void test_register_throws_MemberAlreadyRegisteredException() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> members.register(member));
        assertThrows(MemberAlreadyRegisteredException.class, () -> members.register(member));
    }

    @Test
    public void test_unregister() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> members.register(member));
        assertDoesNotThrow(() -> members.unregister(member.getId()));
    }

    @Test
    public void test_unregister_throws_MemberNotRegisteredException() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        assertThrows(MemberNotRegisteredException.class, () -> members.unregister(member.getId()));
    }

    @Test
    public void test_isRegistered_false() {
        Members members = new Members(context);
        assertFalse(members.isRegistered(UUID.randomUUID().toString()));
    }

    @Test
    public void test_isRegistered_true() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> members.register(member));
        assertTrue(members.isRegistered(member.getId()));
    }

    @Test
    public void test_setStatus() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> members.register(member));

        Member updatedMember = members.setStatus(member.getId(), MemberStatus.RUNNING);
        assertEquals(MemberStatus.RUNNING, updatedMember.getStatus());
    }

    @Test
    public void test_getMember() throws UnknownHostException {
        Members members = new Members(context);
        Member member = createMemberWithEphemeralPort();
        assertDoesNotThrow(() -> members.register(member));

        assertEquals(member, members.getMember(member.getId()));
    }

    @Test
    public void test_getMember_MemberNotRegisteredException() {
        Members members = new Members(context);
        assertThrows(MemberNotRegisteredException.class, () -> members.getMember(UUID.randomUUID().toString()));
    }
}
