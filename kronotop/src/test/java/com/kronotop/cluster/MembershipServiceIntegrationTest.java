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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.network.Address;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;


class MembershipServiceIntegrationTest extends BaseClusterTest {

    @Test
    void shouldGetLatestHeartbeat() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        await().atMost(5, TimeUnit.SECONDS).until(() -> membership.getLatestHeartbeat(instance.getContext().getMember()) > 0);
    }

    @Test
    void shouldListMembers() {
        addNewInstance();

        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        TreeSet<Member> members = membership.listMembers();
        assertEquals(2, members.size());
    }

    @Test
    void shouldFindMemberById() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        Member expected = instance.getContext().getMember();

        Member result = membership.findMember(expected.getId());
        assertEquals(expected, result);
    }

    @Test
    void shouldThrowExceptionWhenFindingUnregisteredMember() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        assertThrows(MemberNotRegisteredException.class, () -> membership.findMember(MemberIdGenerator.generateId()));
    }

    @Test
    void shouldReturnTrueWhenMemberIsRegistered() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        assertTrue(membership.isMemberRegistered(instance.getContext().getMember().getId()));
    }

    @Test
    void shouldReturnFalseWhenMemberIsNotRegistered() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        assertFalse(membership.isMemberRegistered(MemberIdGenerator.generateId()));
    }

    @Test
    void shouldUpdateMemberAttributes() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        Member member = instance.getContext().getMember();

        Member updatedMember = new Member(
                member.getId(),
                member.getExternalAddress(),
                member.getInternalAddress(),
                member.getProcessId()
        );
        updatedMember.setStatus(MemberStatus.UNAVAILABLE);

        instance.getContext().getFoundationDB().run(tr -> {
            membership.updateMember(tr, updatedMember);
            return null;
        });

        Member result = membership.findMember(member.getId());
        assertEquals(MemberStatus.UNAVAILABLE, result.getStatus());
    }

    @Test
    void shouldThrowExceptionWhenUpdatingUnregisteredMember() throws UnknownHostException {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        Member unregisteredMember = new Member(
                MemberIdGenerator.generateId(),
                new Address("127.0.0.1", 5484),
                new Address("127.0.0.1", 3320),
                instance.getContext().getMember().getProcessId()
        );

        CompletionException exception = assertThrows(CompletionException.class, () ->
                instance.getContext().getFoundationDB().run(tr -> {
                    membership.updateMember(tr, unregisteredMember);
                    return null;
                })
        );
        assertInstanceOf(MemberNotRegisteredException.class, exception.getCause());
    }

    @Test
    void shouldRemoveMemberFromCluster() {
        addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        first.shutdownWithoutCleanup();
        kronotopInstances.remove(first.getMember());

        KronotopTestInstance second = getInstances().getFirst();
        MembershipService membership = second.getContext().getService(MembershipService.NAME);

        // Verify member is stopped before removal
        Member stoppedMember = membership.findMember(first.getMember().getId());
        assertEquals(MemberStatus.STOPPED, stoppedMember.getStatus());

        // Remove the stopped member
        second.getContext().getFoundationDB().run(tr -> {
            membership.removeMember(tr, first.getMember().getId());
            return null;
        });

        // Verify member is no longer registered
        assertFalse(membership.isMemberRegistered(first.getMember().getId()));
    }

    @Test
    void shouldThrowExceptionWhenRemovingUnregisteredMember() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        String unregisteredMemberId = MemberIdGenerator.generateId();

        CompletionException exception = assertThrows(CompletionException.class, () ->
                instance.getContext().getFoundationDB().run(tr -> {
                    membership.removeMember(tr, unregisteredMemberId);
                    return null;
                })
        );
        assertInstanceOf(MemberNotRegisteredException.class, exception.getCause());
    }

    @Test
    void shouldReturnOtherMembersInCluster() {
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        await().atMost(5, TimeUnit.SECONDS).until(() -> membership.getKnownMembers().containsKey(second.getMember()));

        Map<Member, MemberView> others = membership.getKnownMembers();
        assertTrue(others.containsKey(second.getMember()));
    }

    @Test
    void shouldReturnOnlySelfWhenNoOtherMembers() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        Map<Member, MemberView> others = membership.getKnownMembers();
        assertEquals(1, others.size());
        assertTrue(others.containsKey(instance.getMember()));
    }

    @Test
    void shouldUpdateMemberStatusAfterShutdown() {
        addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();

        first.shutdownWithoutCleanup();
        kronotopInstances.remove(first.getMember());

        KronotopTestInstance second = getInstances().getFirst();
        MembershipService membership = second.getContext().getService(MembershipService.NAME);
        TreeSet<Member> members = membership.listMembers();
        assertEquals(2, members.size());

        MemberStatus status = MemberStatus.UNKNOWN;
        for (Member member : members) {
            if (member.equals(first.getMember())) {
                status = member.getStatus();
            }
        }
        assertEquals(MemberStatus.STOPPED, status);
    }

    @Test
    void shouldPublishMemberJoinEventWhenNewMemberJoins() {
        KronotopTestInstance first = getInstances().getFirst();
        MembershipService firstMembership = first.getContext().getService(MembershipService.NAME);

        // Initially only self in others
        assertEquals(1, firstMembership.getKnownMembers().size());

        // Add a new member which publishes MemberJoinEvent
        KronotopTestInstance second = addNewInstance();

        // Wait for the first member to process the MemberJoinEvent
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                firstMembership.getKnownMembers().containsKey(second.getMember())
        );

        // Verify the new member is now tracked
        assertTrue(firstMembership.getKnownMembers().containsKey(second.getMember()));
        assertEquals(2, firstMembership.listMembers().size());
    }

    @Test
    void shouldProcessMemberJoinEventFromOtherMember() {
        KronotopTestInstance first = getInstances().getFirst();
        MembershipService firstMembership = first.getContext().getService(MembershipService.NAME);

        KronotopTestInstance second = addNewInstance();

        // Wait for first member to process the MemberJoinEvent from second member
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                firstMembership.getKnownMembers().containsKey(second.getMember())
        );

        // Verify member view is initialized correctly
        MemberView view = firstMembership.getKnownMembers().get(second.getMember());
        assertNotNull(view);
        assertTrue(view.isAlive());
    }

    @Test
    void shouldUpdateHeartbeatPeriodically() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);
        Member member = instance.getMember();

        // Wait for initial heartbeat
        await().atMost(5, TimeUnit.SECONDS).until(() -> membership.getLatestHeartbeat(member) > 0);

        long firstHeartbeat = membership.getLatestHeartbeat(member);

        // Wait for heartbeat to be updated (interval is 1 second in test config)
        await().atMost(5, TimeUnit.SECONDS).until(() -> membership.getLatestHeartbeat(member) > firstHeartbeat);

        long secondHeartbeat = membership.getLatestHeartbeat(member);
        assertTrue(secondHeartbeat > firstHeartbeat);
    }

    @Test
    void shouldTriggerTopologyWatcherOnMemberChange() {
        KronotopTestInstance instance = getInstances().getFirst();
        MembershipService membership = instance.getContext().getService(MembershipService.NAME);

        DirectorySubspace subspace = instance.getContext().getDirectorySubspaceCache()
                .get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_TOPOLOGY_CHANGED));

        // Get initial counter value (little-endian 32-bit integer from atomic ADD)
        Long initialValue = instance.getContext().getFoundationDB().run(tr -> {
            byte[] value = tr.get(key).join();
            if (value == null) {
                return 0L;
            }
            return (long) ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
        });

        // Trigger topology watcher
        instance.getContext().getFoundationDB().run(tr -> {
            membership.triggerClusterTopologyWatcher(tr);
            return null;
        });

        // Verify counter was incremented
        Long newValue = instance.getContext().getFoundationDB().run(tr -> {
            byte[] value = tr.get(key).join();
            if (value == null) {
                return 0L;
            }
            return (long) ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getInt();
        });

        assertTrue(newValue > initialValue);
    }
}
