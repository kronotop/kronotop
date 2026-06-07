/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class FailureDetectionTaskTest extends BaseClusterTest {

    @Test
    void shouldMarkMemberAsDeadWhenSilentPeriodExceeded() {
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        // Wait for the second member to appear in others
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        // Verify the member is initially alive
        MemberView view = membership.getKnownMembers().get(second.getMember());
        assertTrue(view.isAlive());

        // Shutdown second member to stop its heartbeats
        second.shutdownWithoutCleanup();
        kronotopInstances.remove(second.getMember());

        // Simulate failure detection by calling checkClusterMembers repeatedly
        // with a very short maxSilentPeriod (1) to quickly mark member as dead
        for (int i = 0; i < 3; i++) {
            membership.checkClusterMembers(1);
        }

        // Verify member is marked as dead
        assertFalse(view.isAlive());
    }

    @Test
    void shouldKeepMemberAliveWhenHeartbeatIsUpdating() {
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        // Wait for second member to appear in others
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        MemberView view = membership.getKnownMembers().get(second.getMember());
        assertTrue(view.isAlive());

        // Wait for heartbeat to update
        long initialHeartbeat = view.getLatestHeartbeat();
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getLatestHeartbeat(second.getMember()) > initialHeartbeat
        );

        // Call checkClusterMembers multiple times - member should stay alive
        for (int i = 0; i < 5; i++) {
            membership.checkClusterMembers(20);
        }

        assertTrue(view.isAlive());
    }

    @Test
    void shouldReincarnateDeadMemberWhenHeartbeatResumes() {
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        // Wait for second member to appear in others
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        MemberView view = membership.getKnownMembers().get(second.getMember());
        assertTrue(view.isAlive());

        // Manually mark member as dead
        view.setAlive(false);
        assertFalse(view.isAlive());

        // Wait for heartbeat to update (simulating recovery)
        long currentHeartbeat = view.getLatestHeartbeat();
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getLatestHeartbeat(second.getMember()) > currentHeartbeat
        );

        // Call checkClusterMembers - member should be reincarnated
        // Use a large maxSilentPeriod so the member is considered alive again
        membership.checkClusterMembers(100);

        assertTrue(view.isAlive());
    }

    @Test
    void shouldUpdateLatestHeartbeatWhenChanged() {
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        // Wait for the second member to appear in others
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        MemberView view = membership.getKnownMembers().get(second.getMember());
        long initialHeartbeat = view.getLatestHeartbeat();

        // Wait for heartbeat to be updated in FoundationDB
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            membership.checkClusterMembers(100);
            return view.getLatestHeartbeat() > initialHeartbeat;
        });

        assertTrue(view.getLatestHeartbeat() > initialHeartbeat);
    }

    @Test
    void shouldIncreaseExpectedHeartbeatWhenNoChange() {
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        // Wait for second member to appear in others
        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        MemberView view = membership.getKnownMembers().get(second.getMember());

        // First call updates the latestHeartbeat from FoundationDB
        membership.checkClusterMembers(100);

        long initialExpectedHeartbeat = view.getExpectedHeartbeat();

        // Second call immediately - heartbeat hasn't changed, so expectedHeartbeat should increase
        membership.checkClusterMembers(100);

        assertTrue(view.getExpectedHeartbeat() > initialExpectedHeartbeat);
    }

    @Test
    void shouldDetectMultipleDeadMembersInSingleCheck() {
        // Behavior: A single checkClusterMembers call processes ALL members,
        // not just the first one that exceeds the silent period threshold.
        KronotopTestInstance second = addNewInstance();
        KronotopTestInstance third = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember()) &&
                        membership.getKnownMembers().containsKey(third.getMember())
        );

        MemberView secondView = membership.getKnownMembers().get(second.getMember());
        MemberView thirdView = membership.getKnownMembers().get(third.getMember());
        assertTrue(secondView.isAlive());
        assertTrue(thirdView.isAlive());

        // With maxSilentPeriod=0, any member with silentPeriod > 0 is marked dead.
        // After computeIfPresent, silentPeriod is always >= 1.
        // A single call should mark ALL members as dead, not just the first one.
        membership.checkClusterMembers(0);

        assertFalse(secondView.isAlive());
        assertFalse(thirdView.isAlive());
    }

    @Test
    void shouldReincarnateOrganicallyDeadMember() {
        // Behavior: A member marked dead due to heartbeat timeout should be reincarnated
        // when its FDB heartbeat resumes changing. This tests that dead members' heartbeat
        // values are updated from FDB (enabling reincarnation), while increaseExpectedHeartbeat
        // is not called for dead members.
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        MemberView view = membership.getKnownMembers().get(second.getMember());
        assertTrue(view.isAlive());

        // Simulate organic death: inflate expectedHeartbeat so silentPeriod exceeds threshold
        for (int i = 0; i < 50; i++) {
            view.increaseExpectedHeartbeat();
        }
        view.setAlive(false);
        assertFalse(view.isAlive());

        long deadSilentPeriod = view.getExpectedHeartbeat() - view.getLatestHeartbeat();
        assertTrue(deadSilentPeriod > 10);

        // The second member is still alive and sending heartbeats to FDB.
        // Wait for a heartbeat update to ensure FDB value differs from the view's frozen value.
        long frozenHeartbeat = view.getLatestHeartbeat();
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            KronotopDirectoryNode directory = KronotopDirectory.kronotop()
                    .cluster(first.getContext().getClusterName())
                    .metadata().members().member(second.getMember().getId());
            try (Transaction tr = first.getContext().getFoundationDB().createTransaction()) {
                DirectorySubspace subspace = first.getContext().getDirectoryLayer().open(tr, directory.toList()).join();
                return Heartbeat.get(tr, subspace) > frozenHeartbeat;
            }
        });

        // checkClusterMembers should read the new FDB heartbeat, update the dead member's
        // view (resetting silentPeriod to 1), and reincarnate it.
        membership.checkClusterMembers(10);

        assertTrue(view.isAlive());
    }

    @Test
    void shouldNotIncrementExpectedHeartbeatForDeadMembers() {
        // Behavior: When a dead member's FDB heartbeat has not changed,
        // increaseExpectedHeartbeat should not be called.
        KronotopTestInstance second = addNewInstance();

        KronotopTestInstance first = getInstances().getFirst();
        MembershipService membership = first.getContext().getService(MembershipService.NAME);

        await().atMost(5, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(second.getMember())
        );

        // Stop the second member so its FDB heartbeat freezes
        second.shutdownWithoutCleanup();
        kronotopInstances.remove(second.getMember());

        MemberView view = membership.getKnownMembers().get(second.getMember());

        // Sync the view with the frozen FDB heartbeat value
        membership.checkClusterMembers(100);

        // Mark member as dead
        view.setAlive(false);

        long initialExpectedHeartbeat = view.getExpectedHeartbeat();
        long initialLatestHeartbeat = view.getLatestHeartbeat();

        // Call checkClusterMembers - dead member with unchanged FDB heartbeat
        // should not have increaseExpectedHeartbeat called
        membership.checkClusterMembers(100);

        assertEquals(initialExpectedHeartbeat, view.getExpectedHeartbeat());
        assertEquals(initialLatestHeartbeat, view.getLatestHeartbeat());
    }
}
