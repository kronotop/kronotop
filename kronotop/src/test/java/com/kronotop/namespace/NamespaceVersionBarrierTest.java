/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.namespace;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BarrierNotSatisfiedException;
import com.kronotop.BaseClusterTest;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.namespace.handlers.NamespaceMetadata;
import com.kronotop.namespace.handlers.NamespaceRemovedEvent;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NamespaceVersionBarrierTest extends BaseClusterTest {

    @Test
    void shouldSatisfyBarrierWhenAllMembersHaveTargetVersion() {
        // Add a second instance to have multiple members
        addNewInstance();

        KronotopTestInstance firstInstance = getInstances().getFirst();
        Context context = firstInstance.getContext();
        String namespace = "test.barrier.namespace";

        // Create a namespace and set removed (version → 1)
        NamespaceUtil.create(context, namespace);
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        // For each member, set the lastSeenNamespaceVersion to 1
        NamespaceRemovedEvent event = new NamespaceRemovedEvent(metadata.id(), namespace);
        for (KronotopTestInstance instance : getInstances()) {
            Context instanceContext = instance.getContext();
            try (Transaction tr = instanceContext.getFoundationDB().createTransaction()) {
                List<String> memberSubpath = KronotopDirectory.
                        kronotop().
                        cluster(instanceContext.getClusterName()).
                        metadata().
                        members().
                        member(instanceContext.getMember().getId()).toList();
                DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();
                NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, instanceContext.getClusterName(), event);
                tr.commit().join();
            }
        }

        // Re-read metadata to get an updated version
        NamespaceMetadata updatedMetadata = NamespaceUtil.readMetadata(context, namespace);

        // Create the barrier and await - should succeed without throwing
        NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, updatedMetadata);
        assertDoesNotThrow(() -> barrier.await(1, 3, Duration.ofMillis(100)));
    }

    @Test
    void shouldSatisfyBarrierWhenMembersHaveHigherVersion() {
        // Add a second instance to have multiple members
        addNewInstance();

        KronotopTestInstance firstInstance = getInstances().getFirst();
        Context context = firstInstance.getContext();
        String namespace = "test.barrier.higher.version";

        // Create a namespace (version=0), then increment the version twice (version → 2)
        NamespaceUtil.create(context, namespace);
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        NamespaceUtil.setRemoved(context, namespace); // version → 1
        NamespaceUtil.incrementVersion(context, namespace); // version → 2

        // For each member, set the lastSeenNamespaceVersion (will be 2)
        NamespaceRemovedEvent event = new NamespaceRemovedEvent(metadata.id(), namespace);
        for (KronotopTestInstance instance : getInstances()) {
            Context instanceContext = instance.getContext();
            try (Transaction tr = instanceContext.getFoundationDB().createTransaction()) {
                List<String> memberSubpath = KronotopDirectory.
                        kronotop().
                        cluster(instanceContext.getClusterName()).
                        metadata().
                        members().
                        member(instanceContext.getMember().getId()).toList();
                DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();
                NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, instanceContext.getClusterName(), event);
                tr.commit().join();
            }
        }

        // Re-read metadata to get an updated version
        NamespaceMetadata updatedMetadata = NamespaceUtil.readMetadata(context, namespace);
        assertEquals(2, updatedMetadata.version());

        // Create the barrier and await with targetVersion=1 - should succeed since members have version 2 >= 1
        NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, updatedMetadata);
        assertDoesNotThrow(() -> barrier.await(1, 3, Duration.ofMillis(100)));
    }

    @Test
    void shouldThrowBarrierNotSatisfiedWhenNoMemberHasTargetVersion() {
        // Add a second instance to have multiple members
        addNewInstance();

        KronotopTestInstance firstInstance = getInstances().getFirst();
        Context context = firstInstance.getContext();
        String namespace = "test.barrier.no.version";

        // Create a namespace and set removed (version → 1)
        NamespaceUtil.create(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        // Re-read metadata to get an updated version
        NamespaceMetadata updatedMetadata = NamespaceUtil.readMetadata(context, namespace);
        assertEquals(1, updatedMetadata.version());

        // Don't set the lastSeenNamespaceVersion for any member
        // Create a barrier and await - should throw BarrierNotSatisfiedException
        NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, updatedMetadata);
        assertThrows(BarrierNotSatisfiedException.class, () -> barrier.await(1, 2, Duration.ofMillis(100)));
    }

    @Test
    void shouldThrowBarrierNotSatisfiedWhenSomeMembersLagBehind() {
        // Add a second instance to have multiple members
        addNewInstance();

        KronotopTestInstance firstInstance = getInstances().getFirst();
        Context context = firstInstance.getContext();
        String namespace = "test.barrier.partial.version";

        // Create a namespace and set removed (version → 1)
        NamespaceUtil.create(context, namespace);
        NamespaceMetadata metadata = NamespaceUtil.readMetadata(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        // Set the lastSeenNamespaceVersion for only the first member
        NamespaceRemovedEvent event = new NamespaceRemovedEvent(metadata.id(), namespace);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> memberSubpath = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    members().
                    member(context.getMember().getId()).toList();
            DirectorySubspace memberSubspace = DirectoryLayer.getDefault().open(tr, memberSubpath).join();
            NamespaceUtil.setLastSeenNamespaceVersion(tr, memberSubspace, context.getClusterName(), event);
            tr.commit().join();
        }

        // Re-read metadata to get an updated version
        NamespaceMetadata updatedMetadata = NamespaceUtil.readMetadata(context, namespace);
        assertEquals(1, updatedMetadata.version());

        // Create the barrier and await - should throw BarrierNotSatisfiedException
        // because only one member has the version, not all
        NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, updatedMetadata);
        assertThrows(BarrierNotSatisfiedException.class, () -> barrier.await(1, 2, Duration.ofMillis(100)));
    }

    @Test
    void shouldRespectMaxAttemptsAndWaitDuration() {
        KronotopTestInstance firstInstance = getInstances().getFirst();
        Context context = firstInstance.getContext();
        String namespace = "test.barrier.timing";

        // Create a namespace and set removed (version → 1)
        NamespaceUtil.create(context, namespace);
        NamespaceUtil.setRemoved(context, namespace);

        NamespaceMetadata updatedMetadata = NamespaceUtil.readMetadata(context, namespace);

        // Don't set the lastSeenNamespaceVersion-barrier will never be satisfied
        NamespaceVersionBarrier barrier = new NamespaceVersionBarrier(context, updatedMetadata);

        int maxAttempts = 3;
        Duration waitDuration = Duration.ofMillis(100);

        long startTime = System.currentTimeMillis();
        assertThrows(BarrierNotSatisfiedException.class, () -> barrier.await(1, maxAttempts, waitDuration));
        long elapsedTime = System.currentTimeMillis() - startTime;

        // With 3 attempts and 100ms wait between retries, expect at least 200ms (2 waits between 3 attempts)
        long expectedMinTime = (maxAttempts - 1) * waitDuration.toMillis();
        assertTrue(elapsedTime >= expectedMinTime,
                "Expected at least " + expectedMinTime + "ms but took " + elapsedTime + "ms");
    }
}