/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.BarrierNotSatisfiedException;
import com.kronotop.BaseClusterTest;
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberView;
import com.kronotop.cluster.MembershipService;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class TombstoneBarrierTest extends BaseClusterTest {

    private void awaitMemberDiscovery(Context context, Member target) {
        MembershipService membership = context.getService(MembershipService.NAME);
        await().atMost(15, TimeUnit.SECONDS).until(() ->
                membership.getKnownMembers().containsKey(target)
        );
    }

    @Test
    void shouldCleanupWhenAllAliveMembersHaveObserved() {
        // Behavior: checkBarrier passes and cleans up tombstone+observers when every alive member has observed.
        KronotopTestInstance first = getInstances().getFirst();
        KronotopTestInstance second = addNewInstance();
        Context firstContext = first.getContext();
        Context secondContext = second.getContext();
        awaitMemberDiscovery(firstContext, second.getMember());

        String namespace = "test.barrier.cleanup." + UUID.randomUUID();

        String token;
        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(firstContext, tr, namespace);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(firstContext, tr, namespace, token);
            tr.commit().join();
        }

        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(secondContext, tr, namespace, token);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            TombstoneManager.checkBarrier(firstContext, tr, namespace);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertNull(TombstoneManager.getTombstone(firstContext, tr, namespace));
            assertTrue(TombstoneManager.getObservers(firstContext, tr, namespace, token).isEmpty());
        }
    }

    @Test
    void shouldThrowWhenAliveMemberHasNotObserved() {
        // Behavior: checkBarrier throws BarrierNotSatisfiedException when any alive member has not observed the tombstone.
        KronotopTestInstance first = getInstances().getFirst();
        KronotopTestInstance second = addNewInstance();
        Context firstContext = first.getContext();
        awaitMemberDiscovery(firstContext, second.getMember());

        String namespace = "test.barrier.not-observed." + UUID.randomUUID();

        String token;
        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(firstContext, tr, namespace);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(firstContext, tr, namespace, token);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertThrows(BarrierNotSatisfiedException.class, () ->
                    TombstoneManager.checkBarrier(firstContext, tr, namespace)
            );
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertNotNull(TombstoneManager.getTombstone(firstContext, tr, namespace));
        }
    }

    @Test
    void shouldSkipDeadMemberAndPassBarrier() {
        // Behavior: checkBarrier ignores dead members — if the only non-observing member is dead, the barrier passes.
        KronotopTestInstance first = getInstances().getFirst();
        KronotopTestInstance second = addNewInstance();
        Context firstContext = first.getContext();
        awaitMemberDiscovery(firstContext, second.getMember());

        String namespace = "test.barrier.dead-member." + UUID.randomUUID();

        String token;
        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(firstContext, tr, namespace);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(firstContext, tr, namespace, token);
            tr.commit().join();
        }

        MembershipService membership = firstContext.getService(MembershipService.NAME);
        MemberView secondView = membership.getKnownMembers().get(second.getMember());
        secondView.setAlive(false);

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            TombstoneManager.checkBarrier(firstContext, tr, namespace);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertNull(TombstoneManager.getTombstone(firstContext, tr, namespace));
            assertTrue(TombstoneManager.getObservers(firstContext, tr, namespace, token).isEmpty());
        }
    }

    @Test
    void shouldThrowWhenObserverHasStaleMismatchedToken() {
        // Behavior: checkBarrier treats observers with a mismatched token as not having observed.
        KronotopTestInstance first = getInstances().getFirst();
        KronotopTestInstance second = addNewInstance();
        Context firstContext = first.getContext();
        Context secondContext = second.getContext();
        awaitMemberDiscovery(firstContext, second.getMember());

        String namespace = "test.barrier.stale-token." + UUID.randomUUID();

        String token;
        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            token = TombstoneManager.setTombstone(firstContext, tr, namespace);
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(firstContext, tr, namespace, token);
            tr.commit().join();
        }

        try (Transaction tr = secondContext.getFoundationDB().createTransaction()) {
            TombstoneManager.observe(secondContext, tr, namespace, "stale-token-from-previous-move");
            tr.commit().join();
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertThrows(BarrierNotSatisfiedException.class, () ->
                    TombstoneManager.checkBarrier(firstContext, tr, namespace)
            );
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertNotNull(TombstoneManager.getTombstone(firstContext, tr, namespace));
        }
    }

    @Test
    void shouldAllowCreateOnOldNameAfterAllMembersObserve() {
        // Behavior: After MOVE sets a tombstone, CREATE on the old name succeeds once all alive members have observed.
        KronotopTestInstance first = getInstances().getFirst();
        KronotopTestInstance second = addNewInstance();
        Context firstContext = first.getContext();
        awaitMemberDiscovery(firstContext, second.getMember());

        String oldName = UUID.randomUUID().toString();
        String newName = UUID.randomUUID().toString();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = first.getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(oldName).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceMove(oldName, newName).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        String token;
        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            token = TombstoneManager.getTombstone(firstContext, tr, oldName);
        }
        assertNotNull(token);

        String finalToken = token;
        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
                List<String> observers = TombstoneManager.getObservers(firstContext, tr, oldName, finalToken);
                return observers.size() >= 2;
            }
        });

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.namespaceCreate(oldName).encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        try (Transaction tr = firstContext.getFoundationDB().createTransaction()) {
            assertNull(TombstoneManager.getTombstone(firstContext, tr, oldName));
        }
    }
}
