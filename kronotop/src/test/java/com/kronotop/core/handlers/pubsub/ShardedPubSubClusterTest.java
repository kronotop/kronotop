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

package com.kronotop.core.handlers.pubsub;

import com.kronotop.BaseClusterTest;
import com.kronotop.KronotopTestInstance;
import com.kronotop.MemberAttributes;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.HashSlots;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.network.Address;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.PushRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardedPubSubClusterTest extends BaseClusterTest {

    private static ByteBuf encode(String... args) {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(("*" + args.length + "\r\n").getBytes(StandardCharsets.UTF_8));
        for (String arg : args) {
            byte[] raw = arg.getBytes(StandardCharsets.UTF_8);
            buf.writeBytes(("$" + raw.length + "\r\n").getBytes(StandardCharsets.UTF_8));
            buf.writeBytes(raw);
            buf.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }
        return buf;
    }

    /**
     * Returns a member that owns no bucket shards alongside the member that owns the channel's
     * shard. The first member of a freshly initialized cluster becomes primary of every bucket
     * shard, so a second member that joins owns none of them. Waits until the second member has
     * loaded the routing table and sees the first member as the channel's shard owner.
     */
    private MovedFixture prepareMovedFixture(String channel) {
        KronotopTestInstance owner = getInstances().getFirst();
        KronotopTestInstance nonOwner = addNewInstance();

        int numberOfBucketShards = nonOwner.getContext().getConfig().getInt("bucket.shards");
        int shardId = Math.floorMod(HashSlots.slot(channel), numberOfBucketShards);

        await().atMost(15, TimeUnit.SECONDS).until(() -> {
            Attribute<Boolean> initialized = nonOwner.getContext().getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
            if (initialized.get() == null || !initialized.get()) {
                return false;
            }
            RoutingService routing = nonOwner.getContext().getService(RoutingService.NAME);
            Route route = routing.findRoute(ShardKind.BUCKET, shardId);
            return route != null && route.primary().equals(owner.getMember());
        });

        return new MovedFixture(owner, nonOwner, channel);
    }

    private static List<RedisMessage> children(Object msg) {
        if (msg instanceof ArrayRedisMessage array) {
            return array.children();
        }
        if (msg instanceof PushRedisMessage push) {
            return push.children();
        }
        throw new AssertionError("expected an array or push message, got: " + msg);
    }

    private static String bulk(RedisMessage message) {
        return ((FullBulkStringRedisMessage) message).content().toString(CharsetUtil.UTF_8);
    }

    private void createNamespace(EmbeddedChannel ch, String name) {
        runCommand(ch, encode("NAMESPACE", "CREATE", name));
    }

    private void useNamespace(EmbeddedChannel ch, String name) {
        runCommand(ch, encode("NAMESPACE", "USE", name));
    }

    private String expectedMovedSuffix(KronotopTestInstance owner, String channel) {
        Address address = owner.getMember().getExternalAddress();
        return HashSlots.slot(channel) + " " + address.getHost() + ":" + address.getPort();
    }

    @Test
    void shouldRedirectSSubscribeWithMovedWhenChannelShardIsRemote() {
        // Behavior: SSUBSCRIBE sent to a member that does not own the channel's bucket shard returns
        // a MOVED redirection pointing at the shard's primary owner.
        MovedFixture fixture = prepareMovedFixture("news");

        Object response = runCommand(fixture.nonOwner.getChannel(), encode("SSUBSCRIBE", fixture.channel));
        assertInstanceOf(ErrorRedisMessage.class, response);
        String content = ((ErrorRedisMessage) response).content();
        assertEquals("MOVED " + expectedMovedSuffix(fixture.owner, fixture.channel), content);
    }

    @Test
    void shouldRedirectSPublishWithMovedWhenChannelShardIsRemote() {
        // Behavior: SPUBLISH sent to a member that does not own the channel's bucket shard returns a
        // MOVED redirection pointing at the shard's primary owner.
        MovedFixture fixture = prepareMovedFixture("news");

        Object response = runCommand(fixture.nonOwner.getChannel(), encode("SPUBLISH", fixture.channel, "hello"));
        assertInstanceOf(ErrorRedisMessage.class, response);
        String content = ((ErrorRedisMessage) response).content();
        assertTrue(content.startsWith("MOVED "), content);
        assertEquals("MOVED " + expectedMovedSuffix(fixture.owner, fixture.channel), content);
    }

    @Test
    void shouldDeliverOnlyToSubscribersInPublisherNamespaceOnShardOwner() {
        // Behavior: on the member that owns the channel's shard, subscribers of the same channel in
        // different namespaces are isolated. Both namespaces publish before either subscriber reads,
        // so a leak across namespaces would surface as a wrong or extra message: ns-b must read its
        // own "hello-b" and never "hello-a", and ns-a must read its own "hello-a" and never "hello-b".
        KronotopTestInstance owner = getInstances().getFirst();
        EmbeddedChannel subscriberA = owner.newChannel();
        EmbeddedChannel subscriberB = owner.newChannel();
        EmbeddedChannel publisher = owner.newChannel();

        createNamespace(publisher, "ns-a");
        createNamespace(publisher, "ns-b");

        useNamespace(subscriberA, "ns-a");
        runCommand(subscriberA, encode("SSUBSCRIBE", "news"));
        useNamespace(subscriberB, "ns-b");
        runCommand(subscriberB, encode("SSUBSCRIBE", "news"));

        useNamespace(publisher, "ns-a");
        assertEquals(1, ((IntegerRedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-a"))).value());
        useNamespace(publisher, "ns-b");
        assertEquals(1, ((IntegerRedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-b"))).value());

        // ns-a sees only hello-a: if hello-b had leaked in, the first frame would be hello-b.
        assertEquals("hello-a", bulk(children(subscriberA.readOutbound()).get(2)));
        assertNull(subscriberA.readOutbound());

        // ns-b sees only hello-b: if hello-a had leaked in, the first frame would be hello-a.
        assertEquals("hello-b", bulk(children(subscriberB.readOutbound()).get(2)));
        assertNull(subscriberB.readOutbound());
    }

    @Test
    void shouldNotDeliverWhenNoSubscriberSharesPublisherNamespaceOnShardOwner() {
        // Behavior: on the shard owner, a message published from ns-b never reaches a subscriber
        // living in ns-a. The subscriber is then proven live and namespace-correct by a follow-up
        // ns-a publish: its first and only frame is "hello-a", so "hello-b" was never delivered to it.
        KronotopTestInstance owner = getInstances().getFirst();
        EmbeddedChannel subscriber = owner.newChannel();
        EmbeddedChannel publisher = owner.newChannel();

        createNamespace(publisher, "ns-a");
        createNamespace(publisher, "ns-b");

        useNamespace(subscriber, "ns-a");
        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        useNamespace(publisher, "ns-b");
        assertEquals(0, ((IntegerRedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-b"))).value());

        useNamespace(publisher, "ns-a");
        assertEquals(1, ((IntegerRedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-a"))).value());

        assertEquals("hello-a", bulk(children(subscriber.readOutbound()).get(2)));
        assertNull(subscriber.readOutbound());
    }

    private record MovedFixture(KronotopTestInstance owner, KronotopTestInstance nonOwner, String channel) {
    }
}
