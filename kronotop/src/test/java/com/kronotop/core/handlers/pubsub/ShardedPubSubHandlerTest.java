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

import com.kronotop.BaseHandlerTest;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.PushRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShardedPubSubHandlerTest extends BaseHandlerTest {

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

    private static long integer(RedisMessage message) {
        return ((IntegerRedisMessage) message).value();
    }

    private void switchToRESP3(EmbeddedChannel ch) {
        runCommand(ch, encode("HELLO", "3"));
    }

    private void createNamespace(EmbeddedChannel ch, String name) {
        runCommand(ch, encode("NAMESPACE", "CREATE", name));
    }

    private void useNamespace(EmbeddedChannel ch, String name) {
        runCommand(ch, encode("NAMESPACE", "USE", name));
    }

    @Test
    void shouldSubscribeAndReceivePublishedMessage() {
        // Behavior: SSUBSCRIBE registers the channel; a later SPUBLISH on the same channel delivers
        // an smessage push to the subscriber and returns the number of receivers.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        Object confirmation = runCommand(subscriber, encode("SSUBSCRIBE", "news"));
        List<RedisMessage> parts = children(confirmation);
        assertEquals("ssubscribe", bulk(parts.get(0)));
        assertEquals("news", bulk(parts.get(1)));
        assertEquals(1, integer(parts.get(2)));

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "news", "hello"));
        assertInstanceOf(IntegerRedisMessage.class, publishReply);
        assertEquals(1, ((IntegerRedisMessage) publishReply).value());

        Object delivered = subscriber.readOutbound();
        List<RedisMessage> message = children(delivered);
        assertEquals("smessage", bulk(message.get(0)));
        assertEquals("news", bulk(message.get(1)));
        assertEquals("hello", bulk(message.get(2)));
    }

    @Test
    void shouldReturnZeroWhenPublishingToChannelWithoutSubscribers() {
        // Behavior: SPUBLISH to a channel that has no subscribers delivers to nobody and returns 0.
        EmbeddedChannel publisher = newChannel();

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "empty", "hello"));
        assertInstanceOf(IntegerRedisMessage.class, publishReply);
        assertEquals(0, ((IntegerRedisMessage) publishReply).value());
    }

    @Test
    void shouldStopDeliveringAfterUnsubscribe() {
        // Behavior: after SUNSUBSCRIBE the connection no longer receives messages and the
        // remaining-subscription count drops to zero.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        Object confirmation = runCommand(subscriber, encode("SUNSUBSCRIBE", "news"));
        List<RedisMessage> parts = children(confirmation);
        assertEquals("sunsubscribe", bulk(parts.get(0)));
        assertEquals("news", bulk(parts.get(1)));
        assertEquals(0, integer(parts.get(2)));

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "news", "hello"));
        Assertions.assertNotNull(publishReply);
        assertEquals(0, ((IntegerRedisMessage) publishReply).value());
        assertNull(subscriber.readOutbound());
    }

    @Test
    void shouldTrackSubscriptionCountAcrossChannels() {
        // Behavior: a single SSUBSCRIBE with multiple channels confirms each channel with the
        // running total of this connection's subscriptions.
        EmbeddedChannel subscriber = newChannel();

        Object first = runCommand(subscriber, encode("SSUBSCRIBE", "a", "b"));
        List<RedisMessage> firstParts = children(first);
        assertEquals("a", bulk(firstParts.get(1)));
        assertEquals(1, integer(firstParts.get(2)));

        Object second = subscriber.readOutbound();
        List<RedisMessage> secondParts = children(second);
        assertEquals("b", bulk(secondParts.get(1)));
        assertEquals(2, integer(secondParts.get(2)));
    }

    @Test
    void shouldRejectDisallowedCommandWhileSubscribedInRESP2() {
        // Behavior: a RESP2 connection in subscription mode may not run arbitrary commands.
        EmbeddedChannel subscriber = newChannel();
        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        Object reply = runCommand(subscriber, encode("GET", "somekey"));
        assertInstanceOf(ErrorRedisMessage.class, reply);
        String content = ((ErrorRedisMessage) reply).content();
        assertTrue(content.contains("Can't execute 'get'"), content);
    }

    @Test
    void shouldAllowAnyCommandWhileSubscribedInRESP3() {
        // Behavior: a RESP3 connection is never restricted while subscribed.
        EmbeddedChannel subscriber = newChannel();
        switchToRESP3(subscriber);
        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        Object reply = runCommand(subscriber, encode("GET", "somekey"));
        assertInstanceOf(FullBulkStringRedisMessage.class, reply);
    }

    @Test
    void shouldDeliverPushFrameInRESP3() {
        // Behavior: RESP3 subscribers receive sharded messages as push frames, not plain arrays.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        switchToRESP3(subscriber);
        Object confirmation = runCommand(subscriber, encode("SSUBSCRIBE", "news"));
        assertInstanceOf(PushRedisMessage.class, confirmation);

        runCommand(publisher, encode("SPUBLISH", "news", "hello"));

        Object delivered = subscriber.readOutbound();
        assertInstanceOf(PushRedisMessage.class, delivered);
        List<RedisMessage> message = children(delivered);
        assertEquals("smessage", bulk(message.get(0)));
        assertEquals("hello", bulk(message.get(2)));
    }

    @Test
    void shouldDeliverToAllSubscribersOfChannel() {
        // Behavior: SPUBLISH delivers to every subscriber of the channel and returns their count.
        EmbeddedChannel subscriberOne = newChannel();
        EmbeddedChannel subscriberTwo = newChannel();
        EmbeddedChannel publisher = newChannel();

        runCommand(subscriberOne, encode("SSUBSCRIBE", "news"));
        runCommand(subscriberTwo, encode("SSUBSCRIBE", "news"));

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "news", "hello"));
        Assertions.assertNotNull(publishReply);
        assertEquals(2, ((IntegerRedisMessage) publishReply).value());

        assertEquals("hello", bulk(children(subscriberOne.readOutbound()).get(2)));
        assertEquals("hello", bulk(children(subscriberTwo.readOutbound()).get(2)));
    }

    @Test
    void shouldNotDeliverToSubscribersOfOtherChannels() {
        // Behavior: a message published to one channel never reaches subscribers of another channel.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "sports", "hello"));
        Assertions.assertNotNull(publishReply);
        assertEquals(0, ((IntegerRedisMessage) publishReply).value());
        assertNull(subscriber.readOutbound());
    }

    @Test
    void shouldDeliverOnlyToSubscribersInPublisherNamespace() {
        // Behavior: subscribers of the same channel in different namespaces are isolated. Both
        // namespaces publish before either subscriber reads, so a leak across namespaces would
        // surface as a wrong or extra message: ns-b must read its own "hello-b" and never "hello-a",
        // and ns-a must read its own "hello-a" and never "hello-b".
        EmbeddedChannel subscriberA = newChannel();
        EmbeddedChannel subscriberB = newChannel();
        EmbeddedChannel publisher = newChannel();

        createNamespace(publisher, "ns-a");
        createNamespace(publisher, "ns-b");

        useNamespace(subscriberA, "ns-a");
        runCommand(subscriberA, encode("SSUBSCRIBE", "news"));
        useNamespace(subscriberB, "ns-b");
        runCommand(subscriberB, encode("SSUBSCRIBE", "news"));

        useNamespace(publisher, "ns-a");
        assertEquals(1, integer((RedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-a"))));
        useNamespace(publisher, "ns-b");
        assertEquals(1, integer((RedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-b"))));

        // ns-a sees only hello-a: if hello-b had leaked in, the first frame would be hello-b.
        assertEquals("hello-a", bulk(children(subscriberA.readOutbound()).get(2)));
        assertNull(subscriberA.readOutbound());

        // ns-b sees only hello-b: if hello-a had leaked in, the first frame would be hello-a.
        assertEquals("hello-b", bulk(children(subscriberB.readOutbound()).get(2)));
        assertNull(subscriberB.readOutbound());
    }

    @Test
    void shouldNotDeliverWhenNoSubscriberSharesPublisherNamespace() {
        // Behavior: a message published from ns-b never reaches a subscriber living in ns-a. The
        // subscriber is then proven live and namespace-correct by a follow-up ns-a publish: its first
        // and only frame is "hello-a", so "hello-b" was never delivered to it.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        createNamespace(publisher, "ns-a");
        createNamespace(publisher, "ns-b");

        useNamespace(subscriber, "ns-a");
        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        useNamespace(publisher, "ns-b");
        assertEquals(0, integer((RedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-b"))));

        useNamespace(publisher, "ns-a");
        assertEquals(1, integer((RedisMessage) runCommand(publisher, encode("SPUBLISH", "news", "hello-a"))));

        assertEquals("hello-a", bulk(children(subscriber.readOutbound()).get(2)));
        assertNull(subscriber.readOutbound());
    }

    @Test
    void shouldKeepCountStableOnDuplicateSubscribe() {
        // Behavior: subscribing to the same channel twice is idempotent; the count stays at one and
        // only a single message is delivered.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        assertEquals(1, integer(children(runCommand(subscriber, encode("SSUBSCRIBE", "news"))).get(2)));
        assertEquals(1, integer(children(runCommand(subscriber, encode("SSUBSCRIBE", "news"))).get(2)));

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "news", "hello"));
        Assertions.assertNotNull(publishReply);
        assertEquals(1, ((IntegerRedisMessage) publishReply).value());
        assertEquals("hello", bulk(children(subscriber.readOutbound()).get(2)));
        assertNull(subscriber.readOutbound());
    }

    @Test
    void shouldUnsubscribeFromAllWhenNoChannelGiven() {
        // Behavior: SUNSUBSCRIBE without arguments removes every subscription of the connection.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        runCommand(subscriber, encode("SSUBSCRIBE", "a", "b"));
        subscriber.readOutbound();

        Object first = runCommand(subscriber, encode("SUNSUBSCRIBE"));
        Object second = subscriber.readOutbound();
        java.util.Set<String> unsubscribed = new java.util.HashSet<>();
        unsubscribed.add(bulk(children(first).get(1)));
        unsubscribed.add(bulk(children(second).get(1)));
        assertEquals(java.util.Set.of("a", "b"), unsubscribed);

        assertEquals(0, ((IntegerRedisMessage)
                Objects.requireNonNull(runCommand(publisher, encode("SPUBLISH", "a", "x")))).value()
        );
        assertEquals(0, ((IntegerRedisMessage)
                Objects.requireNonNull(runCommand(publisher, encode("SPUBLISH", "b", "x")))).value()
        );
    }

    @Test
    void shouldExitSubscriptionModeAfterUnsubscribingAll() {
        // Behavior: once a RESP2 connection unsubscribes from its last channel it leaves subscription
        // mode and may run ordinary commands again.
        EmbeddedChannel subscriber = newChannel();

        runCommand(subscriber, encode("SSUBSCRIBE", "news"));
        runCommand(subscriber, encode("SUNSUBSCRIBE", "news"));

        Object reply = runCommand(subscriber, encode("GET", "somekey"));
        assertInstanceOf(FullBulkStringRedisMessage.class, reply);
    }

    @Test
    void shouldAllowPingWhileSubscribedInRESP2() {
        // Behavior: PING is permitted while a RESP2 connection is in subscription mode.
        EmbeddedChannel subscriber = newChannel();
        runCommand(subscriber, encode("SSUBSCRIBE", "news"));

        Object reply = runCommand(subscriber, encode("PING"));
        assertInstanceOf(SimpleStringRedisMessage.class, reply);
        assertEquals("PONG", ((SimpleStringRedisMessage) reply).content());
    }

    @Test
    void shouldRejectSPublishWithWrongArgumentCount() {
        // Behavior: SPUBLISH requires exactly a channel and a message.
        EmbeddedChannel publisher = newChannel();

        Object reply = runCommand(publisher, encode("SPUBLISH", "news"));
        assertInstanceOf(ErrorRedisMessage.class, reply);
        assertTrue(((ErrorRedisMessage) reply).content().contains("wrong number of arguments"),
                ((ErrorRedisMessage) reply).content());
    }

    @Test
    void shouldRejectSSubscribeWithoutChannel() {
        // Behavior: SSUBSCRIBE requires at least one channel.
        EmbeddedChannel subscriber = newChannel();

        Object reply = runCommand(subscriber, encode("SSUBSCRIBE"));
        assertInstanceOf(ErrorRedisMessage.class, reply);
        assertTrue(((ErrorRedisMessage) reply).content().contains("wrong number of arguments"),
                ((ErrorRedisMessage) reply).content());
    }

    @Test
    void shouldCleanupSubscriptionsOnDisconnect() {
        // Behavior: when a subscriber disconnects, its subscriptions are removed, so a later publish
        // reaches nobody.
        EmbeddedChannel subscriber = newChannel();
        EmbeddedChannel publisher = newChannel();

        runCommand(subscriber, encode("SSUBSCRIBE", "news"));
        subscriber.close();

        Object publishReply = runCommand(publisher, encode("SPUBLISH", "news", "hello"));
        Assertions.assertNotNull(publishReply);
        assertEquals(0, ((IntegerRedisMessage) publishReply).value());
    }
}
