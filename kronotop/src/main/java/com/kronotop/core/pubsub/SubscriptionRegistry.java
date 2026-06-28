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

package com.kronotop.core.pubsub;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.MemberAttributes;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.HashSlots;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.ClusterNotInitializedException;
import com.kronotop.server.RESPError;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.PushRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.Unpooled;
import io.netty.util.Attribute;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-local registry for sharded Pub/Sub subscriptions. A channel maps to a hash slot, and both
 * subscribe and publish for a channel are routed to the primary owner of that slot, so delivery is
 * entirely in-memory on a single node with no inter-node coordination.
 */
public class SubscriptionRegistry {
    private static final byte[] SMESSAGE = "smessage".getBytes(StandardCharsets.UTF_8);

    private final Context context;
    private final int numberOfBucketShards;
    private final ConcurrentHashMap<String, Set<Session>> channels = new ConcurrentHashMap<>();
    private volatile RoutingService routing;

    public SubscriptionRegistry(Context context) {
        this.context = context;
        this.numberOfBucketShards = context.getConfig().getInt("bucket.shards");
    }

    private RoutingService routing() {
        RoutingService service = routing;
        if (service == null) {
            // RoutingService may not be registered yet when CoreService is constructed, so it is
            // resolved lazily on first use.
            service = context.getService(RoutingService.NAME);
            routing = service;
        }
        return service;
    }

    /**
     * Ensures that this node is the primary owner of the channel's hash slot.
     *
     * @param channel the channel name
     * @throws ClusterNotInitializedException if the cluster has not been initialized yet
     * @throws KronotopException              with a MOVED redirection if another member owns the slot
     */
    public void ensurePrimary(String channel) {
        Attribute<Boolean> clusterInitialized = context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
        if (clusterInitialized.get() == null || !clusterInitialized.get()) {
            throw new ClusterNotInitializedException();
        }

        int slot = HashSlots.slot(channel);
        int shardId = Math.floorMod(slot, numberOfBucketShards);
        Route route = routing().findRoute(ShardKind.BUCKET, shardId);
        if (route == null) {
            throw new KronotopException(String.format("shard id: %d not owned by any member yet", shardId));
        }
        if (!route.primary().equals(context.getMember())) {
            throw new KronotopException(
                    RESPError.MOVED,
                    String.format("%d %s:%d", slot, route.primary().getExternalAddress().getHost(), route.primary().getExternalAddress().getPort())
            );
        }
    }

    /**
     * Subscribes the session to the channel.
     *
     * @param session the subscribing session
     * @param channel the channel name
     * @return the total number of channels this session is now subscribed to
     */
    public int subscribe(Session session, String channel) {
        channels.compute(channel, (k, sessions) -> {
            Set<Session> set = (sessions == null) ? ConcurrentHashMap.newKeySet() : sessions;
            set.add(session);
            return set;
        });
        Set<String> subscriptions = session.attr(SessionAttributes.SUBSCRIPTIONS).get();
        subscriptions.add(channel);
        session.attr(SessionAttributes.SUBSCRIPTION_MODE).set(true);
        return subscriptions.size();
    }

    /**
     * Unsubscribes the session from the channel.
     *
     * @param session the session
     * @param channel the channel name
     * @return the number of channels this session remains subscribed to
     */
    public int unsubscribe(Session session, String channel) {
        removeSubscriber(session, channel);
        Set<String> subscriptions = session.attr(SessionAttributes.SUBSCRIPTIONS).get();
        subscriptions.remove(channel);
        int remaining = subscriptions.size();
        if (remaining == 0) {
            session.attr(SessionAttributes.SUBSCRIPTION_MODE).set(false);
        }
        return remaining;
    }

    /**
     * Removes the session from every channel it is subscribed to. Called when the connection closes.
     *
     * @param session the disconnecting session
     */
    public void unsubscribeAll(Session session) {
        Set<String> subscriptions = session.attr(SessionAttributes.SUBSCRIPTIONS).get();
        if (subscriptions == null || subscriptions.isEmpty()) {
            return;
        }
        for (String channel : new ArrayList<>(subscriptions)) {
            removeSubscriber(session, channel);
        }
        subscriptions.clear();
        session.attr(SessionAttributes.SUBSCRIPTION_MODE).set(false);
    }

    /**
     * Delivers a message to every local subscriber of the channel whose current namespace matches
     * the given namespace. Subscribers attached to a different namespace are skipped.
     *
     * @param namespace the namespace the message is published to
     * @param channel   the channel name
     * @param payload   the message payload
     * @return the number of subscribers the message was delivered to
     */
    public int publish(String namespace, String channel, byte[] payload) {
        Set<Session> subscribers = channels.get(channel);
        if (subscribers == null || subscribers.isEmpty()) {
            return 0;
        }
        int delivered = 0;
        for (Session subscriber : subscribers) {
            String currentNamespace = subscriber.attr(SessionAttributes.CURRENT_NAMESPACE).get();
            if (!currentNamespace.equals(namespace)) {
                continue;
            }
            subscriber.getCtx().writeAndFlush(buildMessage(subscriber, channel, payload));
            delivered++;
        }
        return delivered;
    }

    /**
     * Sends a subscribe/unsubscribe confirmation to the session. RESP3 sessions receive a push
     * frame, RESP2 sessions receive a plain array.
     *
     * @param session the session to notify
     * @param type    the confirmation type ("ssubscribe" or "sunsubscribe")
     * @param channel the channel name, or null when unsubscribing from no channel
     * @param count   the number of channels the session remains subscribed to
     */
    public void sendConfirmation(Session session, String type, String channel, int count) {
        List<RedisMessage> parts = new ArrayList<>(3);
        parts.add(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(type.getBytes(StandardCharsets.UTF_8))));
        if (channel == null) {
            parts.add(FullBulkStringRedisMessage.NULL_INSTANCE);
        } else {
            parts.add(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(channel.getBytes(StandardCharsets.UTF_8))));
        }
        parts.add(new IntegerRedisMessage(count));
        RedisMessage message = (session.protocolVersion() == RESPVersion.RESP3)
                ? new PushRedisMessage(parts)
                : new ArrayRedisMessage(parts);
        session.getCtx().writeAndFlush(message);
    }

    private void removeSubscriber(Session session, String channel) {
        channels.compute(channel, (k, sessions) -> {
            if (sessions == null) {
                return null;
            }
            sessions.remove(session);
            return sessions.isEmpty() ? null : sessions;
        });
    }

    private RedisMessage buildMessage(Session session, String channel, byte[] payload) {
        List<RedisMessage> parts = new ArrayList<>(3);
        parts.add(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(SMESSAGE)));
        parts.add(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(channel.getBytes(StandardCharsets.UTF_8))));
        parts.add(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(payload)));
        if (session.protocolVersion() == RESPVersion.RESP3) {
            return new PushRedisMessage(parts);
        }
        return new ArrayRedisMessage(parts);
    }
}
