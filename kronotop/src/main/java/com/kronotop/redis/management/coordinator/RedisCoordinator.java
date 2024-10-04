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

package com.kronotop.redis.management.coordinator;

import com.kronotop.Context;
import com.kronotop.ServiceContext;
import com.kronotop.cluster.BroadcastEvent;
import com.kronotop.cluster.BroadcastEventHook;
import com.kronotop.cluster.BroadcastEventKind;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.coordinator.Coordinator;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.management.coordinator.hooks.MemberJoinHook;
import com.kronotop.redis.management.coordinator.hooks.MemberLeftHook;
import com.kronotop.redis.storage.RedisShard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisCoordinator implements Coordinator {
    public static final String NAME = "RedisCoordinator";
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisCoordinator.class);
    private final Context context;
    private final ServiceContext<RedisShard> redis;
    private final MembershipService membership;
    private final EnumMap<BroadcastEventKind, BroadcastEventHook> hooks = new EnumMap<>(BroadcastEventKind.class);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
    private volatile boolean started = false;

    public RedisCoordinator(Context context) {
        this.context = context;
        this.membership = context.getService(MembershipService.NAME);
        this.redis = context.getServiceContext(RedisService.NAME);
        this.hooks.put(BroadcastEventKind.MEMBER_JOIN, new MemberJoinHook(this));
        this.hooks.put(BroadcastEventKind.MEMBER_LEFT, new MemberLeftHook(this));
    }

    @Override
    public void submit(BroadcastEvent event) {
        if (!started) {
            LOGGER.debug("{} event submitted but {} not started yet", event.kind(), NAME);
            return;
        }
        executorService.submit(() -> {
            BroadcastEventHook hook = hooks.get(event.kind());
            if (hook == null) {
                LOGGER.error("No registered hook found for: {}", event.kind());
                return;
            }
            hook.run(event);
        });
    }

    @Override
    public void start() {
        LOGGER.info("Starting Redis Coordinator");
        started = true;
    }

    @Override
    public void shutdown() {

    }

    public boolean isCoordinator() {
        return membership.getKnownCoordinator().equals(context.getMember());
    }
}
