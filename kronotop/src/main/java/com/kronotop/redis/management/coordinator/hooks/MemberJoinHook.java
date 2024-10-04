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

package com.kronotop.redis.management.coordinator.hooks;

import com.kronotop.cluster.BroadcastEvent;
import com.kronotop.cluster.BroadcastEventHook;
import com.kronotop.redis.management.coordinator.RedisCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemberJoinHook implements BroadcastEventHook {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemberJoinHook.class);

    private final RedisCoordinator coordinator;

    public MemberJoinHook(RedisCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Override
    public void run(BroadcastEvent event) {
        System.out.println(coordinator.isCoordinator());
        LOGGER.info("Redis MemberJoinHook running");
    }
}
