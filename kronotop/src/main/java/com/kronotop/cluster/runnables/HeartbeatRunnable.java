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

package com.kronotop.cluster.runnables;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.cluster.membership.MembershipService;
import com.kronotop.cluster.membership.impl.MembershipServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class HeartbeatRunnable implements ClusterRunnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatRunnable.class);
    private static final byte[] HEARTBEAT_DELTA = new byte[]{1, 0, 0, 0, 0, 0, 0, 0}; // 1, byte order: little-endian
    private final Context context;
    private final MembershipService service;
    private final int heartbeatInterval;
    private volatile boolean shutdown = false;

    public HeartbeatRunnable(Context context) {
        this.context = context;
        this.service = context.getService(MembershipService.NAME);
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
    }

    @Override
    public void run() {
        if (shutdown) {
            return;
        }
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] key = context.getMember().getSubspace().pack(MembershipServiceImpl.Keys.LAST_HEARTBEAT.toString());
            tr.mutate(MutationType.ADD, key, HEARTBEAT_DELTA);
            tr.commit().join();
        } catch (Exception e) {
            LOGGER.error("Error while running heartbeat task", e);
        } finally {
            if (!shutdown) {
                service.scheduleRunnable(this, heartbeatInterval, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void shutdown() {
        shutdown = true;
    }
}