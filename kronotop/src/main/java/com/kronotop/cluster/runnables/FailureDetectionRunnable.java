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

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.membership.MemberView;
import com.kronotop.cluster.membership.MembershipService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class FailureDetectionRunnable implements ClusterRunnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectionRunnable.class);
    private final Context context;
    private final MembershipService service;
    private final long maxSilentPeriod;
    private final int heartbeatInterval;
    private volatile boolean shutdown;

    public FailureDetectionRunnable(Context context) {
        this.context = context;
        this.service = context.getService(MembershipService.NAME);
        this.heartbeatInterval = context.getConfig().getInt("cluster.heartbeat.interval");
        int heartbeatMaximumSilentPeriod = context.getConfig().getInt("cluster.heartbeat.maximum_silent_period");
        this.maxSilentPeriod = (long) Math.ceil((double) heartbeatMaximumSilentPeriod / heartbeatInterval);
    }

    @Override
    public void run() {
        if (shutdown) {
            return;
        }
        Member coordinator = service.getCoordinator();
        boolean isCoordinatorAlive = true;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (Member member : knownMembers.keySet()) {
                long lastHeartbeat = getLatestHeartbeat(tr, member);
                MemberView view = knownMembers.computeIfPresent(member, (m, memberView) -> {
                    if (memberView.getLastHeartbeat() != lastHeartbeat) {
                        memberView.setLastHeartbeat(lastHeartbeat);
                    } else {
                        memberView.increaseExpectedHeartbeat();
                    }
                    return memberView;
                });

                if (view == null) {
                    continue;
                }

                long silentPeriod = view.getExpectedHeartbeat() - view.getLastHeartbeat();
                if (silentPeriod > maxSilentPeriod) {
                    LOGGER.warn("{} has been suspected to be dead", member.getExternalAddress());
                    view.setAlive(false);
                    if (coordinator.equals(member)) {
                        isCoordinatorAlive = false;
                        LOGGER.info("Cluster coordinator is dead {}", coordinator.getExternalAddress());
                    }
                }
            }

            if (!isCoordinatorAlive) {
                TreeSet<Member> members = service.listMembers();
                findDeadCoordinator(members, context.getMember());
            }

            if (coordinator != null && coordinator.equals(context.getMember())) {
                for (Member member : knownMembers.keySet()) {
                    MemberView memberView = knownMembers.get(member);
                    if (!memberView.getAlive()) {
                        //unregisterMember(member);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.error("Error while running failure detection task", e);
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
