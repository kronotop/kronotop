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

package com.kronotop.metrics;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RuntimeMetrics {
    private final AtomicLong gcFreedBytes = new AtomicLong();
    private final NotificationListener listener = this::handleNotification;
    private final List<NotificationEmitter> emitters = new ArrayList<>();
    private final NetworkMetrics networkMetrics = new NetworkMetrics();

    /**
     * Starts metric collection.
     */
    public synchronized void start() {
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc instanceof NotificationEmitter emitter) {
                emitter.addNotificationListener(listener, null, null);
                emitters.add(emitter);
            }
        }
    }

    /**
     * Stops metric collection. The collected values are kept.
     */
    public synchronized void stop() {
        for (NotificationEmitter emitter : emitters) {
            try {
                emitter.removeNotificationListener(listener);
            } catch (ListenerNotFoundException ignored) {
                // Already removed
            }
        }
        emitters.clear();
    }

    /**
     * Returns the total memory freed by garbage collection since {@link #start()}, in bytes.
     */
    public long getGcFreedBytes() {
        return gcFreedBytes.get();
    }

    public NetworkMetrics getNetworkMetrics() {
        return networkMetrics;
    }

    private void handleNotification(Notification notification, Object handback) {
        if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
            return;
        }
        GcInfo gcInfo = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData()).getGcInfo();
        long before = 0;
        for (MemoryUsage usage : gcInfo.getMemoryUsageBeforeGc().values()) {
            before += usage.getUsed();
        }
        long after = 0;
        for (MemoryUsage usage : gcInfo.getMemoryUsageAfterGc().values()) {
            after += usage.getUsed();
        }
        // Objects moved between pools cancel out. A concurrent collection can end with more memory in use
        // than it started with, because the application keeps allocating.
        if (before > after) {
            gcFreedBytes.addAndGet(before - after);
        }
    }
}
