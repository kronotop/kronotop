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

package com.kronotop.metrics;

import com.kronotop.server.ServerKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NetworkMetricsTest {

    @Test
    void shouldStartAtZero() {
        // Behavior: a new NetworkMetrics reports zero for every counter of both server kinds
        NetworkMetrics metrics = new NetworkMetrics();

        for (ServerKind kind : ServerKind.values()) {
            assertEquals(0, metrics.getReadBytes(kind), kind.name());
            assertEquals(0, metrics.getWrittenBytes(kind), kind.name());
            assertEquals(0, metrics.getTotalCommandsProcessed(kind), kind.name());
        }
    }

    @Test
    void shouldCountReadBytesPerServerKind() {
        // Behavior: read bytes add up per server kind; the other kind stays untouched
        NetworkMetrics metrics = new NetworkMetrics();

        metrics.increaseReadBytes(ServerKind.EXTERNAL, 10);
        metrics.increaseReadBytes(ServerKind.EXTERNAL, 5);

        assertEquals(15, metrics.getReadBytes(ServerKind.EXTERNAL));
        assertEquals(0, metrics.getReadBytes(ServerKind.INTERNAL));
        assertEquals(0, metrics.getWrittenBytes(ServerKind.EXTERNAL));
    }

    @Test
    void shouldCountWrittenBytesPerServerKind() {
        // Behavior: written bytes add up per server kind; the other kind stays untouched
        NetworkMetrics metrics = new NetworkMetrics();

        metrics.increaseWrittenBytes(ServerKind.INTERNAL, 7);
        metrics.increaseWrittenBytes(ServerKind.INTERNAL, 3);

        assertEquals(10, metrics.getWrittenBytes(ServerKind.INTERNAL));
        assertEquals(0, metrics.getWrittenBytes(ServerKind.EXTERNAL));
        assertEquals(0, metrics.getReadBytes(ServerKind.INTERNAL));
    }

    @Test
    void shouldCountCommandsPerServerKind() {
        // Behavior: every increase adds one command to the given server kind only
        NetworkMetrics metrics = new NetworkMetrics();

        metrics.increaseTotalCommandsProcessed(ServerKind.INTERNAL);
        metrics.increaseTotalCommandsProcessed(ServerKind.INTERNAL);
        metrics.increaseTotalCommandsProcessed(ServerKind.INTERNAL);

        assertEquals(3, metrics.getTotalCommandsProcessed(ServerKind.INTERNAL));
        assertEquals(0, metrics.getTotalCommandsProcessed(ServerKind.EXTERNAL));
    }
}
