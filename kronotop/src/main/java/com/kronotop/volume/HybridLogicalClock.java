/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

public final class HybridLogicalClock {
    // 20 bits for the logical counter (Max ~1 million ops/ms)
    private static final int LOGICAL_BITS = 20;
    private static final long LOGICAL_MASK = (1L << LOGICAL_BITS) - 1;

    // Maximum tolerable drift if the clock moves backwards (5 seconds)
    // If backward drift exceeds this, an error is thrown for data consistency.
    private static final long MAX_POSSIBLE_DRIFT_MS = 5000;

    private long lastPhysical;
    private long lastLogical;

    public HybridLogicalClock() {
        // Initialize the clock on startup
        this.lastPhysical = System.currentTimeMillis();
        this.lastLogical = 0;
    }

    /**
     * Creates an HLC timestamp for a given physical time with logical counter set to zero.
     * Useful for generating range scan bounds in time travel queries.
     *
     * @param physicalTimeMs the physical time in milliseconds
     * @return HLC timestamp representing the minimum value for that millisecond
     */
    public static long fromPhysicalTime(long physicalTimeMs) {
        return physicalTimeMs << LOGICAL_BITS;
    }

    /**
     * Generates the next HLC timestamp.
     * Thread-safe: synchronized
     *
     * @param physicalNow Usually System.currentTimeMillis()
     * @return 64-bit HLC Timestamp (44-bit physical | 20-bit logical)
     */
    public synchronized long next(long physicalNow) {
        // CASE 1: Clock moved forward normally
        if (physicalNow > lastPhysical) {
            lastPhysical = physicalNow;
            lastLogical = 0;
        }
        // CASE 2: Same millisecond OR clock moved backwards (NTP Drift)
        else {
            // Even if the clock moves backwards, we don't decrease lastPhysical (Ratchet Effect).
            // However, if the drift is too large, we throw an exception.
            long drift = lastPhysical - physicalNow;
            if (drift > MAX_POSSIBLE_DRIFT_MS) {
                throw new IllegalStateException(
                        String.format("Clock moved backwards too much (%d ms). Refusing to generate Sequence Number.", drift)
                );
            }

            // Increment the logical counter
            lastLogical++;

            // Overflow Check (If > 1 million ops/ms)
            if (lastLogical > LOGICAL_MASK) {
                // Spin-wait: Wait for physical time to catch up to lastPhysical
                while (physicalNow <= lastPhysical) {
                    physicalNow = System.currentTimeMillis();

                    // Protection if the clock moves backwards dramatically while waiting
                    if (lastPhysical - physicalNow > MAX_POSSIBLE_DRIFT_MS) {
                        throw new IllegalStateException("Clock kept moving backwards during spin-wait.");
                    }
                }

                // Time advanced, update values
                lastPhysical = physicalNow;
                lastLogical = 0;
            }
        }

        // Pack HLC Timestamp: [Physical Time (44 bit)] [Logical Counter (20 bit)]
        return (lastPhysical << LOGICAL_BITS) | lastLogical;
    }
}