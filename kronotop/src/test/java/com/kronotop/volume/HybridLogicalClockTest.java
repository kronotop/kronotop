/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.volume;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HybridLogicalClockTest {

    @Test
    void shouldGenerateMonotonicallyIncreasingTimestamps_whenTimeMovesForward() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long now = System.currentTimeMillis();

        long ts1 = hlc.next(now);
        long ts2 = hlc.next(now + 1000);

        assertTrue(ts2 > ts1);
    }

    @Test
    void shouldIncrementLogicalCounter_whenCalledInSameMillisecond() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long now = System.currentTimeMillis();

        long ts1 = hlc.next(now);
        long ts2 = hlc.next(now);
        long ts3 = hlc.next(now);

        assertTrue(ts2 > ts1);
        assertTrue(ts3 > ts2);
    }

    @Test
    void shouldHandleSmallBackwardClockDrift() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long now = System.currentTimeMillis();

        long ts1 = hlc.next(now);
        long ts2 = hlc.next(now - 1000); // 1 second backward

        assertTrue(ts2 > ts1);
    }

    @Test
    void shouldThrowException_whenBackwardDriftExceedsThreshold() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long now = System.currentTimeMillis();

        hlc.next(now);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            hlc.next(now - 6000); // 6 seconds backward, exceeds MAX_POSSIBLE_DRIFT_MS
        });

        assertTrue(exception.getMessage().contains("Clock moved backwards too much"));
    }

    @Test
    void shouldIncrementLogicalCounterCorrectly_whenTimestampsExtracted() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long now = System.currentTimeMillis();

        long ts1 = hlc.next(now + 1000);
        long ts2 = hlc.next(now + 1000);
        long ts3 = hlc.next(now + 1000);

        long logical1 = ts1 & ((1L << 20) - 1);
        long logical2 = ts2 & ((1L << 20) - 1);
        long logical3 = ts3 & ((1L << 20) - 1);

        assertEquals(0, logical1);
        assertEquals(1, logical2);
        assertEquals(2, logical3);
    }

    @Test
    void shouldPackPhysicalAndLogicalComponents_into64BitTimestamp() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long now = System.currentTimeMillis();

        long ts = hlc.next(now + 1000);

        long extractedPhysical = ts >> 20;
        long extractedLogical = ts & ((1L << 20) - 1);

        assertEquals(now + 1000, extractedPhysical);
        assertEquals(0, extractedLogical);
    }

    @Test
    void shouldCreateTimestampWithZeroLogicalCounter_fromPhysicalTime() {
        long physicalTime = System.currentTimeMillis();

        long ts = HybridLogicalClock.fromPhysicalTime(physicalTime);

        long extractedPhysical = ts >> 20;
        long extractedLogical = ts & ((1L << 20) - 1);

        assertEquals(physicalTime, extractedPhysical);
        assertEquals(0, extractedLogical);
    }

    @Test
    void shouldProduceMinimumValueForMillisecond_fromPhysicalTime() {
        HybridLogicalClock hlc = new HybridLogicalClock();
        long physicalTime = System.currentTimeMillis() + 1000;

        long minTs = HybridLogicalClock.fromPhysicalTime(physicalTime);
        long ts1 = hlc.next(physicalTime);
        long ts2 = hlc.next(physicalTime);

        assertTrue(minTs <= ts1);
        assertTrue(minTs < ts2);
    }

    @Test
    void shouldWorkWithPastTimestamps_fromPhysicalTime() {
        long now = System.currentTimeMillis();
        long threeDaysAgo = now - (3 * 24 * 60 * 60 * 1000L);

        long pastTs = HybridLogicalClock.fromPhysicalTime(threeDaysAgo);
        long nowTs = HybridLogicalClock.fromPhysicalTime(now);

        assertTrue(pastTs < nowTs);

        long extractedPhysical = pastTs >> 20;
        assertEquals(threeDaysAgo, extractedPhysical);
    }
}
