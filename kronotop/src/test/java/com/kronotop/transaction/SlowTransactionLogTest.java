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

package com.kronotop.transaction;

import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SlowTransactionLogTest extends BaseStandaloneInstanceTest {

    private InstrumentedTransaction closed() {
        InstrumentedTransaction tr = context.getTransactionManager().create();
        tr.close();
        return tr;
    }

    @Test
    void shouldSkipTransactionUnderThreshold() {
        SlowTransactionLog log = new SlowTransactionLog(3_600_000, 10);
        log.record(closed());
        assertTrue(log.entries().isEmpty());
    }

    @Test
    void shouldRecordTransactionAtThreshold() {
        SlowTransactionLog log = new SlowTransactionLog(0, 10);
        InstrumentedTransaction tr = closed();
        log.record(tr);

        List<SlowTransaction> entries = log.entries();
        assertEquals(1, entries.size());
        SlowTransaction entry = entries.getFirst();
        assertEquals(tr.getId(), entry.id());
        assertEquals(InstrumentedTransaction.INTERNAL, entry.clientId());
        assertNull(entry.command());
        assertTrue(entry.timestamp() > 0);
        assertTrue(entry.durationMs() >= 0);
    }

    @Test
    void shouldDropOldestEntry() {
        SlowTransactionLog log = new SlowTransactionLog(0, 2);
        InstrumentedTransaction first = closed();
        InstrumentedTransaction second = closed();
        InstrumentedTransaction third = closed();
        log.record(first);
        log.record(second);
        log.record(third);

        List<SlowTransaction> entries = log.entries();
        assertEquals(2, entries.size());
        assertEquals(third.getId(), entries.get(0).id());
        assertEquals(second.getId(), entries.get(1).id());
    }

    @Test
    void shouldSkipWatchingTransaction() {
        SlowTransactionLog log = new SlowTransactionLog(0, 10);
        InstrumentedTransaction tr = context.getTransactionManager().create();
        tr.watch("slow-log-watch".getBytes());
        tr.close();
        log.record(tr);
        assertTrue(log.entries().isEmpty());
    }
}
