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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * In-memory log of the most recent slow transactions. A transaction is logged when it is closed
 * and its total duration reaches the threshold. Transactions that own a watch are skipped, because
 * they stay open until the watched key changes. The log is lost on restart.
 */
public class SlowTransactionLog {
    private final long thresholdNanos;
    private final int maxEntries;
    // Keyed by transaction id. Ids are monotonic, so the oldest entry is the first key.
    private final ConcurrentSkipListMap<Long, SlowTransaction> entries = new ConcurrentSkipListMap<>();

    public SlowTransactionLog(long thresholdMillis, int maxEntries) {
        this.thresholdNanos = TimeUnit.MILLISECONDS.toNanos(thresholdMillis);
        this.maxEntries = maxEntries;
    }

    /**
     * Records the closed transaction when it ran at least as long as the threshold.
     */
    public void record(InstrumentedTransaction tr) {
        TransactionMetrics metrics = tr.getMetrics();
        if (metrics.isWatching() || metrics.getTotalDurationNanos() < thresholdNanos) {
            return;
        }
        entries.put(tr.getId(), SlowTransaction.of(tr));
        while (entries.size() > maxEntries) {
            entries.pollFirstEntry();
        }
    }

    /**
     * Returns a copy of the logged transactions, newest first.
     */
    public List<SlowTransaction> entries() {
        return new ArrayList<>(entries.descendingMap().values());
    }
}
