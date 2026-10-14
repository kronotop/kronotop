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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.kronotop.metrics.AggregatedTransactionMetrics;
import com.kronotop.server.Session;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The single place that creates FDB transactions. Every transaction gets a unique id,
 * is tracked until it is closed, and is wrapped in an {@link InstrumentedTransaction}.
 */
public class TransactionManager {
    private final Database database;
    private final AtomicLong counter = new AtomicLong();
    private final Map<Long, InstrumentedTransaction> registry = new ConcurrentHashMap<>(1 << 16);
    private final AggregatedTransactionMetrics aggregatedTransactionMetrics;

    public TransactionManager(
            Database database,
            AggregatedTransactionMetrics aggregatedTransactionMetrics
    ) {
        this.database = database;
        this.aggregatedTransactionMetrics = aggregatedTransactionMetrics;
    }

    public InstrumentedTransaction create() {
        return register(database.createTransaction());
    }

    public InstrumentedTransaction create(ExecutorService executor) {
        return register(database.createTransaction(executor));
    }

    /**
     * Creates a transaction for the command that runs on the session. The transaction takes
     * the client id and the command metadata of the session, so TRANSACTIONS can show them.
     */
    public InstrumentedTransaction create(Session session) {
        InstrumentedTransaction tr = create();
        session.bind(tr);
        return tr;
    }

    private InstrumentedTransaction register(Transaction underlying) {
        long id = counter.getAndIncrement();
        InstrumentedTransaction tr = new InstrumentedTransaction(id, underlying, this);
        registry.put(id, tr);
        aggregatedTransactionMetrics.incrementCreated();
        return tr;
    }

    void release(InstrumentedTransaction tr) {
        // A second close() must not fold the totals twice.
        if (registry.remove(tr.getId()) != null) {
            aggregatedTransactionMetrics.record(tr.getMetrics());
        }
    }

    public int getActiveTransactionCount() {
        return registry.size();
    }

    /**
     * Returns a read-only view of the open transactions. The view is weakly consistent;
     * transactions may be opened or closed while it is iterated.
     */
    public Collection<InstrumentedTransaction> transactions() {
        return Collections.unmodifiableCollection(registry.values());
    }

    /**
     * Returns the start time (System.nanoTime) of the oldest open transaction, or 0 when none is open.
     * Transactions that own a watch are skipped. They stay open until the watched key changes.
     */
    public long oldestActiveTransactionStartNanos() {
        long oldest = 0;
        for (InstrumentedTransaction tr : registry.values()) {
            if (tr.getMetrics().isWatching()) {
                continue;
            }
            long start = tr.getMetrics().getStartNanos();
            if (oldest == 0 || start < oldest) {
                oldest = start;
            }
        }
        return oldest;
    }

    public int getWatchingTransactionCount() {
        int count = 0;
        for (InstrumentedTransaction tr : registry.values()) {
            if (tr.getMetrics().isWatching()) {
                count++;
            }
        }
        return count;
    }

    AggregatedTransactionMetrics getAggregatedTransactionMetrics() {
        return aggregatedTransactionMetrics;
    }
}
