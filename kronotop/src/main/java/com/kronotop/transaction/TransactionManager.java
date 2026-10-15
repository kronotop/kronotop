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
    private final SlowTransactionLog slowLog;

    public TransactionManager(
            Database database,
            AggregatedTransactionMetrics aggregatedTransactionMetrics,
            SlowTransactionLog slowLog
    ) {
        this.database = database;
        this.aggregatedTransactionMetrics = aggregatedTransactionMetrics;
        this.slowLog = slowLog;
    }

    public InstrumentedTransaction create() {
        return register(database.createTransaction());
    }

    public InstrumentedTransaction create(ExecutorService executor) {
        return register(database.createTransaction(executor));
    }

    /**
     * Creates a transaction for the command that runs on the session. The transaction takes
     * the client id and the command metadata of the session, so TRANSACTION LIST can show them.
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
            slowLog.record(tr);
        }
    }

    public SlowTransactionLog getSlowLog() {
        return slowLog;
    }

    /**
     * Closes every open transaction. Called once at instance shutdown.
     */
    public void closeAll() {
        for (InstrumentedTransaction tr : registry.values()) {
            tr.close();
        }
    }

    /**
     * Returns the open transaction with the given id, or null when there is none.
     */
    public InstrumentedTransaction get(long id) {
        return registry.get(id);
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
