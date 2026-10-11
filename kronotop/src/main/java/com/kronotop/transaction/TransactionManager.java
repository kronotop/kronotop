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

    public TransactionManager(Database database) {
        this.database = database;
    }

    public InstrumentedTransaction create() {
        return register(database.createTransaction());
    }

    public InstrumentedTransaction create(ExecutorService executor) {
        return register(database.createTransaction(executor));
    }

    private InstrumentedTransaction register(Transaction underlying) {
        long id = counter.getAndIncrement();
        InstrumentedTransaction tr = new InstrumentedTransaction(id, underlying, this);
        registry.put(id, tr);
        return tr;
    }

    void release(InstrumentedTransaction tr) {
        registry.remove(tr.getId());
    }
}
