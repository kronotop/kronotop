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

import java.util.concurrent.atomic.LongAdder;

public class TransactionMetrics {
    private final LongAdder transactionsCreated = new LongAdder();
    private final LongAdder transactionsCommitted = new LongAdder();
    private final LongAdder transactionsRolledBack = new LongAdder();

    public void increaseTransactionsCreated() {
        transactionsCreated.increment();
    }

    public long getTransactionsCreated() {
        return transactionsCreated.sum();
    }

    public void increaseTransactionsCommitted() {
        transactionsCommitted.increment();
    }

    public long getTransactionsCommitted() {
        return transactionsCommitted.sum();
    }

    public void increaseTransactionsRolledBack() {
        transactionsRolledBack.increment();
    }

    public long getTransactionsRolledBack() {
        return transactionsRolledBack.sum();
    }
}
