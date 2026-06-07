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

package com.kronotop.internal;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;

import java.util.function.Function;

/**
 * AutoCloseable wrapper around FDB Transaction that proactively renews
 * transactions approaching the 5-second FDB time limit.
 */
public class RenewableTransaction implements AutoCloseable {

    private static final int TRANSACTION_TOO_OLD = 1007;
    private static final long MAX_AGE_NANOS = 4_900_000_000L; // 4.9 seconds

    private final Database database;
    long createdAtNanos; // package-private for testability
    private Transaction transaction;

    public RenewableTransaction(Database database) {
        this.database = database;
    }

    /**
     * Returns the current transaction if it exists and is not too old,
     * otherwise closes the old one (if any) and creates a fresh one.
     */
    public Transaction getOrRenew() {
        if (transaction == null || System.nanoTime() - createdAtNanos >= MAX_AGE_NANOS) {
            renew();
        }
        return transaction;
    }

    /**
     * Closes the current transaction (if any) and creates a fresh one.
     */
    private void renew() {
        if (transaction != null) {
            transaction.close();
        }
        transaction = database.createTransaction();
        createdAtNanos = System.nanoTime();
    }

    /**
     * Executes the action, retrying once on TRANSACTION_TOO_OLD (error 1007).
     * Any other exception is rethrown immediately.
     */
    public <T> T runWithRetry(Function<Transaction, T> action) {
        return runWithRetry(action, 1);
    }

    /**
     * Executes the action, retrying up to maxRetries times on TRANSACTION_TOO_OLD (error 1007).
     * Any other exception is rethrown immediately.
     */
    public <T> T runWithRetry(Function<Transaction, T> action, int maxRetries) {
        for (int attempt = 0; ; attempt++) {
            try {
                return action.apply(transaction);
            } catch (FDBException e) {
                if (e.getCode() == TRANSACTION_TOO_OLD && attempt < maxRetries) {
                    renew();
                } else {
                    throw e;
                }
            }
        }
    }

    @Override
    public void close() {
        if (transaction != null) {
            transaction.close();
        }
    }
}
