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

package com.kronotop;

import com.apple.foundationdb.Transaction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper class that combines Kronotop's application context with a FoundationDB transaction
 * and provides automatic user version management for task ordering.
 * <p>
 * This class serves as a unified transactional context that provides access to:
 * <ul>
 *   <li>The application {@link Context} for accessing services and configuration</li>
 *   <li>A FoundationDB {@link Transaction} for database operations</li>
 *   <li>An auto-incrementing user version for task sequencing</li>
 * </ul>
 *
 * <p>The user version is particularly useful when creating multiple tasks within a single
 * transaction, as it provides a monotonically increasing sequence number to maintain task
 * ordering across distributed operations.
 *
 * <p><strong>Thread Safety:</strong> This class uses {@link AtomicInteger} for user version
 * management, making the {@link #userVersion()} method thread-safe. However, the encapsulated
 * {@link Transaction} itself follows FoundationDB's transaction threading model and should
 * typically be used from a single thread.
 *
 * <p><strong>Usage Example:</strong>
 * <pre>{@code
 * Database db = context.getFoundationDB();
 * try (Transaction tr = db.createTransaction()) {
 *     TransactionalContext tx = new TransactionalContext(context, tr);
 *
 *     // Create multiple tasks with automatic versioning
 *     TaskStorage.create(tx.tr(), tx.userVersion(), taskSubspace, task1);
 *     TaskStorage.create(tx.tr(), tx.userVersion(), taskSubspace, task2);
 *
 *     tr.commit().join();
 * }
 * }</pre>
 *
 * @see Context
 * @see Transaction
 * @see com.kronotop.internal.task.TaskStorage
 */
public class TransactionalContext {
    private final Context context;
    private final Transaction tr;
    private final AtomicInteger userVersion;


    /**
     * Constructs a new TransactionalContext with the specified application context and transaction.
     * <p>
     * The user version counter is initialized to 0 and will be incremented with each call to
     * {@link #userVersion()}.
     *
     * @param context the application context providing access to services and configuration
     * @param tr      the FoundationDB transaction for database operations
     */
    public TransactionalContext(Context context, Transaction tr) {
        this.context = context;
        this.tr = tr;
        this.userVersion = new AtomicInteger(0);
    }

    /**
     * Returns the application context.
     * <p>
     * The context provides access to all registered services (e.g., BucketService, VolumeService)
     * and application configuration.
     *
     * @return the application context
     */
    public Context context() {
        return context;
    }

    /**
     * Returns the FoundationDB transaction.
     * <p>
     * This transaction should be used for all database operations within the current
     * transactional scope.
     *
     * @return the FoundationDB transaction
     */
    public Transaction tr() {
        return tr;
    }

    /**
     * Returns the current user version and atomically increments it for the next call.
     * <p>
     * This method provides a monotonically increasing sequence number starting from 0.
     * Each call returns the current value and then increments the internal counter,
     * ensuring unique version numbers within this transactional context.
     *
     * <p>The user version is commonly used for task ordering in {@link com.kronotop.internal.task.TaskStorage}
     * to maintain execution order when multiple tasks are created in a single transaction.
     *
     * <p><strong>Thread Safety:</strong> This method is thread-safe due to the use of
     * {@link AtomicInteger#getAndIncrement()}.
     *
     * @return the current user version before incrementing
     */
    public int userVersion() {
        return userVersion.getAndIncrement();
    }
}
