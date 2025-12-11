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

package com.kronotop.internal;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.kronotop.CommitHook;
import com.kronotop.Context;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.volume.RetryableStateException;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class TransactionUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionUtils.class);

    /**
     * Retrieves the current transaction associated with the given session or creates a new one
     * if no transaction is active. A new transaction is created when the session's "BEGIN"
     * attribute is not set or is false. The method also initializes relevant session attributes
     * for transaction management.
     *
     * @param context the Context object containing the FoundationDB instance used for transaction creation
     * @param session the Session object where transaction attributes are stored and managed
     * @return the current transaction from the session, or a newly created transaction if none exists
     */
    public static Transaction getOrCreateTransaction(Context context, Session session) {
        Attribute<Transaction> transactionAttr = session.attr(SessionAttributes.TRANSACTION);
        Attribute<Boolean> beginAttr = session.attr(SessionAttributes.BEGIN);
        if (!Boolean.TRUE.equals(beginAttr.get())) {
            // There is no ongoing transaction in this Session, create a new one and
            // set AUTO_COMMIT to true.
            // This will be a one-off transaction.
            // WARNING: Using "virtualThreadPerTaskExecutor" can be dangerous here.
            Transaction tr = context.getFoundationDB().createTransaction(context.getVirtualThreadPerTaskExecutor());
            session.setTransaction(tr);
            session.attr(SessionAttributes.AUTO_COMMIT).set(true);
            return tr;
        }
        return transactionAttr.get();
    }

    /**
     * Adds a post-commit hook to the session. Post-commit hooks are executed
     * after a transaction has been successfully committed.
     *
     * @param hook    the {@code CommitHook} instance to be added
     * @param session the {@code Session} object where the hook will be registered
     */
    public static void addPostCommitHook(CommitHook hook, Session session) {
        session.attr(SessionAttributes.POST_COMMIT_HOOKS).get().add(hook);
    }

    /**
     * Commits the given transaction if auto-commit is enabled for the provided session.
     * If the session's auto-commit status is true, the transaction is committed,
     * followed by the execution of post-commit hooks and cleanup operations.
     *
     * @param tr      the {@code Transaction} object representing the current transaction
     * @param session the {@code Session} object that holds the session context and attributes
     */
    public static void commitIfAutoCommitEnabled(Transaction tr, Session session) {
        if (getAutoCommit(session)) {
            try {
                tr.commit().join();
                runPostCommitHooks(session);
            } finally {
                session.unsetTransaction();
            }
        }
    }

    /**
     * Awaits the completion of the given {@link CompletableFuture} without blocking the carrier thread.
     * <p>
     * This method allows a virtual thread to suspend while the provided future completes.
     * Under the hood, the carrier thread is released, and the virtual thread is parked until
     * the result (or exception) is available.
     * <p>
     * If the provided future completes exceptionally with a {@link java.util.concurrent.CompletionException},
     * its cause will be unwrapped and propagated to the returned future to avoid double-wrapping.
     *
     * @param action the {@link CompletableFuture} to await
     * @param <T>    the result type returned by the future
     * @return a new {@link CompletableFuture} that completes with the same result or exception
     * as the provided future, without blocking the carrier thread
     */
    public static <T> CompletableFuture<T> await(CompletableFuture<T> action) {
        CompletableFuture<T> future = new CompletableFuture<>();
        action.thenAccept(future::complete).
                exceptionally(ex -> {
                    Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                            ? ex.getCause()
                            : ex;
                    future.completeExceptionally(cause);
                    return null;
                });
        return future;
    }

    /**
     * Retrieves the auto-commit status of the given session.
     * The method checks the "AUTO_COMMIT" attribute in the session and returns
     * {@code true} if it is set and not {@code false}; otherwise, returns {@code false}.
     *
     * @param session the {@code Session} object from which the auto-commit status will be retrieved
     * @return {@code true} if auto-commit is enabled for the session; otherwise, {@code false}
     */
    public static Boolean getAutoCommit(Session session) {
        Attribute<Boolean> autoCommitAttr = session.attr(SessionAttributes.AUTO_COMMIT);
        return Boolean.TRUE.equals(autoCommitAttr.get());
    }

    /**
     * Determines if the current session is in snapshot read mode.
     * The method checks the session's `SNAPSHOT_READ` attribute and returns true
     * if it is set and evaluates to true; otherwise, it returns false.
     *
     * @param session the {@code Session} object where the snapshot read attribute
     *                will be checked
     * @return {@code true} if snapshot read mode is enabled; otherwise, {@code false}
     */
    public static Boolean isSnapshotRead(Session session) {
        Attribute<Boolean> snapshotReadAttr = session.attr(SessionAttributes.SNAPSHOT_READ);
        return Boolean.TRUE.equals(snapshotReadAttr.get());
    }

    /**
     * Executes all post-commit hooks registered in the specified session.
     * Each post-commit hook is run in sequence, and any exceptions that occur
     * during the execution of a commit hook are logged without interrupting
     * the execution of subsequent hooks.
     *
     * @param session the {@code Session} object containing the post-commit hooks
     *                to be executed
     */
    public static void runPostCommitHooks(Session session) {
        List<CommitHook> commitHooks = session.attr(SessionAttributes.POST_COMMIT_HOOKS).get();
        for (CommitHook commitHook : commitHooks) {
            try {
                commitHook.run();
            } catch (Exception e) {
                LOGGER.error("Error while running a commit hook", e);
            }
        }
    }

    /**
     * Retrieves the current user version associated with the provided session,
     * increments the version counter, and returns the previous version.
     *
     * @param session the {@code Session} object from which the user version counter is retrieved
     * @return the current user version before incrementing
     */
    public static int getUserVersion(Session session) {
        AtomicInteger counter = session.attr(SessionAttributes.USER_VERSION_COUNTER).get();
        return counter.getAndIncrement();
    }

    public static <T> T execute(Context context, Function<? super Transaction, T> action) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return action.apply(tr);
        }
    }

    public static <T> T executeThenCommit(Context context, Function<? super Transaction, T> action) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            T result = action.apply(tr);
            tr.commit().join();
            return result;
        }
    }

    private static Throwable getRootCause(Throwable t) {
        Throwable result = t;
        while (result.getCause() != null && result.getCause() != result) {
            result = result.getCause();
        }
        return result;
    }

    public static Retry retry(int maxAttempts, Duration waitDuration) {
        return Retry.of("transaction-with-retry", RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .waitDuration(waitDuration)
                .retryOnException(throwable -> {
                    if (throwable instanceof RetryableStateException) {
                        return true;
                    }

                    Throwable root = getRootCause(throwable);

                    // Network-transient
                    if (root instanceof IOException) {
                        return true;
                    }

                    // FDB retry-safe codes
                    if (root instanceof FDBException fdb) {
                        int code = fdb.getCode();
                        return code == 1007 || // transaction_too_old
                                code == 1020 || // not_committed (conflict)
                                code == 1021 || // commit_unknown_result
                                code == 1031; // transaction_timed_out
                    }

                    return false;
                }).build());
    }
}
