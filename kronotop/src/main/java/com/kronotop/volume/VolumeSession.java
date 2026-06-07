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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;

import javax.annotation.Nonnull;
import java.util.function.IntSupplier;

/**
 * Carries the per-operation context for Volume calls: an optional FoundationDB
 * transaction, the {@link Prefix} that scopes all entries, and a user version
 * counter that orders entries written within the same transaction.
 *
 * <p><b>Transactional mode.</b> A session created with a transaction supports all
 * operations: append, update, delete, and transactional reads that resolve entry
 * metadata through the transaction. Volume never commits; committing or rolling
 * back the transaction is the caller's responsibility.
 * <pre>{@code
 * try (Transaction tr = context.getFoundationDB().createTransaction()) {
 *     VolumeSession session = new VolumeSession(tr, prefix);
 *     AppendResult result = volume.append(session, entry1, entry2);
 *     tr.commit().join();
 * }
 * }</pre>
 *
 * <p><b>Read-only mode.</b> A session created without a transaction serves reads
 * of previously committed entries from the entry metadata cache. Write operations
 * require a transaction and fail with a read-only session.
 * <pre>{@code
 * VolumeSession session = new VolumeSession(prefix);
 * ByteBuffer data = volume.get(session, versionstampedKey);
 * }</pre>
 *
 * <p><b>Prefix isolation.</b> The prefix logically partitions entries within a
 * shared Volume; buckets and Stash data structures each store their entries under
 * their own prefix, and all operations in a session are scoped to it.
 *
 * <p><b>User versions.</b> The user version is the 2-byte component (0-65535) of
 * the FoundationDB versionstamped key that orders entries committed in the same
 * transaction. By default each session keeps its own counter starting at 0. When
 * multiple requests write within the same transaction, an external
 * {@link IntSupplier} can be shared across their sessions to keep versionstamped
 * keys unique and ordered.
 *
 * <p>Sessions are lightweight, short-lived objects, typically one per transaction,
 * with no cleanup required. They are not thread-safe and must not be shared across
 * threads.
 *
 * @see Volume
 * @see Prefix
 * @see UserVersion
 */
public class VolumeSession {
    /**
     * Internal user version counter, used when no external supplier is set.
     */
    private final UserVersion userVersion = new UserVersion();

    /**
     * FoundationDB transaction for this session, or null in read-only mode.
     */
    private final Transaction transaction;

    /**
     * Prefix that scopes all entries accessed through this session.
     */
    private final Prefix prefix;

    /**
     * Optional external user version supplier, shared across sessions when multiple
     * requests write within the same transaction.
     */
    private final IntSupplier userVersionSupplier;

    /**
     * Creates a read-only session without a transaction. Reads of previously committed
     * entries are served from the entry metadata cache; write operations fail.
     *
     * @param prefix the prefix that scopes entries, must not be null
     */
    public VolumeSession(@Nonnull Prefix prefix) {
        this.prefix = prefix;
        this.transaction = null;
        this.userVersionSupplier = null;
    }

    /**
     * Creates a transactional session for reads and writes. The caller is responsible
     * for committing or rolling back the transaction; Volume never commits.
     *
     * @param tr     the FoundationDB transaction for this session, must not be null
     * @param prefix the prefix that scopes entries, must not be null
     */
    public VolumeSession(@Nonnull Transaction tr, @Nonnull Prefix prefix) {
        this.transaction = tr;
        this.prefix = prefix;
        this.userVersionSupplier = null;
    }

    /**
     * Creates a transactional session that draws user versions from an external supplier.
     * Used when multiple requests write within the same transaction: each request gets
     * its own session, but all entries must receive unique, sequential user versions
     * to avoid versionstamp collisions.
     *
     * @param tr                  the FoundationDB transaction for this session, must not be null
     * @param prefix              the prefix that scopes entries, must not be null
     * @param userVersionSupplier supplier that returns the next user version, must not be null
     */
    public VolumeSession(@Nonnull Transaction tr, @Nonnull Prefix prefix, @Nonnull IntSupplier userVersionSupplier) {
        this.transaction = tr;
        this.prefix = prefix;
        this.userVersionSupplier = userVersionSupplier;
    }

    /**
     * Returns the prefix that scopes all operations in this session.
     *
     * @return the prefix
     */
    public Prefix prefix() {
        return prefix;
    }

    /**
     * Returns the FoundationDB transaction for this session, or null if this is a
     * read-only session backed by the entry metadata cache.
     *
     * @return the transaction, or null for read-only sessions
     */
    public Transaction transaction() {
        return transaction;
    }

    /**
     * Returns the next user version for an entry written in this session. Each entry
     * gets a unique, sequential value that becomes part of its versionstamped key.
     * Drawn from the external supplier if one was provided at construction, otherwise
     * from the session's internal counter. Called by Volume internals only.
     *
     * @return the current user version before incrementing
     * @throws TooManyEntriesException if the internal counter exceeds 65535 entries
     */
    protected int getAndIncrementUserVersion() {
        if (userVersionSupplier != null) {
            return userVersionSupplier.getAsInt();
        }
        return userVersion.getAndIncrement();
    }
}
