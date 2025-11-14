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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;

import javax.annotation.Nonnull;

/**
 * VolumeSession encapsulates the context for Volume operations, managing the transaction,
 * namespace prefix, and user version tracking for entries within a single session.
 *
 * <p><b>Core Responsibilities:</b></p>
 * <ul>
 *   <li><b>Transaction Management</b>: Associates Volume operations with a FoundationDB transaction</li>
 *   <li><b>Namespace Isolation</b>: Enforces prefix-based logical partitioning of entries</li>
 *   <li><b>Entry Ordering</b>: Tracks user versions for ordering multiple entries within a transaction</li>
 * </ul>
 *
 * <p><b>Two Usage Modes:</b></p>
 *
 * <p><b>1. Transactional Mode (Write Operations)</b></p>
 * <p>Used for operations that modify data (append, update, delete). Requires a FoundationDB transaction:</p>
 * <pre>{@code
 * try (Transaction tr = context.getFoundationDB().createTransaction()) {
 *     VolumeSession session = new VolumeSession(tr, prefix);
 *     AppendResult result = volume.append(session, entry1, entry2);
 *     tr.commit().join();
 * }
 * }</pre>
 *
 * <p><b>2. Read-Only Mode (Cached Reads)</b></p>
 * <p>Used for read operations that can leverage the entry metadata cache without a transaction:</p>
 * <pre>{@code
 * VolumeSession session = new VolumeSession(prefix);
 * ByteBuffer data = volume.get(session, versionstampedKey);
 * // Reads from cache, no transaction needed
 * }</pre>
 *
 * <p><b>Prefix-Based Isolation:</b></p>
 * <p>The prefix parameter provides logical namespace isolation within a Volume. Common use cases:</p>
 * <ul>
 *   <li><b>Bucket separation</b>: Each bucket gets its own prefix for isolated storage</li>
 *   <li><b>Redis compatibility</b>: Redis data structures use dedicated prefixes</li>
 *   <li><b>Multi-tenancy</b>: Different tenants or applications can share a Volume with isolated prefixes</li>
 * </ul>
 *
 * <p><b>Example - Multiple Prefixes:</b></p>
 * <pre>{@code
 * Prefix bucketPrefix = new Prefix("bucket:users");
 * Prefix redisPrefix = new Prefix("redis:strings");
 *
 * // Operations on different prefixes are isolated
 * try (Transaction tr = context.getFoundationDB().createTransaction()) {
 *     VolumeSession bucketSession = new VolumeSession(tr, bucketPrefix);
 *     VolumeSession redisSession = new VolumeSession(tr, redisPrefix);
 *
 *     volume.append(bucketSession, userData);   // Stored under bucket:users prefix
 *     volume.append(redisSession, stringData);  // Stored under redis:strings prefix
 *     tr.commit().join();
 * }
 * }</pre>
 *
 * <p><b>User Version Tracking:</b></p>
 * <p>Each session maintains its own {@link UserVersion} counter (0-65535) to order entries
 * within a transaction. This enables FoundationDB to create unique, ordered versionstamped keys
 * for all entries in a single transaction commit.</p>
 *
 * <p><b>Session Lifecycle:</b></p>
 * <ul>
 *   <li>Sessions are lightweight, short-lived objects (typically one per transaction)</li>
 *   <li>Each session has an independent user version counter starting at 0</li>
 *   <li>Sessions do not need explicit cleanup (no close() method)</li>
 *   <li>A new session should be created for each transaction</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b></p>
 * <p>VolumeSession instances are not thread-safe and should not be shared across threads.
 * Each thread should create its own session with its own FoundationDB transaction.</p>
 *
 * @see Volume
 * @see Prefix
 * @see UserVersion
 * @see com.apple.foundationdb.Transaction
 */
public class VolumeSession {
    /**
     * User version tracker for ordering entries within this session's transaction.
     * Each entry appended increments this counter to ensure proper ordering.
     */
    private final UserVersion userVersion = new UserVersion();

    /**
     * FoundationDB transaction for this session, or null for read-only cached operations.
     */
    private final Transaction transaction;

    /**
     * Namespace prefix for logical isolation of entries within the Volume.
     */
    private final Prefix prefix;

    /**
     * Creates a read-only VolumeSession without a transaction for cached read operations.
     *
     * <p>This constructor is used when reading data that can be served from the entry metadata
     * cache without requiring a FoundationDB transaction. Suitable for:</p>
     * <ul>
     *   <li>Reading previously committed entries via {@link Volume#get(VolumeSession, com.apple.foundationdb.tuple.Versionstamp)}</li>
     *   <li>Operations that don't require transactional consistency</li>
     * </ul>
     *
     * <p><b>Note:</b> Write operations (append, update, delete) require a transaction and will
     * fail if attempted with a read-only session.</p>
     *
     * @param prefix the namespace prefix for entry isolation, must not be null
     */
    public VolumeSession(@Nonnull Prefix prefix) {
        this.prefix = prefix;
        this.transaction = null;
    }

    /**
     * Creates a transactional VolumeSession for read and write operations.
     *
     * <p>This constructor is used for all operations that modify data or require
     * transactional consistency:</p>
     * <ul>
     *   <li>Appending new entries: {@link Volume#append(VolumeSession, java.nio.ByteBuffer...)}</li>
     *   <li>Updating existing entries: {@link Volume#update(VolumeSession, KeyEntry...)}</li>
     *   <li>Deleting entries: {@link Volume#delete(VolumeSession, com.apple.foundationdb.tuple.Versionstamp...)}</li>
     *   <li>Transactional reads: Reading data within the context of an active transaction</li>
     * </ul>
     *
     * <p><b>Important:</b> The caller is responsible for committing or rolling back the
     * transaction. Volume operations do NOT commit transactions automatically.</p>
     *
     * @param tr     the FoundationDB transaction for this session, must not be null
     * @param prefix the namespace prefix for entry isolation, must not be null
     */
    public VolumeSession(@Nonnull Transaction tr, @Nonnull Prefix prefix) {
        this.transaction = tr;
        this.prefix = prefix;
    }

    /**
     * Returns the namespace prefix for this session.
     *
     * <p>The prefix determines the logical namespace within the Volume where entries
     * will be stored or retrieved. All operations within this session are scoped to
     * this prefix.</p>
     *
     * @return the prefix for namespace isolation
     */
    public Prefix prefix() {
        return prefix;
    }

    /**
     * Returns the FoundationDB transaction for this session, or null for read-only sessions.
     *
     * <p>A null transaction indicates this is a read-only session that uses the entry
     * metadata cache. Non-null transactions are required for all write operations.</p>
     *
     * @return the FoundationDB transaction, or null for read-only sessions
     */
    public Transaction transaction() {
        return transaction;
    }

    /**
     * Returns the current user version and increments it for the next entry.
     *
     * <p>This method is called internally by Volume operations to assign sequential
     * user versions to entries within a transaction. Each entry gets a unique user
     * version (0, 1, 2, ...) that becomes part of its final versionstamped key.</p>
     *
     * <p><b>Internal Use Only:</b> This method is package-protected and should only
     * be called by Volume implementation classes.</p>
     *
     * @return the current user version before incrementing
     * @throws TooManyEntriesException if more than 65535 entries are appended in this session
     */
    protected int getAndIncrementUserVersion() {
        return userVersion.getAndIncrement();
    }
}
