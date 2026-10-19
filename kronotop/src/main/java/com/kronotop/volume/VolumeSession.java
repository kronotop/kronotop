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
import com.kronotop.volume.replication.ReplicationHint;

import javax.annotation.Nonnull;
import java.util.function.IntSupplier;

/**
 * Per-call context for {@link Volume} operations: an optional FoundationDB
 * transaction, the {@link Prefix} that scopes all entries, and the user version
 * counter that orders entries written in the same transaction.
 *
 * <p>Without a transaction the session is read-only and reads come from the entry
 * metadata cache. Writes need a transaction. Volume never commits; the caller does.
 *
 * <p>Each session starts its own user version counter at 0. When several requests
 * write in one transaction, pass a shared {@link IntSupplier} so the versionstamped
 * keys stay unique.
 *
 * <p>Not thread-safe.
 */
public class VolumeSession {
    private final UserVersion userVersion = new UserVersion();
    private final Transaction transaction;
    private final Prefix prefix;
    private final IntSupplier userVersionSupplier;
    private ReplicationHint replicationHint;

    /**
     * Read-only session. Reads come from the entry metadata cache.
     */
    public VolumeSession(@Nonnull Prefix prefix) {
        this.prefix = prefix;
        this.transaction = null;
        this.userVersionSupplier = null;
    }

    /**
     * Transactional session with its own user version counter.
     */
    public VolumeSession(@Nonnull Transaction tr, @Nonnull Prefix prefix) {
        this.transaction = tr;
        this.prefix = prefix;
        this.userVersionSupplier = null;
    }

    /**
     * Transactional session that takes user versions from a shared supplier.
     * Use it when several requests write in the same transaction.
     */
    public VolumeSession(@Nonnull Transaction tr, @Nonnull Prefix prefix, @Nonnull IntSupplier userVersionSupplier) {
        this.transaction = tr;
        this.prefix = prefix;
        this.userVersionSupplier = userVersionSupplier;
    }

    public Prefix prefix() {
        return prefix;
    }

    /**
     * Returns the transaction, or null for a read-only session.
     */
    public Transaction transaction() {
        return transaction;
    }

    /**
     * Returns the next user version for an entry. Comes from the shared supplier if
     * set, else from the internal counter.
     *
     * @throws TooManyEntriesException if the internal counter passes 65535
     */
    protected int getAndIncrementUserVersion() {
        if (userVersionSupplier != null) {
            return userVersionSupplier.getAsInt();
        }
        return userVersion.getAndIncrement();
    }

    public void setReplicationHint(ReplicationHint hint) {
        this.replicationHint = hint;
    }

    public ReplicationHint getReplicationHint() {
        return replicationHint;
    }
}
