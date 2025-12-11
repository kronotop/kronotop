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

package com.kronotop.redis.storage.syncer;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.redis.storage.DataStructurePack;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.DeleteResult;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.VolumeSession;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.CompletionException;

/**
 * Manages a batch of Redis data structure writes and deletes to Volume storage within a single
 * FoundationDB transaction.
 *
 * <p>VolumeSyncSession accumulates entries via {@link #pack(DataStructurePack)} and deletions via
 * {@link #delete(Versionstamp)}, then commits them atomically via {@link #sync()}. After sync,
 * versionstamps for appended entries are available via {@link #versionstamps()}.</p>
 *
 * <p><b>Usage:</b></p>
 * <ol>
 *   <li>Call {@link #pack(DataStructurePack)} for each data structure to persist</li>
 *   <li>Call {@link #delete(Versionstamp)} for each entry to remove</li>
 *   <li>Call {@link #sync()} to commit all changes atomically</li>
 *   <li>Use {@link #versionstamps()} to get keys for persisted entries</li>
 * </ol>
 *
 * <p><b>Thread Safety:</b> Not thread-safe. Each session should be used by a single thread.</p>
 *
 * @see VolumeSyncer
 * @see DataStructurePack
 */
public class VolumeSyncSession {
    private final Context context;
    private final RedisShard shard;
    private final LinkedList<ByteBuffer> entries;
    private final LinkedList<Versionstamp> versionstampedKeys;
    private final Prefix prefix;
    private Versionstamp[] versionstamps;
    private int appendCursor;

    public VolumeSyncSession(Context context, RedisShard shard, Prefix prefix) {
        this.context = context;
        this.shard = shard;
        this.versionstampedKeys = new LinkedList<>();
        this.entries = new LinkedList<>();
        this.prefix = prefix;
    }

    /**
     * Packs the provided DataStructurePack into a ByteBuffer and stores it in the entries array.
     * The method increments the size based on the buffer's remaining bytes and returns the current cursor position.
     * The cursor is then incremented for the next pack operation.
     *
     * @param dataStructurePack the DataStructurePack to be packed and stored
     * @return the position in the entry array where the ByteBuffer was stored
     */
    public int pack(DataStructurePack dataStructurePack) {
        ByteBuffer buffer = dataStructurePack.pack();
        try {
            entries.add(buffer);
            return appendCursor;
        } finally {
            appendCursor++;
        }
    }

    public void delete(Versionstamp versionstampedKey) {
        versionstampedKeys.add(versionstampedKey);
    }

    private AppendResult appendEntries(VolumeSession session) throws IOException {
        if (entries.isEmpty()) {
            return null;
        }
        return shard.volume().append(
                session,
                entries.toArray(new ByteBuffer[0])
        );
    }

    private DeleteResult deleteEntries(VolumeSession session) {
        if (versionstampedKeys.isEmpty()) {
            return null;
        }
        return shard.volume().delete(
                session,
                versionstampedKeys.toArray(new Versionstamp[0])
        );
    }

    /**
     * Persists the data stored in the VolumeSyncSession to a storage system.
     * The data is packed into byte buffers and appended to the storage system.
     *
     * @throws IOException if an I/O error occurs while persisting the data
     */
    public void sync() throws IOException {
        // TODO: We should consider the transaction time limit: 5 seconds
        try (Transaction tr = context.getFoundationDB().createTransaction()) {

            // memtier_benchmark sets the same key many times, and this triggers a bizarre bug in VolumeSyncer
            // See https://forums.foundationdb.org/t/why-is-read-or-wrote-unreadable-key-necessary/3753
            tr.options().setBypassUnreadable();

            VolumeSession session = new VolumeSession(tr, prefix);

            AppendResult appendResult = appendEntries(session);
            DeleteResult deleteResult = deleteEntries(session);

            shard.volume().flush();

            tr.commit().join();
            if (deleteResult != null) {
                deleteResult.complete();
            }
            if (appendResult != null) {
                versionstamps = appendResult.getVersionstampedKeys();
            }
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException) {
                if (((FDBException) e.getCause()).getCode() == 2021) {
                    return;
                }
            }
            throw e;
        }
    }

    public Versionstamp[] versionstamps() {
        return versionstamps;
    }

    public RedisShard shard() {
        return shard;
    }
}