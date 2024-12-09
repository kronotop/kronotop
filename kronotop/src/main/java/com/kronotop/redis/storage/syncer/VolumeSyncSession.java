/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.storage.DataStructurePack;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.DeleteResult;
import com.kronotop.volume.Prefix;
import com.kronotop.volume.Session;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletionException;

/**
 * The VolumeSyncSession class manages synchronization of volume data by packing
 * data structures into byte buffers and storing versionstamped keys. This class
 * interacts with a RedisShard and a Context to handle data persistence and deletion.
 * <p>
 * VolumeSyncSession is not thread-safe.
 */
public class VolumeSyncSession {
    private final Context context;
    private final RedisShard shard;
    private final RoutingService routing;
    private final LinkedList<ByteBuffer> entries;
    private final LinkedList<Versionstamp> versionstampedKeys;
    private final Prefix prefix;
    private final boolean syncReplicationEnabled;
    private Versionstamp[] versionstamps;
    private int appendCursor;

    public VolumeSyncSession(Context context, RedisShard shard, Prefix prefix, boolean syncReplicationEnabled) {
        this.context = context;
        this.shard = shard;
        this.syncReplicationEnabled = syncReplicationEnabled;
        this.routing = context.getService(RoutingService.NAME);
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

    private void synchronousReplication(AppendResult appendResult) {
        if (!syncReplicationEnabled) {
            return;
        }

        if (appendResult.getEntryMetadataList().length == 0) {
            return;
        }

        Route route = routing.findRoute(ShardKind.REDIS, shard.id());
        Set<Member> syncStandbys = route.syncStandbys();
        if (!syncStandbys.isEmpty()) {
            SynchronousReplication sync = new SynchronousReplication(context, shard.volume().getConfig(), syncStandbys, entries, appendResult);
            if (!sync.run()) {
                // Failed to write to sync standbys. Don't commit metadata to FDB. Vacuum will
                // clean all the garbage.
                throw new KronotopException("Synchronous replication failed due to errors");
            }
        }
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

            Session session = new Session(tr, prefix);

            AppendResult appendResult = shard.volume().append(
                    session,
                    entries.toArray(new ByteBuffer[entries.size()])
            );
            DeleteResult deleteResult = shard.volume().delete(
                    session,
                    versionstampedKeys.toArray(new Versionstamp[versionstampedKeys.size()])
            );

            shard.volume().flush(true);
            synchronousReplication(appendResult);
            tr.commit().join();
            deleteResult.complete();
            versionstamps = appendResult.getVersionstampedKeys();
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