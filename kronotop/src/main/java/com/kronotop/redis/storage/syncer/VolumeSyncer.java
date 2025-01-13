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

import com.kronotop.Context;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.syncer.jobs.VolumeSyncJob;
import com.kronotop.volume.Prefix;

import java.io.IOException;
import java.util.List;

/**
 * The VolumeSyncer class is responsible for synchronizing volume data by persisting a list of jobs
 * into a storage system. It interacts with a given context and shard to ensure the data is correctly
 * persisted and applied.
 */
public class VolumeSyncer {
    private final Context context;
    private final RedisShard shard;
    private final boolean syncReplicationEnabled;
    private final Prefix prefix;

    /**
     * Constructs a new VolumeSyncer.
     *
     * @param context the context of the Kronotop instance.
     * @param shard   the Redis shard to synchronize volume data for.
     */
    public VolumeSyncer(Context context, RedisShard shard) {
        this.context = context;
        this.shard = shard;
        this.syncReplicationEnabled = context.getConfig().getBoolean("redis.volume_syncer.synchronous_replication");
        this.prefix = new Prefix(context.getConfig().getString("redis.volume_syncer.prefix").getBytes());
    }

    /**
     * Checks if the syncer queue is empty.
     *
     * @return true if the syncer queue is empty, false otherwise
     */
    public boolean isQueueEmpty() {
        return shard.volumeSyncQueue().isEmpty();
    }

    /**
     * Sync data for a list of jobs.
     * <p>
     * This method takes a list of jobs as input and persists the corresponding data to a storage system.
     * It uses a {@link VolumeSyncSession} object to pack the data structures into byte buffers,
     * which are then appended to the storage system.
     *
     * @param jobs the list of jobs to persist
     * @throws IOException if an I/O error occurs while persisting the data
     */
    private void sync(List<VolumeSyncJob> jobs) throws IOException {
        VolumeSyncSession session = new VolumeSyncSession(context, shard, prefix, syncReplicationEnabled);

        for (VolumeSyncJob job : jobs) {
            job.run(session);
        }

        session.sync();
        for (VolumeSyncJob job : jobs) {
            job.postHook(session);
        }
    }

    public void run() {
        List<VolumeSyncJob> jobs = shard.volumeSyncQueue().poll(1000);
        if (jobs.isEmpty()) {
            return;
        }

        try {
            sync(jobs);
        } catch (Exception e) {
            for (VolumeSyncJob job : jobs) {
                shard.volumeSyncQueue().add(job);
            }
            throw new RuntimeException(e);
        }
    }
}