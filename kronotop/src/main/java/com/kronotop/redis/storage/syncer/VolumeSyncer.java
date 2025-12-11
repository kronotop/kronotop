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
 * Synchronizes Redis data structures to Volume storage by processing queued sync jobs.
 *
 * <p>VolumeSyncer polls jobs from a shard's sync queue, executes them via {@link VolumeSyncSession},
 * and persists the data to the underlying Volume. On failure, jobs are re-queued for retry.</p>
 *
 * <p><b>Sync Flow:</b></p>
 * <ol>
 *   <li>Poll up to 1000 jobs from the shard's sync queue</li>
 *   <li>Execute each job via {@link VolumeSyncJob#run(VolumeSyncSession)}</li>
 *   <li>Commit all changes via {@link VolumeSyncSession#sync()}</li>
 *   <li>Run post-hooks for each job (e.g., cleanup, state updates)</li>
 * </ol>
 *
 * <p><b>Error Handling:</b> On any exception during sync, all jobs are re-added to the queue
 * for retry, and the exception is wrapped in a RuntimeException.</p>
 *
 * @see VolumeSyncSession
 * @see VolumeSyncJob
 * @see RedisShard#volumeSyncQueue()
 */
public class VolumeSyncer {
    private final Context context;
    private final RedisShard shard;
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
        VolumeSyncSession session = new VolumeSyncSession(context, shard, prefix);

        for (VolumeSyncJob job : jobs) {
            job.run(session);
        }

        session.sync();
        for (VolumeSyncJob job : jobs) {
            job.postHook(session);
        }
    }

    /**
     * Polls pending sync jobs from the queue and executes them.
     *
     * <p>Polls up to 1000 jobs, syncs them to Volume storage, and runs post-hooks.
     * On failure, all jobs are re-queued for retry.</p>
     *
     * @throws RuntimeException if sync fails (wraps the underlying exception)
     */
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