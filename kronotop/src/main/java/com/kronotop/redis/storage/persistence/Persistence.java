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

package com.kronotop.redis.storage.persistence;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.redis.storage.RedisShard;
import com.kronotop.redis.storage.persistence.jobs.PersistenceJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * The Persistence class is responsible for persisting data to the FoundationDB cluster.
 * It provides methods for persisting different types of data structures such as strings and hashes.
 */
public class Persistence {
    private static final Logger LOGGER = LoggerFactory.getLogger(Persistence.class);
    private final Context context;
    private final RedisShard shard;

    /**
     * Constructs a Persistence object with the given context and shard.
     * Initializes the layout and directories for each data structure in the persistence layer.
     *
     * @param context the Context object containing information about the cluster and database
     * @param shard   the Shard object representing a shard in the cluster
     */
    public Persistence(Context context, RedisShard shard) {
        this.context = context;
        this.shard = shard;
    }

    /**
     * Checks if the persistence queue of a shard is empty.
     *
     * @return true if the persistence queue is empty, false otherwise
     */
    public boolean isQueueEmpty() {
        return shard.persistenceQueue().size() == 0;
    }

    /**
     * Persist data for a list of jobs.
     * <p>
     * This method takes a list of jobs as input and persists the corresponding data to a storage system.
     * It uses a {@link PersistenceSession} object to pack the data structures into byte buffers,
     * which are then appended to the storage system.
     *
     * @param jobs the list of jobs to persist
     * @throws IOException if an I/O error occurs while persisting the data
     */
    private void persist(List<PersistenceJob> jobs) throws IOException {
        PersistenceSession session = new PersistenceSession(context, shard, jobs.size());
        for (PersistenceJob job : jobs) {
            job.run(session);
        }
        Versionstamp[] versionstampedKeys = session.persist();
        for (PersistenceJob job : jobs) {
            job.postHook(session, versionstampedKeys);
        }
    }

    public void run() {
        List<PersistenceJob> jobs = shard.persistenceQueue().poll(1000);
        if (jobs.isEmpty()) {
            return;
        }

        try {
            persist(jobs);
        } catch (Exception e) {
            for (PersistenceJob job : jobs) {
                shard.persistenceQueue().add(job);
            }
            throw new RuntimeException(e);
        }
    }
}