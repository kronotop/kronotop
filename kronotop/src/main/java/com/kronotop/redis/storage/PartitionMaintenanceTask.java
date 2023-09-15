/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.redis.storage;

import com.kronotop.core.Context;
import com.kronotop.redis.storage.persistence.Persistence;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public class PartitionMaintenanceTask implements Runnable {
    private final Context context;
    private final int id;
    private final int numWorkers;
    private final HashMap<Integer, Persistence> cache = new HashMap<>();
    private final ConcurrentMap<String, LogicalDatabase> logicalDatabases;

    public PartitionMaintenanceTask(Context context, int id, ConcurrentMap<String, LogicalDatabase> logicalDatabases) {
        this.id = id;
        this.context = context;
        this.numWorkers = context.getConfig().getInt("persistence.num_workers");
        ;
        this.logicalDatabases = logicalDatabases;
    }

    @Override
    public void run() {
        for (LogicalDatabase logicalDatabase : logicalDatabases.values()) {
            for (Partition partition : logicalDatabase.getPartitions().values()) {
                if (partition.getId() % numWorkers != id) {
                    continue;
                }
                partition.getIndex().flush();
                if (partition.getPersistenceQueue().size() > 0) {
                    Persistence persistence = cache.compute(partition.getId(),
                            (k, value) ->
                                    Objects.requireNonNullElseGet(value,
                                            () -> new Persistence(context, logicalDatabase.getName(), partition)));
                    persistence.run();
                }
            }
        }
    }
}
