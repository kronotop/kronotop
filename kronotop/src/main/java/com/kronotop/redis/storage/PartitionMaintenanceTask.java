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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Objects;

public class PartitionMaintenanceTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PartitionMaintenanceTask.class);
    private final Context context;
    private final int id;
    private final int numWorkers;
    private final HashMap<Integer, Persistence> cache = new HashMap<>();

    public PartitionMaintenanceTask(Context context, int id) {
        this.id = id;
        this.context = context;
        this.numWorkers = context.getConfig().getInt("persistence.num_workers");
    }

    @Override
    public void run() {
        // TODO: Persistence.run can take transaction as a parameter.
        try {
            for (Partition partition : context.getLogicalDatabase().getPartitions().values()) {
                if (partition.getId() % numWorkers != id) {
                    continue;
                }
                partition.getIndex().flush();
                if (partition.getPersistenceQueue().size() > 0) {
                    Persistence persistence = cache.compute(partition.getId(),
                            (k, value) ->
                                    Objects.requireNonNullElseGet(value,
                                            () -> new Persistence(context, partition)));
                    persistence.run();
                }
            }
        } catch (Exception e) {
            logger.error("Error while running persistence maintenance task {}", e.getMessage());
            throw e;
        }
    }
}
