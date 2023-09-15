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

import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.impl.OnHeapPartitionImpl;
import com.kronotop.redis.storage.persistence.StringKey;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PartitionMaintenanceTaskTest extends BaseStorageTest {
    @Test
    public void testRun() {
        ConcurrentMap<String, LogicalDatabase> logicalDatabases = new ConcurrentHashMap<>();
        LogicalDatabase logicalDatabase = new LogicalDatabase("0");
        logicalDatabases.put("0", logicalDatabase);

        Partition partition = new OnHeapPartitionImpl(0);
        partition.put("key-1", new StringValue("value-1".getBytes(), 0));
        partition.getPersistenceQueue().add(new StringKey("key-1"));
        logicalDatabase.getPartitions().put(0, partition);

        PartitionMaintenanceTask partitionMaintenanceTask = new PartitionMaintenanceTask(context, 0, logicalDatabases);
        partitionMaintenanceTask.run();

        assertEquals(0, partition.getPersistenceQueue().size());

    }
}
