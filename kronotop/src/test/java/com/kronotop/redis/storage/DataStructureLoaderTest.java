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

import com.kronotop.redis.RedisService;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.impl.OnHeapPartitionImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.DataStructureLoader;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redis.storage.persistence.StringKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataStructureLoaderTest extends BaseStorageTest {
    @Test
    public void testDataStructureLoader_STRING() {
        Partition partition = new OnHeapPartitionImpl(0);

        for (int i = 0; i < 10; i++) {
            String key = String.format("key-%d", i);
            String value = String.format("value-%d", i);
            partition.put(key, new StringValue(value.getBytes(), 0));
            partition.getPersistenceQueue().add(new StringKey(key));
        }

        Persistence persistence = new Persistence(context, RedisService.DEFAULT_LOGICAL_DATABASE, partition);
        persistence.run();

        Partition newPartition = new OnHeapPartitionImpl(0);
        DataStructureLoader dataStructureLoader = new DataStructureLoader(context);
        dataStructureLoader.load(newPartition, DataStructure.STRING);

        assertEquals(partition.size(), newPartition.size());

        for (String key : newPartition.keySet()) {
            Object newObj = newPartition.get(key);
            StringValue newValue = (StringValue) newObj;

            Object obj = partition.get(key);
            StringValue value = (StringValue) obj;

            assertEquals(value.getTTL(), newValue.getTTL());
            assertEquals(new String(value.getValue()), new String(newValue.getValue()));
        }
    }
}
