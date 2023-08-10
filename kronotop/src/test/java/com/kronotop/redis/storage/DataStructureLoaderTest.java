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
import com.kronotop.redis.storage.impl.OnHeapLogicalDatabaseImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.DataStructureLoader;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redis.storage.persistence.StringKey;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataStructureLoaderTest extends BaseStorageTest {
    @Test
    public void testDataStructureLoader_STRING() {
        LogicalDatabase logicalDatabase = new OnHeapLogicalDatabaseImpl(RedisService.DEFAULT_LOGICAL_DATABASE);

        for (int i = 0; i < 10; i++) {
            String key = String.format("key-%d", i);
            String value = String.format("value-%d", i);
            logicalDatabase.put(key, new StringValue(value.getBytes(), 0));
            logicalDatabase.getPersistenceQueue().add(new StringKey(key));
        }

        Persistence persistence = new Persistence(context, logicalDatabase);
        persistence.run();

        LogicalDatabase newLogicalDatabase = new OnHeapLogicalDatabaseImpl(RedisService.DEFAULT_LOGICAL_DATABASE);
        DataStructureLoader dataStructureLoader = new DataStructureLoader(context);
        dataStructureLoader.load(newLogicalDatabase, DataStructure.STRING);

        assertEquals(logicalDatabase.size(), newLogicalDatabase.size());

        for (String key : newLogicalDatabase.keySet()) {
            Object newObj = newLogicalDatabase.get(key);
            StringValue newValue = (StringValue) newObj;

            Object obj = logicalDatabase.get(key);
            StringValue value = (StringValue) obj;

            assertEquals(value.getTTL(), newValue.getTTL());
            assertEquals(new String(value.getValue()), new String(newValue.getValue()));
        }
    }
}
