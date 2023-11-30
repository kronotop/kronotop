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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.redis.StringValue;
import com.kronotop.redis.storage.impl.OnHeapShardImpl;
import com.kronotop.redis.storage.persistence.DataStructure;
import com.kronotop.redis.storage.persistence.Persistence;
import com.kronotop.redis.storage.persistence.StringKey;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class PersistenceTest extends BaseStorageTest {
    @Test
    public void testPersistence_STRING() {
        Shard shard = new OnHeapShardImpl(0);
        shard.put("key-1", new StringValue("value-1".getBytes(), 0));
        shard.getPersistenceQueue().add(new StringKey("key-1"));

        Persistence persistence = new Persistence(context, shard);
        assertFalse(persistence.isQueueEmpty());
        persistence.run();
        assertTrue(persistence.isQueueEmpty());

        String subspaceName = DirectoryLayout.Builder.
                clusterName(context.getClusterName()).
                internal().
                redis().
                persistence().
                logicalDatabase(LogicalDatabase.NAME).
                shardId("0").
                dataStructure(DataStructure.STRING.toString().toLowerCase()).
                toString();

        Database database = context.getFoundationDB();
        DirectorySubspace subspace;
        try (Transaction tr = database.createTransaction()) {
            subspace = directoryLayer.createOrOpen(tr, Arrays.asList(subspaceName.split("\\."))).join();
            tr.commit().join();
        }
        database.run(tr -> {
            byte[] rawValue = tr.get(subspace.pack("key-1")).join();
            assertNotNull(rawValue);

            try {
                StringValue value = StringValue.decode(rawValue);
                assertEquals(0, value.getTTL());
                assertEquals("value-1", new String(value.getValue()));
            } catch (IOException e) {
                // TODO: Is that OK?
                throw new RuntimeException(e);
            }

            return null;
        });
    }
}
