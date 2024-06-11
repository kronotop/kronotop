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

package com.kronotop.redis.storage;

import com.kronotop.redis.storage.persistence.Key;
import com.kronotop.redis.storage.persistence.StringKey;
import com.kronotop.redis.storage.persistence.impl.OnHeapPersistenceQueue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OnHeapPersistenceQueueTest {
    @Test
    public void testAdd() {
        OnHeapPersistenceQueue p = new OnHeapPersistenceQueue();
        p.add(new StringKey("key-1"));
        assertEquals(1, p.size());
    }

    @Test
    public void testPoll() {
        OnHeapPersistenceQueue p = new OnHeapPersistenceQueue();
        p.add(new StringKey("key-1"));
        p.add(new StringKey("key-2"));
        p.add(new StringKey("key-3"));

        List<Key> keys = p.poll(10);
        assertEquals(3, keys.size());
    }

    @Test
    public void testPoll_Slice() {
        OnHeapPersistenceQueue p = new OnHeapPersistenceQueue();
        p.add(new StringKey("key-1"));
        p.add(new StringKey("key-2"));
        p.add(new StringKey("key-3"));

        List<Key> keys = p.poll(1);
        assertEquals(1, keys.size());
    }

    @Test
    public void testPoll_Preserve_Insertion_Order() {
        OnHeapPersistenceQueue p = new OnHeapPersistenceQueue();
        for (int i = 0; i < 10; i++) {
            p.add(new StringKey(String.format("key-%s", i)));
        }

        List<Key> keys = p.poll(10);
        for (int i = 0; i < 10; i++) {
            Key key = keys.get(i);
            assertEquals(String.format("key-%s", i), key.getKey());
        }
    }

    @Test
    public void testClear() {
        OnHeapPersistenceQueue p = new OnHeapPersistenceQueue();
        p.add(new StringKey("key-1"));
        p.add(new StringKey("key-2"));
        p.add(new StringKey("key-3"));

        p.clear();

        List<Key> keys = p.poll(10);
        assertEquals(0, keys.size());
    }
}
