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

import com.kronotop.redis.storage.syncer.impl.OnHeapVolumeSyncQueue;
import com.kronotop.redis.storage.syncer.jobs.AppendStringJob;
import com.kronotop.redis.storage.syncer.jobs.VolumeSyncJob;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OnHeapVolumeSyncQueueTest {
    @Test
    public void test_add() {
        OnHeapVolumeSyncQueue queue = new OnHeapVolumeSyncQueue();
        queue.add(new AppendStringJob("key-1"));
        assertEquals(1, queue.size());
    }

    @Test
    public void test_poll() {
        OnHeapVolumeSyncQueue queue = new OnHeapVolumeSyncQueue();
        queue.add(new AppendStringJob("key-1"));
        queue.add(new AppendStringJob("key-2"));
        queue.add(new AppendStringJob("key-3"));

        List<VolumeSyncJob> keys = queue.poll(10);
        assertEquals(3, keys.size());
    }

    @Test
    public void testPoll_Slice() {
        OnHeapVolumeSyncQueue queue = new OnHeapVolumeSyncQueue();
        queue.add(new AppendStringJob("key-1"));
        queue.add(new AppendStringJob("key-2"));
        queue.add(new AppendStringJob("key-3"));

        List<VolumeSyncJob> jobs = queue.poll(1);
        assertEquals(1, jobs.size());
    }

    @Test
    public void test_poll_Preserve_Insertion_Order() {
        OnHeapVolumeSyncQueue queue = new OnHeapVolumeSyncQueue();
        for (int i = 0; i < 10; i++) {
            queue.add(new AppendStringJob(String.format("key-%s", i)));
        }

        List<VolumeSyncJob> jobs = queue.poll(10);
        for (int i = 0; i < 10; i++) {
            AppendStringJob job = (AppendStringJob) jobs.get(i);
            assertEquals(String.format("key-%s", i), job.key());
        }
    }

    @Test
    public void test_clear() {
        OnHeapVolumeSyncQueue queue = new OnHeapVolumeSyncQueue();
        queue.add(new AppendStringJob("key-1"));
        queue.add(new AppendStringJob("key-2"));
        queue.add(new AppendStringJob("key-3"));

        queue.clear();

        List<VolumeSyncJob> jobs = queue.poll(10);
        assertEquals(0, jobs.size());
    }
}
