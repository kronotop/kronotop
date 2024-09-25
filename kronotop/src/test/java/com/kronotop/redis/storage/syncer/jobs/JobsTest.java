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

package com.kronotop.redis.storage.syncer.jobs;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.redis.storage.BaseStorageTest;
import com.kronotop.redis.storage.syncer.VolumeSyncQueue;
import com.kronotop.redis.storage.syncer.impl.OnHeapVolumeSyncQueue;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JobsTest extends BaseStorageTest {
    byte[] trVersion = {0, 0, 11, 26, 50, 29, 118, -70, 0, 0};

    @Test
    public void test_AppendStringJob_the_same_key() {
        VolumeSyncQueue queue = new OnHeapVolumeSyncQueue();

        AppendStringJob first = new AppendStringJob("foobar");
        AppendStringJob second = new AppendStringJob("foobar");

        queue.add(first);
        queue.add(second);

        assertEquals(1, queue.size());
    }

    @Test
    public void test_AppendHashFieldJob_the_same_key() {
        VolumeSyncQueue queue = new OnHeapVolumeSyncQueue();

        AppendHashFieldJob first = new AppendHashFieldJob("foobar", "barfoo");
        AppendHashFieldJob second = new AppendHashFieldJob("foobar", "barfoo");

        queue.add(first);
        queue.add(second);

        assertEquals(1, queue.size());
    }

    @Test
    public void test_STRING_jobs() {
        VolumeSyncQueue queue = new OnHeapVolumeSyncQueue();

        Versionstamp versionstamp = Versionstamp.complete(trVersion);
        DeleteByVersionstampJob first = new DeleteByVersionstampJob(versionstamp);
        AppendStringJob second = new AppendStringJob("foobar");

        queue.add(first);
        queue.add(second);

        assertEquals(2, queue.size());
    }

    @Test
    public void test_DeleteStringJob_the_same_key() {
        VolumeSyncQueue queue = new OnHeapVolumeSyncQueue();

        Versionstamp versionstamp = Versionstamp.complete(trVersion);
        DeleteByVersionstampJob first = new DeleteByVersionstampJob(versionstamp);
        DeleteByVersionstampJob second = new DeleteByVersionstampJob(versionstamp);

        queue.add(first);
        queue.add(second);

        assertEquals(1, queue.size());
    }
}
