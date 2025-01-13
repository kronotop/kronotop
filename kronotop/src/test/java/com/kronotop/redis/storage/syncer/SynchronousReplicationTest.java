/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.storage.syncer;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.Member;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.Session;
import com.kronotop.volume.Volume;
import com.kronotop.volume.VolumeTestUtils;
import com.kronotop.volume.replication.BaseNetworkedVolumeIntegrationTest;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SynchronousReplicationTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    public void sync_replication_when_have_one_sync_standby() throws IOException {
        ByteBuffer[] entries = VolumeTestUtils.getEntries(2);
        AppendResult appendResult;
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, VolumeTestUtils.prefix);
            appendResult = volume.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] keys = appendResult.getVersionstampedKeys();
        assertEquals(2, keys.length);

        KronotopTestInstance secondInstance = addNewInstance(true);
        Volume standbyVolume = newStandbyVolume(secondInstance.getContext());

        Set<Member> members = new HashSet<>();
        members.add(secondInstance.getMember());
        SynchronousReplication synchronousReplication = new SynchronousReplication(context, volumeConfig, members, List.of(entries), appendResult);
        boolean result = synchronousReplication.run();
        assertTrue(result);

        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, VolumeTestUtils.prefix);
            for (int i = 0; i < keys.length; i++) {
                Versionstamp key = keys[i];
                ByteBuffer entry = entries[i];
                ByteBuffer buffer = standbyVolume.get(session, key);
                assertArrayEquals(entry.array(), buffer.array());
            }
        }
    }
}