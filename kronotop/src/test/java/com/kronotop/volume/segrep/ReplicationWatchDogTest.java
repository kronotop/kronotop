/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.segrep;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationWatchDogTest extends BaseNetworkedVolumeIntegrationTest {

    private void appendEntries(int number, int length) throws IOException {
        ByteBuffer[] entries = getEntries(number, length);
        AppendResult result;
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }
        assertEquals(number, result.getVersionstampedKeys().length);
    }

    @Test
    void test(@TempDir Path destination) throws IOException {
        int length = 1024;
        int number = Math.toIntExact(volume.getConfig().segmentSize() / length);
        number += (number / 2);
        appendEntries(number, length);

        VolumeReplication watchDog = new VolumeReplication(context, ShardKind.REDIS, 1, destination.toString());
        watchDog.run();
    }
}