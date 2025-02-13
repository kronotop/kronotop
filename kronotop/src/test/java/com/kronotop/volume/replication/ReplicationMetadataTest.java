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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.BaseVolumeIntegrationTest;
import com.kronotop.volume.VolumeSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplicationMetadataTest extends BaseVolumeIntegrationTest {

    @Test
    public void test_findLatestVersionstampedKey() throws IOException {
        ByteBuffer[] entries = getEntries(2);
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, redisVolumeSyncerPrefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        Versionstamp latestVersionstampedKey = ReplicationMetadata.findLatestVersionstampedKey(context, volume.getConfig().subspace());

        assertEquals(versionstampedKeys[versionstampedKeys.length - 1], latestVersionstampedKey);
    }

}