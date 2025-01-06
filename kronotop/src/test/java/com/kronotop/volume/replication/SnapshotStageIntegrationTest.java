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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.volume.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotStageIntegrationTest extends BaseNetworkedVolumeIntegrationTest {
    Random random = new Random();
    @TempDir
    private Path standbyVolumeDataDir;

    private ByteBuffer randomBytes(int size) {
        byte[] b = new byte[size];
        random.nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    private void runSnapshotStageAndCheckIt(Versionstamp[] versionstampedKeys) throws IOException {
        VolumeConfig standbyVolumeConfig = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                standbyVolumeDataDir.toString(),
                volume.getConfig().segmentSize()
        );

        ReplicationConfig config = new ReplicationConfig(standbyVolumeConfig, ShardKind.REDIS, 1, ReplicationStage.SNAPSHOT);
        Versionstamp slotId = ReplicationMetadata.newReplication(context, config);

        Replication replication = new Replication(context, slotId, config);
        try {
            replication.start();
            await().atMost(5, TimeUnit.SECONDS).until(() -> {
                try (Transaction tr = database.createTransaction()) {
                    ReplicationSlot slot = ReplicationSlot.load(tr, config, slotId);
                    return slot.isReplicationStageCompleted(ReplicationStage.SNAPSHOT);
                }
            });
        } finally {
            replication.stop();
        }

        try (Transaction tr = database.createTransaction()) {
            ReplicationSlot slot = ReplicationSlot.load(tr, config, slotId);
            for (Snapshot snapshot : slot.getSnapshots().values()) {
                assertEquals(10, snapshot.getTotalEntries());
                assertEquals(snapshot.getTotalEntries(), snapshot.getProcessedEntries());
                assertTrue(snapshot.getLastUpdate() > 0);
            }
        }

        Session session = new Session(prefix);
        Volume replicaVolume = new Volume(context, standbyVolumeConfig);
        for (Versionstamp versionstampedKey : versionstampedKeys) {
            ByteBuffer buf = volume.get(session, versionstampedKey);
            ByteBuffer replicaBuf = replicaVolume.get(session, versionstampedKey);
            assertArrayEquals(buf.array(), replicaBuf.array());
        }

        // Check replication metadata
        try (Transaction tr = database.createTransaction()) {
            ReplicationSlot slot = ReplicationSlot.load(tr, config, slotId);
            assertTrue(slot.isReplicationStageCompleted(ReplicationStage.SNAPSHOT));
            for (Snapshot snapshot : slot.getSnapshots().values()) {
                assertArrayEquals(snapshot.getBegin(), snapshot.getEnd());
            }

            Snapshot snapshot = slot.getSnapshots().lastEntry().getValue();
            assertArrayEquals(snapshot.getEnd(), slot.getReceivedVersionstampedKey());
        }
    }

    @Test
    public void take_snapshot_of_an_existing_volume() throws IOException {
        Versionstamp[] versionstampedKeys;
        AppendResult result;
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefix);
            result = volume.append(session, entries);
            tr.commit().join();
        }

        versionstampedKeys = result.getVersionstampedKeys();
        assertEquals(10, versionstampedKeys.length);

        runSnapshotStageAndCheckIt(versionstampedKeys);
    }

    @Test
    public void take_snapshot_of_an_existing_volume_when_many_segments_exists() throws IOException {
        Versionstamp[] versionstampedKeys;

        long bufferSize = 100480;
        long segmentSize = VolumeConfiguration.segmentSize;
        long numIterations = 2 * (segmentSize / bufferSize);

        ByteBuffer[] entries = new ByteBuffer[(int) numIterations];
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            for (int i = 1; i <= numIterations; i++) {
                entries[i - 1] = randomBytes((int) bufferSize);
            }
            Session session = new Session(tr, prefix);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();
            versionstampedKeys = result.getVersionstampedKeys();
        }

        assertEquals(2, volume.analyze().size());

        runSnapshotStageAndCheckIt(versionstampedKeys);
    }
}