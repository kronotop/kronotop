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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class WatchChangesStageIntegrationTest extends BaseNetworkedVolumeTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchChangesStageIntegrationTest.class);

    @TempDir
    private Path standbyVolumeRootPath;

    private Replication newReplication() {
        final Host source;
        final Versionstamp jobId = ReplicationJob.newJob(database, volume.getConfig().subspace(), context.getMember());
        try (Transaction tr = database.createTransaction()) {
            VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, volume.getConfig().subspace());
            source = volumeMetadata.getOwner();
        }

        Host destination = new Host(Role.STANDBY, context.getMember());
        ReplicationConfig config = new ReplicationConfig(
                source,
                destination,
                volume.getConfig().subspace(),
                jobId,
                volume.getConfig().name(),
                volume.getConfig().segmentSize(),
                standbyVolumeRootPath.toString(),
                true
        );
        return new Replication(context, config);
    }

    private Versionstamp[] appendKeys(int number) throws IOException {
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            AppendResult result = volume.append(session, entries);
            tr.commit().join();

            LOGGER.info("Successfully appended {} keys", number);
            return result.getVersionstampedKeys();
        }
    }

    private Volume standbyVolume() throws IOException {
        VolumeConfig standbyVolumeConfig = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                standbyVolumeRootPath.toString(),
                volume.getConfig().segmentSize(),
                volume.getConfig().allowedGarbageRatio()
        );
        System.out.println(standbyVolumeConfig.rootPath());
        return new Volume(context, standbyVolumeConfig);
    }

    private boolean checkAppendedEntries(Versionstamp[] versionstampedKeys, Volume standbyVolume) throws IOException {
        for (Versionstamp versionstampedKey : versionstampedKeys) {
            try {
                ByteBuffer buf = volume.get(versionstampedKey);
                if (buf == null) {
                    return false;
                }
                ByteBuffer replicaBuf = standbyVolume.get(versionstampedKey);
                if (!Arrays.equals(buf.array(), replicaBuf.array())) {
                    return false;
                }
            } catch (SegmentNotFoundException e) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void test_watch_changes_stage() throws IOException {
        Replication replication = newReplication();
        Volume standbyVolume = standbyVolume();
        try {
            replication.start();

            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);

            WatchChangesStageRunner watchChangesStageRunner = (WatchChangesStageRunner) replication.getActiveStageRunner();

            await().atMost(10, TimeUnit.SECONDS).until(watchChangesStageRunner::isWatching);

            Versionstamp[] versionstampedKeys = appendKeys(10);
            await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(versionstampedKeys, standbyVolume));
        } finally {
            replication.stop();
        }
    }
}
