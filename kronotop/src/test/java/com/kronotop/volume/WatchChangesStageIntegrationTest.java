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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class WatchChangesStageIntegrationTest extends BaseNetworkedVolumeTest {
    @TempDir
    private Path standbyVolumeRootPath;

    @Test
    public void test_watch_changes_stage() throws IOException, InterruptedException {
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
        Replication replication = new Replication(context, config);
        try {
            replication.start();
            Thread.sleep(5000);
            {
                AppendResult result;
                ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(10);
                try (Transaction tr = database.createTransaction()) {
                    Session session = new Session(tr);
                    result = volume.append(session, entries);
                    tr.commit().join();
                    System.out.println("INSERTED 10 KEYS");
                }
                Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
                Versionstamp key = versionstampedKeys[4];
                System.out.println(key);
                WatchChangesStageRunner runner = new WatchChangesStageRunner(context, config, null);
                try (Transaction tr = database.createTransaction()) {
                    System.out.println(runner.findNextSegmentId(tr, key));
                }
            }
            Thread.sleep(5000);
        } finally {
            replication.stop();
        }
    }
}
