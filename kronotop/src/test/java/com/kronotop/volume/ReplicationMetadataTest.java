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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseMetadataStoreTest;
import com.kronotop.JSONUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplicationMetadataTest extends BaseMetadataStoreTest {

    @Test
    public void test_ReplicationMetadata_SnapshotJob() {
        ReplicationMetadata metadata = new ReplicationMetadata();

        byte[] begin = Versionstamp.incomplete(0).getBytes();
        byte[] end = Versionstamp.incomplete(1).getBytes();

        SnapshotJob snapshotJob = new SnapshotJob();
        Snapshot snapshot = new Snapshot(0, 1, begin, end);
        snapshotJob.put(snapshot.getSegmentId(), snapshot);
        metadata.setSnapshotJob(snapshotJob);

        byte[] data = JSONUtils.writeValueAsBytes(metadata);
        ReplicationMetadata result = JSONUtils.readValue(data, ReplicationMetadata.class);
        assertThat(metadata).usingRecursiveComparison().isEqualTo(result);
    }

    @Test
    public void test_ReplicationMetadata_compute() {
        ReplicationMetadata replicationMetadata;
        DirectorySubspace subspace = getClusterSubspace("replication-metadata-subspace");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            replicationMetadata = ReplicationMetadata.compute(tr, subspace, (metadata) -> {
                byte[] begin = Versionstamp.incomplete(0).getBytes();
                byte[] end = Versionstamp.incomplete(1).getBytes();
                Snapshot snapshot = new Snapshot(0, 1, begin, end);
                SnapshotJob snapshotJob = new SnapshotJob();
                snapshotJob.put(snapshot.getSegmentId(), snapshot);
                metadata.setSnapshotJob(snapshotJob);
            });
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationMetadata.compute(tr, subspace, (metadata) -> assertThat(replicationMetadata).usingRecursiveComparison().isEqualTo(metadata));
        }
    }

    @Test
    public void test_ReplicationMetadata_load() {
        ReplicationMetadata replicationMetadata;
        DirectorySubspace subspace = getClusterSubspace("replication-metadata-subspace");
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            replicationMetadata = ReplicationMetadata.compute(tr, subspace, (metadata) -> {
                byte[] begin = Versionstamp.incomplete(0).getBytes();
                byte[] end = Versionstamp.incomplete(1).getBytes();
                SnapshotJob snapshotJob = new SnapshotJob();
                Snapshot snapshot = new Snapshot(0, 1, begin, end);
                snapshotJob.put(snapshot.getSegmentId(), snapshot);
                metadata.setSnapshotJob(snapshotJob);
            });
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationMetadata result = ReplicationMetadata.load(tr, subspace);
            assertThat(replicationMetadata).usingRecursiveComparison().isEqualTo(result);
        }
    }

    @Test
    public void test_ReplicationMetadata_SnapshotCompleted() {
        ReplicationMetadata metadata = new ReplicationMetadata();

        metadata.setSnapshotCompleted(true);

        byte[] data = JSONUtils.writeValueAsBytes(metadata);
        ReplicationMetadata result = JSONUtils.readValue(data, ReplicationMetadata.class);
        assertTrue(result.isSnapshotCompleted());
        assertThat(metadata).usingRecursiveComparison().isEqualTo(result);
    }
}