/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster;

import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.directory.Shards;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.VolumeAlreadyExistsException;
import com.kronotop.volume.VolumeMetadata;
import com.kronotop.volume.VolumeMetadataMaintainer;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.VolumeStatus;
import com.kronotop.volume.VolumeSubspace;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.*;

class ShardUtilTest extends BaseStandaloneInstanceTest {

    private Shards.Shard shardDirectory(int shardId) {
        return KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata().shards().shard(shardId);
    }

    private boolean directoryExists(KronotopDirectoryNode directory) {
        return TransactionUtil.execute(context, tr -> context.getDirectoryLayer().exists(tr, directory.toList()).join());
    }

    @Test
    void shouldCreateShard() {
        // Behavior: On a cluster initialized with NUM_SHARDS shards, createShard is called for the unused id
        // UNUSED_SHARD_ID and the transaction is committed. The returned subspace is the directory of the new shard.
        // The shard is stored with the status INOPERABLE, and its index task directory, its last seen versions
        // directory and its volume directory exist. The volume metadata is stored with an ID, the status READWRITE
        // and segment 0 as the only segment.
        DirectorySubspace shardSubspace = TransactionUtil.executeThenCommit(context, tr -> ShardUtil.createShard(context, tr, UNUSED_SHARD_ID));
        assertEquals(shardDirectory(UNUSED_SHARD_ID).toList(), shardSubspace.getPath());

        ShardStatus status = TransactionUtil.execute(context, tr -> ShardUtil.getShardStatus(context, tr, UNUSED_SHARD_ID));
        assertEquals(ShardStatus.INOPERABLE, status);

        assertTrue(directoryExists(shardDirectory(UNUSED_SHARD_ID).buckets().maintenance().index().tasks()));
        assertTrue(directoryExists(shardDirectory(UNUSED_SHARD_ID).buckets().lastSeenVersions()));
        assertTrue(directoryExists(VolumeMetadataMaintainer.getShardVolumeDirectory(context, UNUSED_SHARD_ID)));

        VolumeMetadata volumeMetadata = TransactionUtil.execute(context, tr -> {
            List<String> volumePath = VolumeMetadataMaintainer.getShardVolumeDirectory(context, UNUSED_SHARD_ID).toList();
            VolumeSubspace volumeSubspace = new VolumeSubspace(context.getDirectoryLayer().open(tr, volumePath).join());
            return new VolumeMetadata(
                    VolumeMetadataMaintainer.readVolumeId(tr, volumeSubspace),
                    VolumeMetadataMaintainer.readVolumeStatus(tr, volumeSubspace),
                    VolumeMetadataMaintainer.loadSegmentIds(tr, volumeSubspace)
            );
        });
        assertNotEquals(0L, volumeMetadata.id());
        assertEquals(VolumeStatus.READWRITE, volumeMetadata.status());
        assertEquals(List.of(0L), volumeMetadata.segmentIds());
    }

    @Test
    void shouldThrowWhenShardAlreadyExists() {
        // Behavior: On a cluster initialized with NUM_SHARDS shards, createShard is called for shard 0, which
        // already exists. The call fails with a CompletionException caused by DirectoryAlreadyExistsException.
        CompletionException exception = assertThrows(CompletionException.class,
                () -> TransactionUtil.execute(context, tr -> ShardUtil.createShard(context, tr, 0)));
        assertInstanceOf(DirectoryAlreadyExistsException.class, exception.getCause());
    }

    @Test
    void shouldThrowWhenVolumeAlreadyExists() {
        // Behavior: On a cluster initialized with NUM_SHARDS shards, the volume directory of the unused id
        // UNUSED_SHARD_ID is created first, and createShard is then called for the same id in the same
        // transaction. The createShard call fails with VolumeAlreadyExistsException and the message
        // "Volume already exists: '<volume name>'".
        VolumeAlreadyExistsException exception = assertThrows(VolumeAlreadyExistsException.class,
                () -> TransactionUtil.execute(context, tr -> {
                    VolumeMetadataMaintainer.createVolumeSubspace(context, tr, UNUSED_SHARD_ID);
                    return ShardUtil.createShard(context, tr, UNUSED_SHARD_ID);
                }));
        assertEquals(String.format("Volume already exists: '%s'", VolumeNames.format(UNUSED_SHARD_ID)), exception.getMessage());
    }
}
