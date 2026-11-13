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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.ShardUtil;
import com.kronotop.journal.JournalName;
import com.kronotop.journal.JournalService;
import com.kronotop.transaction.TransactionUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

public class VolumeServiceTest extends BaseVolumeTest {
    protected VolumeService service;
    protected VolumeConfig volumeConfig;

    @BeforeEach
    public void setUp() {
        volumeConfig = ShardUtil.newShardVolumeConfig(context, 1);
        service = context.getService(VolumeService.NAME);
    }

    @AfterEach
    public void tearDown() {
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    public void shouldOpenVolume() throws IOException {
        // Behavior: When the service has no open volume for the config, openVolume returns a non-null volume.
        Volume volume = service.openVolume(volumeConfig);
        assertNotNull(volume);
        volume.close();
    }

    @Test
    public void shouldThrowWhenOpenVolumeWithoutMetadata() {
        // Behavior: The volume directory of the unused shard UNUSED_SHARD_ID is created with createVolumeSubspace
        // and committed, but no volume metadata is written. openVolume with a config that points to that directory
        // fails with NoSuchVolumeException and the message "No such volume: '<volume name>'".
        String name = VolumeNames.format(UNUSED_SHARD_ID);
        DirectorySubspace subspace = TransactionUtil.executeThenCommit(context, tr -> VolumeMetadataUtil.createVolumeSubspace(
                context,
                tr,
                name,
                ShardUtil.getShardVolumeDirectory(context, UNUSED_SHARD_ID)
        ));
        VolumeConfig config = new VolumeConfig(
                subspace,
                name,
                ShardUtil.shardDataDir(context.getDataDir(), UNUSED_SHARD_ID).toString(),
                volumeConfig.segmentSize()
        );
        NoSuchVolumeException exception = assertThrows(NoSuchVolumeException.class, () -> service.openVolume(config));
        assertEquals(String.format("No such volume: '%s'", name), exception.getMessage());
    }

    @Test
    public void shouldFindExistingVolume() throws IOException {
        Volume volume = service.openVolume(volumeConfig);
        assertDoesNotThrow(() -> service.findVolume(volumeConfig.name()));
        volume.close();
    }

    @Test
    public void shouldThrowVolumeNotOpenExceptionWhenVolumeNotFound() {
        assertThrows(VolumeNotOpenException.class, () -> service.findVolume("foobar"));
    }

    @Test
    public void shouldThrowClosedVolumeExceptionWhenVolumeClosed() throws IOException {
        Volume volume = service.openVolume(volumeConfig);
        volume.close();
        assertThrows(ClosedVolumeException.class, () -> service.findVolume(volumeConfig.name()));
    }

    @Test
    public void shouldCloseVolume() throws IOException {
        service.openVolume(volumeConfig);
        assertDoesNotThrow(() -> service.closeVolume(volumeConfig.name()));
    }

    @Test
    public void shouldRegisterVolumeInList() throws IOException {
        Volume volume = service.openVolume(volumeConfig);
        try {
            boolean found = false;
            for (Volume v : service.list()) {
                if (v.getConfig().name().equals(volumeConfig.name())) {
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        } finally {
            volume.close();
        }
    }

    @Test
    public void shouldRemoveEntriesWhenPrefixInvalidated() throws IOException {
        Volume volume = service.openVolume(volumeConfig);

        ByteBuffer entry = ByteBuffer.wrap("entry".getBytes());
        try {
            try (Transaction tr = context.getTransactionManager().create()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                volume.append(session, entry);
                tr.commit().join();
            }
            JournalService journal = context.getService(JournalService.NAME);
            journal.getPublisher().publish(JournalName.PREFIX_INVALIDATIONS, prefix.asBytes());
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                try (Transaction tr = context.getTransactionManager().create()) {
                    VolumeSession session = new VolumeSession(tr, prefix);
                    Iterable<VolumeEntry> iterable = volume.getRange(session);
                    return !iterable.iterator().hasNext();
                }
            });
        } finally {
            volume.close();
        }
    }

    @Test
    public void shouldOpenSubspaceByShardId() {
        // Behavior: openSubspace with the existing shard TEST_SHARD_ID returns a subspace whose path is the volume
        // directory of that shard. A second call with the same shard ID and a call with the volume name of that
        // shard both return the same cached instance.
        DirectorySubspace subspace = service.openSubspace(TEST_SHARD_ID);

        assertEquals(ShardUtil.getShardVolumeDirectory(context, TEST_SHARD_ID).toList(), subspace.getPath());
        assertSame(subspace, service.openSubspace(TEST_SHARD_ID));
        assertSame(subspace, service.openSubspace(VolumeNames.format(TEST_SHARD_ID)));
    }

    @Test
    public void shouldThrowWhenOpenSubspaceWithUnusedShardId() {
        // Behavior: No volume directory exists for the unused shard UNUSED_SHARD_ID. openSubspace with that shard
        // ID fails with NoSuchVolumeException and the message "No such volume: '<volume name>'".
        NoSuchVolumeException exception = assertThrows(
                NoSuchVolumeException.class,
                () -> service.openSubspace(UNUSED_SHARD_ID)
        );
        assertEquals(String.format("No such volume: '%s'", VolumeNames.format(UNUSED_SHARD_ID)), exception.getMessage());
    }

    @Test
    public void shouldThrowWhenOpenSubspaceWithInvalidVolumeName() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.openSubspace("invalid-name")
        );
        assertEquals("Invalid volume name: 'invalid-name'", exception.getMessage());
    }

    @Test
    public void shouldThrowWhenOpenSubspaceWithNoSuchVolume() {
        // Behavior: No volume directory exists for the unused shard UNUSED_SHARD_ID. openSubspace with the volume
        // name of that shard fails with NoSuchVolumeException and the message "No such volume: '<volume name>'".
        String name = VolumeNames.format(UNUSED_SHARD_ID);
        NoSuchVolumeException exception = assertThrows(
                NoSuchVolumeException.class,
                () -> service.openSubspace(name)
        );
        assertEquals(String.format("No such volume: '%s'", name), exception.getMessage());
    }
}
