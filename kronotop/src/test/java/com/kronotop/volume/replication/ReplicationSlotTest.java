/*
 * Copyright (c) 2023-2025 Kronotop
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
import com.kronotop.volume.BaseVolumeIntegrationTest;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;

class ReplicationSlotTest extends BaseVolumeIntegrationTest {

    private ReplicationConfig getReplicationConfig() {
        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.REDIS, 1);
        VolumeConfig volumeConfig = generator.volumeConfig();
        return new ReplicationConfig(volumeConfig, ShardKind.REDIS, 1, ReplicationStage.SNAPSHOT);
    }

    @Test
    public void test_newSlot() {
        ReplicationConfig config = getReplicationConfig();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> ReplicationSlot.newSlot(tr, config));
            tr.commit().join();
        }
    }

    @Test
    public void test_load() {
        Versionstamp slotId;
        ReplicationConfig config = getReplicationConfig();
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> ReplicationSlot.newSlot(tr, config));
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();
            slotId = Versionstamp.complete(future.join());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot slot = assertDoesNotThrow(() -> ReplicationSlot.load(tr, config, slotId));
            assertNotNull(slot);
        }
    }

    @Test
    public void test_compute() {
        Versionstamp slotId;
        ReplicationConfig config = getReplicationConfig();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> ReplicationSlot.newSlot(tr, config));
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();
            slotId = Versionstamp.complete(future.join());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot.compute(tr, config, slotId, (slot) -> {
                slot.setLatestSegmentId(100);
                slot.setReplicationStage(ReplicationStage.STREAMING);
                slot.completeReplicationStage(ReplicationStage.SNAPSHOT);
                slot.setActive(true);
            });
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot slot = assertDoesNotThrow(() -> ReplicationSlot.load(tr, config, slotId));
            assertNotNull(slot);
            assertEquals(ReplicationStage.STREAMING, slot.getReplicationStage());
            assertTrue(slot.isReplicationStageCompleted(ReplicationStage.SNAPSHOT));
            assertEquals(1, slot.getCompletedStages().size());
            assertEquals(100, slot.getLatestSegmentId());
            assertTrue(slot.isActive());
        }
    }

    @Test
    public void test_setStale_while_slot_is_active() {
        Versionstamp slotId;
        ReplicationConfig config = getReplicationConfig();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> ReplicationSlot.newSlot(tr, config));
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();
            slotId = Versionstamp.complete(future.join());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertThrows(IllegalStateException.class, () -> ReplicationSlot.compute(tr, config, slotId, (slot) -> {
                slot.setActive(true);
                slot.setStale();
            }));
        }
    }

    @Test
    public void test_setStale_while_slot_is_not_active() {
        Versionstamp slotId;
        ReplicationConfig config = getReplicationConfig();

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> ReplicationSlot.newSlot(tr, config));
            CompletableFuture<byte[]> future = tr.getVersionstamp();
            tr.commit().join();
            slotId = Versionstamp.complete(future.join());
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            assertDoesNotThrow(() -> ReplicationSlot.compute(tr, config, slotId, (slot) -> {
                slot.setActive(false);
                slot.setStale();
            }));
            tr.commit().join();
        }

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            ReplicationSlot slot = assertDoesNotThrow(() -> ReplicationSlot.load(tr, config, slotId));
            assertNotNull(slot);
            assertFalse(slot.isActive());
            assertTrue(slot.isStale());
        }
    }
}