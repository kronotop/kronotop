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
import com.kronotop.Context;
import com.kronotop.KronotopTestInstance;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commandbuilder.kronotop.KrAdminCommandBuilder;
import com.kronotop.commandbuilder.kronotop.VolumeAdminCommandBuilder;
import com.kronotop.server.resp3.*;
import com.kronotop.volume.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ReplicationIntegrationTest extends BaseNetworkedVolumeIntegrationTest {
    @TempDir
    private Path standbyVolumeDataDir;

    private ReplicationConfig config;
    private Versionstamp slotId;

    static <T> T[] concatWithArrayCopy(T[] array1, T[] array2) {
        T[] result = Arrays.copyOf(array1, array1.length + array2.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
    }

    private Volume standbyVolume() throws IOException {
        VolumeConfig standbyVolumeConfig = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                standbyVolumeDataDir.toString(),
                volume.getConfig().segmentSize()
        );
        return new Volume(context, standbyVolumeConfig);
    }

    private Replication newReplication(Context instanceContext, VolumeConfig standbyVolumeConfig) {
        config = new ReplicationConfig(standbyVolumeConfig, ShardKind.REDIS, 1, ReplicationStage.SNAPSHOT);
        slotId = ReplicationMetadata.newReplication(instanceContext, config);
        return new Replication(instanceContext, slotId, config);
    }

    private boolean checkAppendedEntries(Versionstamp[] versionstampedKeys, Volume standbyVolume) throws IOException {
        Session session = new Session(prefix);
        for (Versionstamp versionstampedKey : versionstampedKeys) {
            try {
                ByteBuffer buf = volume.get(session, versionstampedKey);
                if (buf == null) {
                    return false;
                }
                ByteBuffer replicaBuf = standbyVolume.get(session, versionstampedKey);
                if (!Arrays.equals(buf.array(), replicaBuf.array())) {
                    return false;
                }
            } catch (SegmentNotFoundException e) {
                return false;
            }
        }
        return true;
    }

    private Versionstamp[] appendEntries(Volume instance, int number, Versionstamp[] versionstampedKeys) throws IOException {
        AppendResult result;
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(number);
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr, prefix);
            result = instance.append(session, entries);
            tr.commit().join();
        }
        Versionstamp[] keys = result.getVersionstampedKeys();
        assertEquals(number, keys.length);
        return concatWithArrayCopy(versionstampedKeys, keys);
    }

    private IntegrationTest createIntegrationTest() throws IOException {
        Versionstamp[] versionstampedKeys = new Versionstamp[0];

        // Insert some keys to the primary volume
        versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);

        // Start a standby
        KronotopTestInstance standbyInstance = addNewInstance();
        VolumeConfig standbyVolumeConfig = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                standbyVolumeDataDir.toString(),
                volume.getConfig().segmentSize()
        );

        VolumeService volumeService = standbyInstance.getContext().getService(VolumeService.NAME);
        Volume standbyVolume = volumeService.newVolume(standbyVolumeConfig);
        Replication replication = newReplication(standbyInstance.getContext(), standbyVolume.getConfig());

        replication.start();

        await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
        Versionstamp[] keysAfterSnapshot = versionstampedKeys;
        await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(keysAfterSnapshot, standbyVolume));


        // Replication is running at the background.
        versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);
        Versionstamp[] keysWhileStreaming = versionstampedKeys;
        await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(keysWhileStreaming, standbyVolume));

        return new IntegrationTest(kronotopInstance, replication, new ArrayList<>(List.of(standbyInstance)));
    }

    @Test
    public void take_snapshot_then_start_streaming_changes() throws IOException {
        Versionstamp[] versionstampedKeys = new Versionstamp[0];

        // Insert some keys to the primary volume
        versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);

        // Start a standby
        Volume standbyVolume = standbyVolume();
        Replication replication = newReplication(context, standbyVolume.getConfig());
        try {
            replication.start();

            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
            {
                Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
            }

            // Replication is running at the background.
            {
                versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);
                Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
            }
        } finally {
            replication.stop();
        }
    }

    @Test
    public void take_snapshot_then_start_streaming_concurrent_changes() throws IOException {
        Versionstamp[] versionstampedKeys = new Versionstamp[0];

        // Insert some keys to the primary volume
        versionstampedKeys = appendEntries(volume, 10, versionstampedKeys);

        // Start a standby
        Volume standbyVolume = standbyVolume();
        Replication replication = newReplication(context, standbyVolume.getConfig());

        try {
            replication.start();

            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);

            {
                Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
            }

            List<Versionstamp> data = new ArrayList<>(List.of(versionstampedKeys));

            CountDownLatch latch = new CountDownLatch(10);
            ReadWriteLock lock = new ReentrantReadWriteLock();

            for (int i = 0; i < 10; i++) {
                Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                Thread.ofVirtual().name(String.format("volume-replication-thread-%d", i)).factory().newThread(() -> {
                    try {
                        Versionstamp[] keys = appendEntries(volume, 10, finalVersionstampedKeys);
                        try {
                            lock.writeLock().lock();
                            data.addAll(Arrays.asList(keys));
                        } finally {
                            lock.writeLock().unlock();
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } finally {
                        latch.countDown();
                    }
                }).start();
            }
            latch.await();

            Versionstamp[] finalResult = data.toArray(new Versionstamp[0]);
            await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalResult, standbyVolume));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            replication.stop();
        }
    }

    @Test
    public void restart_replication() throws IOException {
        Versionstamp[] versionstampedKeys = new Versionstamp[0];

        // Insert some keys to the primary volume
        versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);

        // Start a standby
        Volume standbyVolume = standbyVolume();

        {
            Replication replication = newReplication(context, standbyVolume.getConfig());
            try {
                replication.start();

                await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
                {
                    Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                    await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
                }

                {
                    versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);
                    Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                    await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
                }
            } finally {
                replication.stop();
            }
        }

        // Insert some entries to the primary volume and restart the replication.
        {
            versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);
            Versionstamp[] finalVersionstampedKeys = versionstampedKeys;

            ReplicationConfig config = new ReplicationConfig(standbyVolume.getConfig(), ShardKind.REDIS, 1, ReplicationStage.SNAPSHOT);
            Versionstamp slotId = ReplicationMetadata.findSlotId(context, config);
            Replication replication = new Replication(context, slotId, config);
            try {
                replication.start();
                {
                    // Check all keys from the beginning
                    await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
                }
            } finally {
                replication.stop();
            }
        }
    }

    private boolean isActive() {
        try (Transaction tr = database.createTransaction()) {
            return ReplicationSlot.load(tr, config, slotId).isActive();
        }
    }

    @Test
    public void first_start_replication_then_stop() throws IOException {
        Volume standbyVolume = standbyVolume();
        Replication replication = newReplication(context, standbyVolume.getConfig());

        replication.start();
        await().atMost(Duration.ofSeconds(5)).until(this::isActive);

        replication.stop();
        await().atMost(Duration.ofSeconds(5)).until(() -> !isActive());
    }

    @Test
    public void update_entries_while_running_streaming_replication() throws IOException, KeyNotFoundException {
        Versionstamp[] versionstampedKeys = new Versionstamp[0];

        // Insert some keys to the primary volume
        versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);

        KronotopTestInstance secondInstance = addNewInstance();

        VolumeConfig standbyVolumeConfig = new VolumeConfig(
                volume.getConfig().subspace(),
                volume.getConfig().name(),
                standbyVolumeDataDir.toString(),
                volume.getConfig().segmentSize()
        );

        // Start a standby
        VolumeService volumeService = secondInstance.getContext().getService(VolumeService.NAME);
        Volume standbyVolume = volumeService.newVolume(standbyVolumeConfig);

        Replication replication = newReplication(secondInstance.getContext(), standbyVolume.getConfig());
        try {
            replication.start();

            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
            {
                Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
            }

            // Replication is running at the background.
            KeyEntry[] entries = new KeyEntry[versionstampedKeys.length];
            try (Transaction tr = database.createTransaction()) {
                Session session = new Session(tr, prefix);
                for (int i = 0; i < versionstampedKeys.length; i++) {
                    Versionstamp key = versionstampedKeys[i];
                    entries[i] = new KeyEntry(key, ByteBuffer.wrap(String.format("new-entry-%d", i).getBytes()));
                }
                UpdateResult updateResult = volume.update(session, entries);
                tr.commit().join();
                updateResult.complete();
            }

            {
                Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
                await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));
            }
        } finally {
            replication.stop();
        }
    }

    @Test
    public void primary_ownership_change_but_shard_status_inappropriate() throws IOException {
        IntegrationTest test = createIntegrationTest();
        try {
            KronotopTestInstance standby = test.standbys.getFirst();
            KrAdminCommandBuilder<String, String> cmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.route("SET", "PRIMARY", "REDIS", 1, standby.getMember().getId()).encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ErrorRedisMessage.class, msg);
            ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
            assertEquals("ERR Shard status must not be READWRITE", actualMessage.content());
        } finally {
            test.stop();
        }
    }

    @Test
    public void start_replication_then_check_replication_slots() throws IOException {
        Versionstamp[] versionstampedKeys = new Versionstamp[0];

        // Insert some keys to the primary volume
        versionstampedKeys = appendEntries(volume, 100, versionstampedKeys);

        // Start a standby
        Volume standbyVolume = standbyVolume();
        Replication replication = newReplication(context, standbyVolume.getConfig());
        try {
            replication.start();
            await().atMost(10, TimeUnit.SECONDS).until(() -> replication.getActiveStageRunner() != null);
            Versionstamp[] finalVersionstampedKeys = versionstampedKeys;
            await().atMost(10, TimeUnit.SECONDS).until(() -> checkAppendedEntries(finalVersionstampedKeys, standbyVolume));

            VolumeAdminCommandBuilder<String, String> volumeAdmin = new VolumeAdminCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            volumeAdmin.replications().encode(buf);
            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(MapRedisMessage.class, msg);

            MapRedisMessage actualMessage = (MapRedisMessage) msg;
            for (RedisMessage value : actualMessage.children().values()) {
                MapRedisMessage mapRedisMessage = (MapRedisMessage) value;
                for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
                    SimpleStringRedisMessage key = (SimpleStringRedisMessage) entry.getKey();
                    if (key.content().equals("active")) {
                        assertTrue(((BooleanRedisMessage) entry.getValue()).value());
                    }
                    if (key.content().equals("stale")) {
                        assertFalse(((BooleanRedisMessage) entry.getValue()).value());
                    }
                    if (key.content().equals("shard_kind")) {
                        assertEquals(ShardKind.REDIS.name(), ((SimpleStringRedisMessage) entry.getValue()).content());
                    }
                    if (key.content().equals("replication_stage")) {
                        assertEquals(ReplicationStage.STREAMING.name(), ((SimpleStringRedisMessage) entry.getValue()).content());
                    }
                    if (key.content().equals("completed_stages")) {
                        List<RedisMessage> rawCompletedStages = ((ArrayRedisMessage) entry.getValue()).children();
                        List<String> completedStages = new ArrayList<>();
                        for (RedisMessage rawCompletedStage : rawCompletedStages) {
                            String completedStage = ((SimpleStringRedisMessage) rawCompletedStage).content();
                            completedStages.add(completedStage);
                        }
                        assertTrue(completedStages.contains(ReplicationStage.SNAPSHOT.name()));
                    }
                    if (key.content().equals("latest_segment_id")) {
                        assertEquals(0, ((IntegerRedisMessage) entry.getValue()).value());
                    }
                    if (key.content().equals("received_versionstamped_key")) {
                        assertNotNull(((SimpleStringRedisMessage) entry.getValue()).content());
                    }
                    if (key.content().equals("latest_versionstamped_key")) {
                        assertNotNull(((SimpleStringRedisMessage) entry.getValue()).content());
                    }
                }
            }
        } finally {
            replication.stop();
        }
    }

    private record IntegrationTest(KronotopTestInstance primary, Replication replication,
                                   List<KronotopTestInstance> standbys) {
        public void stop() {
            replication.stop();
        }
    }
}
