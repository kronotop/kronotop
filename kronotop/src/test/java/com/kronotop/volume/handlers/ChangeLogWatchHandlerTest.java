/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.volume.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.client.protocol.InternalCommandBuilder;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.volume.BaseNetworkedVolumeIntegrationTest;
import com.kronotop.volume.VolumeSession;
import com.kronotop.volume.changelog.ChangeLog;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class ChangeLogWatchHandlerTest extends BaseNetworkedVolumeIntegrationTest {

    @Test
    void shouldReturnErrorWhenClientSequenceNumberGreaterThanVolumeSequenceNumber() {
        // Client sends sequence number greater than volume's latest
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.changelogWatch(volumeConfig.name(), 100).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Sequence number cannot be greater than the volume's latest sequence number", errorMessage.content());
    }

    @Test
    void shouldReturnLatestSequenceNumberWhenHigherThanClientValue() throws IOException {
        // Append entries to volume to create changelog entries
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Get the actual latest sequence number
        long latestSequenceNumber;
        try (Transaction tr = database.createTransaction()) {
            latestSequenceNumber = ChangeLog.getLatestSequenceNumber(tr, volumeService.openSubspace(volumeConfig.name()));
        }

        // Call CHANGELOG.WATCH with sequence number 0 (lower than current)
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.changelogWatch(volumeConfig.name(), 0).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(IntegerRedisMessage.class, msg);
        IntegerRedisMessage integerMessage = (IntegerRedisMessage) msg;
        assertEquals(latestSequenceNumber, integerMessage.value());
    }

    @Test
    void shouldWaitForMutationAndReturnNewSequenceNumber() throws Exception {
        // First, get the initial sequence number (should be 0 for empty volume)
        long initialSequenceNumber;
        try (Transaction tr = database.createTransaction()) {
            initialSequenceNumber = ChangeLog.getLatestSequenceNumber(tr, volumeService.openSubspace(volumeConfig.name()));
        }
        assertEquals(0, initialSequenceNumber);

        // Start the watch in a separate thread
        CompletableFuture<Object> watchFuture = CompletableFuture.supplyAsync(() -> {
            InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.changelogWatch(volumeConfig.name(), 0).encode(buf);
            return runCommand(channel, buf);
        });

        // Check and wait a bit to ensure the watch is registered
        await().atMost(Duration.ofSeconds(5)).until(() -> volumeService.mutationWatcher().hasWatcher(volume.getId()));

        // Append entries to trigger mutation
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(3);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Get the expected sequence number after mutation
        long expectedSequenceNumber;
        try (Transaction tr = database.createTransaction()) {
            expectedSequenceNumber = ChangeLog.getLatestSequenceNumber(tr, volumeService.openSubspace(volumeConfig.name()));
        }

        // Wait for the watch to complete
        Object msg = watchFuture.get(5, TimeUnit.SECONDS);

        assertInstanceOf(IntegerRedisMessage.class, msg);
        IntegerRedisMessage integerMessage = (IntegerRedisMessage) msg;
        assertEquals(expectedSequenceNumber, integerMessage.value());
    }

    @Test
    void shouldReturnSafeWatermarkNotLatestWhenInFlightExists() throws IOException {
        // Behavior: CHANGELOG.WATCH returns the safe watermark (not the latest sequence number)
        // when in-flight transactions exist, preventing CDC from reading uncommitted data.

        // Step 1: Append 5 entries and commit to establish a baseline
        ByteBuffer[] entries = baseVolumeTestWrapper.getEntries(5);
        try (Transaction tr = database.createTransaction()) {
            VolumeSession session = new VolumeSession(tr, prefix);
            volume.append(session, entries);
            tr.commit().join();
        }

        // Step 2: Wait for watermark to settle and record baseline
        ChangeLog changeLog = volume.getChangeLog();
        long baselineSeq;
        try (Transaction tr = database.createTransaction()) {
            baselineSeq = ChangeLog.getLatestSequenceNumber(tr, volumeService.openSubspace(volumeConfig.name()));
        }
        assertTrue(baselineSeq > 0);

        // Wait for the watermark to catch up to the committed baseline
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (changeLog.getSafeWatermark() < baselineSeq && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        assertEquals(baselineSeq, changeLog.getSafeWatermark());

        // Step 3: Open a new transaction and append without committing (creates in-flight entry)
        ByteBuffer[] moreEntries = baseVolumeTestWrapper.getEntries(1);
        Transaction danglingTr = database.createTransaction();
        try {
            VolumeSession session = new VolumeSession(danglingTr, prefix);
            volume.append(session, moreEntries);

            // Step 4: Send CHANGELOG.WATCH with sequence number 0
            InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
            ByteBuf buf = Unpooled.buffer();
            cmd.changelogWatch(volumeConfig.name(), 0).encode(buf);

            Object msg = runCommand(channel, buf);

            // Step 5: Assert returned value equals baselineSeq, not the latest sequence number
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage integerMessage = (IntegerRedisMessage) msg;
            assertEquals(baselineSeq, integerMessage.value());
        } finally {
            // Step 6: Clean up the dangling transaction
            danglingTr.cancel();
            danglingTr.close();
        }
    }

    @Test
    void shouldReturnErrorWhenVolumeNotOpen() {
        InternalCommandBuilder<String, String> cmd = new InternalCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.changelogWatch("nonexistent-volume", 0).encode(buf);

        Object msg = runCommand(channel, buf);

        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) msg;
        assertEquals("ERR Volume: 'nonexistent-volume' is not open", errorMessage.content());
    }
}
