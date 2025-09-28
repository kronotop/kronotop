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

package com.kronotop.volume;

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.journal.JournalName;
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
        VolumeConfigGenerator generator = new VolumeConfigGenerator(context, ShardKind.REDIS, 1);
        volumeConfig = generator.volumeConfig();
        service = context.getService(VolumeService.NAME);
    }

    @AfterEach
    public void tearDown() {
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    public void test_newVolume() throws IOException {
        Volume volume = service.newVolume(volumeConfig);
        assertNotNull(volume);
        volume.close();
    }

    @Test
    public void test_findVolume() throws IOException {
        Volume volume = service.newVolume(volumeConfig);
        assertDoesNotThrow(() -> service.findVolume(volumeConfig.name()));
        volume.close();
    }

    @Test
    public void test_findVolume_VolumeNotOpenException() {
        assertThrows(VolumeNotOpenException.class, () -> service.findVolume("foobar"));
    }

    @Test
    public void test_findVolume_ClosedVolumeException() throws IOException {
        Volume volume = service.newVolume(volumeConfig);
        volume.close();
        assertThrows(ClosedVolumeException.class, () -> service.findVolume(volumeConfig.name()));
    }

    @Test
    public void test_closeVolume() throws IOException {
        service.newVolume(volumeConfig);
        assertDoesNotThrow(() -> service.closeVolume(volumeConfig.name()));
    }

    @Test
    public void test_register_volume() throws IOException {
        Volume volume = service.newVolume(volumeConfig);
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
    public void test_DisusedPrefixesWatcher() throws IOException {
        Volume volume = service.newVolume(volumeConfig);

        ByteBuffer entry = ByteBuffer.wrap("entry".getBytes());
        try {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                VolumeSession session = new VolumeSession(tr, prefix);
                volume.append(session, entry);
                tr.commit().join();
            }
            context.getJournal().getPublisher().publish(JournalName.DISUSED_PREFIXES, prefix.asBytes());
            await().atMost(Duration.ofSeconds(5)).until(() -> {
                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    VolumeSession session = new VolumeSession(tr, prefix);
                    Iterable<KeyEntry> iterable = volume.getRange(session);
                    return !iterable.iterator().hasNext();
                }
            });
        } finally {
            volume.close();
        }
    }
}
