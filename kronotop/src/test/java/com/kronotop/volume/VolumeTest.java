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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseMetadataStoreTest;
import com.kronotop.common.utils.DirectoryLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class VolumeTest extends BaseMetadataStoreTest {
    VolumeService service;
    Volume volume;

    private DirectorySubspace getDirectorySubspace() {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).add("volumes-test").add(UUID.randomUUID().toString()).asList();
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }

    @BeforeEach
    public void setupVolumeTestEnvironment() {
        service = new VolumeService(context);
        VolumeConfig volumeConfig = new VolumeConfig(getDirectorySubspace(), "append-test");
        volume = service.newVolume(volumeConfig);
    }

    @AfterEach
    public void tearDownVolumeTest() {
        volume.close();
        service.shutdown();
    }

    @Test
    public void append() throws IOException {
        ByteBuffer[] entries = {
                ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
        };
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            result = volume.append(tr, entries);
            tr.commit().join();
        }
        assertEquals(2, result.getVersionstampedKeys().length);
    }

    @Test
    public void get() throws SegmentNotFoundException, IOException {
        ByteBuffer[] entries = {
                ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
        };
        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            result = volume.append(tr, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampedKeys = result.getVersionstampedKeys();
        List<ByteBuffer> retrievedEntries = new ArrayList<>();
        try (Transaction tr = database.createTransaction()) {
            for (Versionstamp versionstamp: versionstampedKeys) {
                ByteBuffer buffer = volume.get(tr, versionstamp);
                retrievedEntries.add(buffer);
            }
        }
        for(int i = 0; i < retrievedEntries.size(); i++) {
            assertArrayEquals(entries[i].array(), retrievedEntries.get(i).array());
        }
    }

    @Test
    public void update() throws IOException, SegmentNotFoundException, KeyNotFoundException {
        VolumeService service = new VolumeService(context);
        DirectorySubspace subspace = getDirectorySubspace();
        VolumeConfig volumeConfig = new VolumeConfig(subspace, "append-test");
        Volume volume = service.newVolume(volumeConfig);

        ByteBuffer[] entries = {
                ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
        };

        AppendResult result;
        try (Transaction tr = database.createTransaction()) {
            result = volume.append(tr, entries);
            tr.commit().join();
        }

        Versionstamp[] versionstampList = result.getVersionstampedKeys();
        for (Versionstamp versionstamp : versionstampList) {
            ByteBuffer buffer = volume.get(versionstamp);
            System.out.println(new String(buffer.array()));
        }

        {
            KeyEntry[] e = new KeyEntry[2];
            int i = 0;
            for (Versionstamp key : versionstampList) {
                e[i] = new KeyEntry(key, ByteBuffer.allocate(9).put(String.format("updated-%d", i).getBytes()).flip());
                i++;
            }
            UpdateResult result2;
            try (Transaction tr = database.createTransaction()) {
                result2 = volume.update(tr, e);
                tr.commit().join();
            }
            result2.complete();

            for (Versionstamp versionstamp : versionstampList) {
                ByteBuffer buffer = volume.get(versionstamp);
                System.out.println(new String(buffer.array()));
            }
        }
    }

    @Test
    public void reopen() throws IOException, SegmentNotFoundException {
        VolumeService service = new VolumeService(context);
        DirectorySubspace subspace = getDirectorySubspace();
        VolumeConfig volumeConfig = new VolumeConfig(subspace, "append-test");

        {
            Volume volume = service.newVolume(volumeConfig);
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("foobar".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("barfoo".getBytes()).flip(),
            };

            AppendResult result;
            try (Transaction tr = database.createTransaction()) {
                result = volume.append(tr, entries);
                tr.commit().join();
            }

            Versionstamp[] versionstampList = result.getVersionstampedKeys();
            for (Versionstamp versionstamp : versionstampList) {
                ByteBuffer buffer = volume.get(versionstamp);
                System.out.println(new String(buffer.array()));
            }
            volume.close();
        }

        {
            Volume volume = service.newVolume(volumeConfig);
            ByteBuffer[] entries = {
                    ByteBuffer.allocate(6).put("FOOBAR".getBytes()).flip(),
                    ByteBuffer.allocate(6).put("BARFOO".getBytes()).flip(),
            };

            AppendResult result;
            try (Transaction tr = database.createTransaction()) {
                result = volume.append(tr, entries);
                tr.commit().join();
            }

            Versionstamp[] versionstampList = result.getVersionstampedKeys();
            volume.close();
        }
    }
}