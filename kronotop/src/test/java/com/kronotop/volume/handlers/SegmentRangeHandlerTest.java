package com.kronotop.volume.handlers;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.volume.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SegmentRangeHandlerTest extends BaseVolumeTest {
    String volumeName = "test_volume";
    Database database;
    VolumeService service;
    DirectorySubspace directorySubspace;
    Volume volume;
    VolumeConfig volumeConfig;

    @BeforeEach
    public void setupVolumeTestEnvironment() throws IOException {
        database = kronotopInstance.getContext().getFoundationDB();
        service = kronotopInstance.getContext().getService(VolumeService.NAME);
        directorySubspace = getDirectorySubspace();
        volumeConfig = new VolumeConfig(directorySubspace, volumeName);
        volume = service.newVolume(volumeConfig);
    }

    @Test
    public void test_SEGMENTRANGE() throws IOException {
        ByteBuffer[] entries = {
                ByteBuffer.wrap(new byte[] {1, 2, 3}),
                ByteBuffer.wrap(new byte[] {4, 5, 6}),
        };
        try (Transaction tr = database.createTransaction()) {
            Session session = new Session(tr);
            volume.append(session, entries);
            tr.commit().join();
        }

    }
}