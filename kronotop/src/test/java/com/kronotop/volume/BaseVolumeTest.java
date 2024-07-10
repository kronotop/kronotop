package com.kronotop.volume;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.google.common.base.Strings;
import com.kronotop.BaseMetadataStoreTest;
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

public class BaseVolumeTest extends BaseMetadataStoreTest {

    protected VolumeConfig getVolumeConfig(Config config, DirectorySubspace subspace) {
        String name = config.getString("volume_test.volume.name");
        String rootPath = config.getString("volume_test.volume.root_path");
        Long segmentSize = config.getLong("volume_test.volume.segment_size");
        Float allowedGarbageRatio = (float) config.getDouble("volume_test.volume.allowed_garbage_ratio");
        return new VolumeConfig(subspace, name, rootPath, segmentSize, allowedGarbageRatio);
    }

    protected ByteBuffer[] getEntries(int number) {
        int capacity = 10;
        ByteBuffer[] entries = new ByteBuffer[number];
        for (int i = 0; i < number; i++) {
            byte[] data = Strings.padStart(Integer.toString(i), capacity, '0').getBytes();
            entries[i] = ByteBuffer.allocate(capacity).put(data).flip();
        }
        return entries;
    }

    protected DirectorySubspace getSubspace(Database database, Config config) {
        try (Transaction tr = database.createTransaction()) {
            String clusterName = config.getString("cluster.name");
            List<String> subpath = DirectoryLayout.Builder.clusterName(clusterName).add("volumes-test").add(UUID.randomUUID().toString()).asList();
            DirectorySubspace subspace = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
            return subspace;
        }
    }
}