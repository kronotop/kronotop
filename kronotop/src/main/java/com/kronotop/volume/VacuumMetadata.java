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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.JSONUtils;

public class VacuumMetadata {
    private String taskName;
    private long readVersion;
    private double allowedGarbageRatio;

    VacuumMetadata() {
    }

    public VacuumMetadata(String volumeName, long readVersion, double allowedGarbageRatio) {
        this.taskName = VacuumTaskName(volumeName);
        this.readVersion = readVersion;
        this.allowedGarbageRatio = allowedGarbageRatio;
    }

    public static String VacuumTaskName(String volumeName) {
        return "vacuum:" + volumeName;
    }

    static byte[] getMetadataKey(DirectorySubspace volumeSubspace) {
        return volumeSubspace.pack(
                Tuple.from(
                        VolumeSubspaceConstants.VACUUM_SUBSPACE,
                        VolumeSubspaceConstants.VACUUM_METADATA_KEY
                )
        );
    }

    public static void remove(Transaction tr, DirectorySubspace volumeSubspace) {
        byte[] metadataKey = getMetadataKey(volumeSubspace);
        tr.clear(metadataKey);
    }

    public static VacuumMetadata load(Transaction tr, DirectorySubspace volumeSubspace) {
        byte[] metadataKey = getMetadataKey(volumeSubspace);
        byte[] value = tr.get(metadataKey).join();
        if (value == null) return null;
        return JSONUtils.readValue(value, VacuumMetadata.class);
    }

    public String getTaskName() {
        return taskName;
    }

    public double getAllowedGarbageRatio() {
        return allowedGarbageRatio;
    }

    public long getReadVersion() {
        return readVersion;
    }

    public void save(Transaction tr, DirectorySubspace volumeSubspace) {
        byte[] metadataKey = getMetadataKey(volumeSubspace);
        tr.set(metadataKey, JSONUtils.writeValueAsBytes(this));
    }
}
