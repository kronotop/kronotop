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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.kronotop.JSONUtils;

import java.util.*;
import java.util.function.Consumer;

import static com.kronotop.volume.Prefixes.SEGMENT_REPLICATION_PREFIX;

public class ReplicationMetadata {
    @JsonIgnore
    private static final String REPLICATION_METADATA_KEY = "replication-metadata";

    @JsonIgnore
    private static final Tuple preKey = Tuple.from(SEGMENT_REPLICATION_PREFIX, REPLICATION_METADATA_KEY);

    private final Map<String,SnapshotJob> snapshotJobs = new HashMap<>();

    public ReplicationMetadata() {
    }

    public static ReplicationMetadata load(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(preKey);
        byte[] value = tr.get(key).join();
        if (value == null) {
            return new ReplicationMetadata();
        }
        return JSONUtils.readValue(value, ReplicationMetadata.class);
    }

    public static ReplicationMetadata compute(Transaction tr, DirectorySubspace subspace, Consumer<ReplicationMetadata> remappingFunction) {
        byte[] key = subspace.pack(preKey);
        ReplicationMetadata replicationMetadata;
        byte[] value = tr.get(key).join();
        if (value == null) {
            replicationMetadata = new ReplicationMetadata();
        } else {
            replicationMetadata = JSONUtils.readValue(value, ReplicationMetadata.class);
        }
        remappingFunction.accept(replicationMetadata);
        tr.set(key, replicationMetadata.toByte());
        return replicationMetadata;
    }

    @JsonIgnore
    public SnapshotJob getSnapshotJob(String jobId) {
        return snapshotJobs.get(jobId);
    }

    @JsonIgnore
    public String setSnapshotJob(SnapshotJob job) {
        String jobId = UUID.randomUUID().toString();
        snapshotJobs.put(jobId, job);
        return jobId;
    }

    @JsonIgnore
    public void dropSnapshotJob(String jobId) {
        snapshotJobs.remove(jobId);
    }

    public Map<String, SnapshotJob> getSnapshotJobs() {
        return Collections.unmodifiableMap(snapshotJobs);
    }

    public byte[] toByte() {
        return JSONUtils.writeValueAsBytes(this);
    }
}
