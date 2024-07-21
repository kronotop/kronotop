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

import java.util.TreeMap;

public class ReplicationJob {

    private final TreeMap<Long, Snapshot> snapshots = new TreeMap<>();

    private boolean snapshotCompleted;

    private long latestSegmentId;

    private byte[] latestVersionstampedKey;

    public TreeMap<Long, Snapshot> getSnapshots() {
        return snapshots;
    }

    public boolean isSnapshotCompleted() {
        return snapshotCompleted;
    }

    public void setSnapshotCompleted(boolean snapshotCompleted) {
        this.snapshotCompleted = snapshotCompleted;
    }

    public void setLatestVersionstampedKey(byte[] latestVersionstampedKey) {
        this.latestVersionstampedKey = latestVersionstampedKey;
    }

    public byte[] getLatestVersionstampedKey() {
        return latestVersionstampedKey;
    }

    public void setLatestSegmentId(long latestSegmentId) {
        this.latestSegmentId = latestSegmentId;
    }

    public long getLatestSegmentId() {
        return latestSegmentId;
    }
}

