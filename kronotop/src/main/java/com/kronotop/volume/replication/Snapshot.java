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

public class Snapshot {
    private byte[] begin;
    private byte[] end;
    private long totalEntries;
    private long segmentId;
    private long processedEntries;
    private long lastUpdate;

    Snapshot() {
    }

    public Snapshot(long segmentId, long totalEntries, byte[] begin, byte[] end) {
        this.segmentId = segmentId;
        this.totalEntries = totalEntries;
        this.begin = begin;
        this.end = end;
    }

    public byte[] getBegin() {
        return begin;
    }

    public void setBegin(byte[] begin) {
        this.begin = begin;
    }

    public byte[] getEnd() {
        return end;
    }

    public long getProcessedEntries() {
        return processedEntries;
    }

    public void setProcessedEntries(long processedEntries) {
        this.processedEntries = processedEntries;
    }

    public long getSegmentId() {
        return segmentId;
    }

    public long getTotalEntries() {
        return totalEntries;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
}