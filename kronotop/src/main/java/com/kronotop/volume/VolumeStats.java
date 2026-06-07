/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory counters for Volume operations and byte throughput. All counters reset on restart.
 */
public class VolumeStats {
    private final AtomicLong appends = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong gets = new AtomicLong();
    private final AtomicLong bytesAppended = new AtomicLong();
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong segmentsCreated = new AtomicLong();

    /**
     * Adds the given count to the total number of append operations.
     *
     * @param count number of appends in this batch
     */
    public void addAppends(long count) {
        appends.addAndGet(count);
    }

    /**
     * Adds the given count to the total number of delete operations.
     *
     * @param count number of deletes in this batch
     */
    public void addDeletes(long count) {
        deletes.addAndGet(count);
    }

    /**
     * Adds the given count to the total number of update operations.
     *
     * @param count number of updates in this batch
     */
    public void addUpdates(long count) {
        updates.addAndGet(count);
    }

    /**
     * Increments the total number of get operations by one.
     */
    public void incrementGets() {
        gets.incrementAndGet();
    }

    /**
     * Adds the given amount to the total bytes written via append operations.
     *
     * @param bytes number of bytes appended
     */
    public void addBytesAppended(long bytes) {
        bytesAppended.addAndGet(bytes);
    }

    /**
     * Adds the given amount to the total bytes read via get operations.
     *
     * @param bytes number of bytes read
     */
    public void addBytesRead(long bytes) {
        bytesRead.addAndGet(bytes);
    }

    /**
     * Increments the total number of segments created by one.
     */
    public void incrementSegmentsCreated() {
        segmentsCreated.incrementAndGet();
    }

    /**
     * Returns the total number of append operations since the last reset.
     */
    public long getAppends() {
        return appends.get();
    }

    /**
     * Returns the total number of delete operations since the last reset.
     */
    public long getDeletes() {
        return deletes.get();
    }

    /**
     * Returns the total number of update operations since the last reset.
     */
    public long getUpdates() {
        return updates.get();
    }

    /**
     * Returns the total number of get operations since the last reset.
     */
    public long getGets() {
        return gets.get();
    }

    /**
     * Returns the total bytes written via append operations since the last reset.
     */
    public long getBytesAppended() {
        return bytesAppended.get();
    }

    /**
     * Returns the total bytes read via get operations since the last reset.
     */
    public long getBytesRead() {
        return bytesRead.get();
    }

    /**
     * Returns the total number of segments created since the last reset.
     */
    public long getSegmentsCreated() {
        return segmentsCreated.get();
    }

    /**
     * Resets all counters to zero.
     */
    public void reset() {
        appends.set(0);
        deletes.set(0);
        updates.set(0);
        gets.set(0);
        bytesAppended.set(0);
        bytesRead.set(0);
        segmentsCreated.set(0);
    }
}
