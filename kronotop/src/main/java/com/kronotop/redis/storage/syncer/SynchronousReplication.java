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

package com.kronotop.redis.storage.syncer;

import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.client.StatefulInternalConnection;
import com.kronotop.cluster.client.protocol.PackedEntry;
import com.kronotop.common.KronotopException;
import com.kronotop.server.Response;
import com.kronotop.volume.AppendResult;
import com.kronotop.volume.EntryMetadata;
import com.kronotop.volume.VolumeConfig;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SynchronousReplication {
    private final Context context;
    private final VolumeConfig volumeConfig;
    private final Set<Member> members;
    private final AppendResult appendResult;
    private final List<ByteBuffer> entries;

    public SynchronousReplication(Context context, VolumeConfig volumeConfig, Set<Member> members, List<ByteBuffer> entries, AppendResult appendResult) {
        this.context = context;
        this.volumeConfig = volumeConfig;
        this.members = members;
        this.entries = entries;
        this.appendResult = appendResult;
    }

    private HashMap<String, AtomicInteger> getSegmentIndexCounter(Set<String> segments) {
        HashMap<String, AtomicInteger> result = new HashMap<>();
        for (String segment : segments) {
            result.computeIfAbsent(segment, (k) -> new AtomicInteger());
        }
        return result;
    }

    /**
     * Allocates and organizes packed entries by their corresponding segments from the provided append result.
     * The method first calculates the number of entries per segment and then initializes an array
     * of packed entries for each segment.
     *
     * @param result the append result containing entry metadata from which the segments are derived
     * @return a HashMap where each key is a segment name, and the corresponding value is an array
     * of packed entries allocated for that segment
     */
    private HashMap<String, PackedEntry[]> allocateEntriesBySegment(AppendResult result) {
        HashMap<String, Integer> capacityBySegment = new HashMap<>();
        for (EntryMetadata entryMetadata : result.getEntryMetadataList()) {
            capacityBySegment.compute(entryMetadata.segment(), (k, number) -> {
                if (number == null) {
                    number = 0;
                }
                return ++number;
            });
        }

        HashMap<String, PackedEntry[]> entriesBySegment = new HashMap<>();
        for (String segment : capacityBySegment.keySet()) {
            entriesBySegment.computeIfAbsent(segment, (k) -> new PackedEntry[capacityBySegment.get(segment)]);
        }
        return entriesBySegment;
    }

    public boolean run() {
        HashMap<String, PackedEntry[]> entriesBySegment = allocateEntriesBySegment(appendResult);
        HashMap<String, AtomicInteger> segmentIndexCounter = getSegmentIndexCounter(entriesBySegment.keySet());
        for (int index = 0; index < appendResult.getEntryMetadataList().length; index++) {
            EntryMetadata entryMetadata = appendResult.getEntryMetadataList()[index];
            PackedEntry[] packedEntries = entriesBySegment.get(entryMetadata.segment());
            ByteBuffer buffer = entries.get(index);
            buffer.flip();

            int segmentIndex = segmentIndexCounter.get(entryMetadata.segment()).getAndIncrement();
            packedEntries[segmentIndex] = new PackedEntry(entryMetadata.position(), buffer.array());
        }

        CountDownLatch latch = new CountDownLatch(members.size());
        for (Member standby : members) {
            SyncReplicationRunnable syncReplication = new SyncReplicationRunnable(latch, standby, entriesBySegment);
            Thread.ofVirtual().name("sync-replication-virtual-thread", 0).start(syncReplication);
        }
        try {
            return latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new KronotopException(e);
        }
    }

    /**
     * SyncReplicationRunnable is responsible for synchronously replicating a set of packed entries
     * across segments to a specified member.
     * <p>
     * The replication process involves:
     * - Establishing or retrieving an internal connection to the specified member.
     * - Iterating over the entries organized by segments and inserting them into the member's
     * corresponding segment using the internal connection.
     * - Handling unsuccessful operations by throwing a KronotopException.
     * - Counting down a provided CountDownLatch upon successful completion to signal other
     * waiting threads.
     * <p>
     * This class is typically utilized in systems where data synchronization is crucial,
     * ensuring consistent replication across different nodes or members.
     */
    class SyncReplicationRunnable implements Runnable {
        private final CountDownLatch latch;
        private final Member member;
        private final HashMap<String, PackedEntry[]> entriesBySegment;

        SyncReplicationRunnable(CountDownLatch latch, Member member, HashMap<String, PackedEntry[]> entriesBySegment) {
            this.latch = latch;
            this.member = member;
            this.entriesBySegment = entriesBySegment;
        }

        @Override
        public void run() {
            StatefulInternalConnection<byte[], byte[]> connection = context.getInternalConnectionPool().get(member);
            entriesBySegment.forEach((segment, packedEntries) -> {
                String status = connection.sync().segmentinsert(volumeConfig.name(), segment, packedEntries);
                if (!status.equals(Response.OK)) {
                    throw new KronotopException("Failed to replicate entries synchronously to " + member + " : " + status);
                }
            });
            latch.countDown();
        }
    }
}
