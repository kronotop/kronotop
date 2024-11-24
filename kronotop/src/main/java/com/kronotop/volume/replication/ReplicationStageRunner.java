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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.cluster.client.protocol.SegmentRange;
import com.kronotop.volume.NotEnoughSpaceException;
import com.kronotop.volume.OperationKind;
import com.kronotop.volume.VersionstampedKeySelector;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.segment.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Class responsible for managing and executing replication stages for segments.
 */
public class ReplicationStageRunner {
    protected static final int MAXIMUM_BATCH_SIZE = 100;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationStageRunner.class);
    protected final Context context;
    protected final ReplicationConfig config;
    protected final VolumeConfig volumeConfig;
    protected final ReplicationClient client;
    protected final HashMap<Long, Segment> openSegments = new HashMap<>();
    protected Versionstamp slotId;
    private volatile boolean stopped = false;

    public ReplicationStageRunner(Context context, ReplicationContext replicationContext) {
        this.context = context;
        this.config = replicationContext.config();
        this.volumeConfig = replicationContext.volumeConfig();
        this.client = replicationContext.client();
        this.slotId = replicationContext.slotId();
    }

    protected ReplicationSlot loadReplicationSlot() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            return loadReplicationSlot(tr);
        }
    }

    protected ReplicationSlot loadReplicationSlot(Transaction tr) {
        return ReplicationSlot.load(tr, config, slotId);
    }

    /**
     * Iterates over log entries for a specified segment, fetching data ranges and inserting them into the segment.
     *
     * @param tr The transaction context within which the iteration occurs.
     * @param segment The target segment for processing log entries and data ranges.
     * @param begin The starting key selector for the iteration.
     * @param end The ending key selector for the iteration.
     * @param limit The maximum number of log entries to process in one iteration.
     * @return An IterationResult containing the latest processed key and the number of processed keys.
     * @throws NotEnoughSpaceException If there is insufficient space to process the entries.
     * @throws IOException If an I/O error occurs during processing.
     */
    protected IterationResult iterate(Transaction tr, Segment segment, VersionstampedKeySelector begin, VersionstampedKeySelector end, int limit) throws NotEnoughSpaceException, IOException {
        SegmentLogIterable iterable = new SegmentLogIterable(tr, volumeConfig.subspace(), segment.getName(), begin, end, limit);
        List<SegmentLogEntry> segmentLogEntries = new ArrayList<>();
        for (SegmentLogEntry entry : iterable) {
            segmentLogEntries.add(entry);
        }

        if (segmentLogEntries.isEmpty()) {
            return new IterationResult(null, 0);
        }

        List<Object> dataRanges = fetchSegmentRange(segment.getName(), segmentLogEntries);
        insertSegmentRange(segment, segmentLogEntries, dataRanges);
        return new IterationResult(segmentLogEntries.getLast().key(), segmentLogEntries.size());
    }

    /**
     * Inserts a range of data into the specified segment based on the given log entries.
     *
     * @param segment The segment into which the data will be inserted.
     * @param entries The list of log entries that describe how the data should be inserted.
     * @param dataRange The list of data objects to be inserted into the segment.
     * @throws IOException If an I/O error occurs during the insert operation.
     * @throws NotEnoughSpaceException If there is not enough space in the segment to insert the data.
     */
    protected void insertSegmentRange(Segment segment, List<SegmentLogEntry> entries, List<Object> dataRange) throws IOException, NotEnoughSpaceException {
        for (int i = 0; i < dataRange.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            byte[] data = (byte[]) dataRange.get(i);
            segment.insert(ByteBuffer.wrap(data), entry.value().position());
        }
        segment.flush(true);
    }

    /**
     * Fetches the data ranges for a specified segment from the log entries and returns them as a list of objects.
     * Skips entries marked as DELETE since they are meant for the vacuuming process.
     *
     * @param segmentName the name of the segment for which data ranges need to be fetched
     * @param entries the list of log entries from which data ranges are to be derived
     * @return a list of objects representing the fetched data ranges for the specified segment
     */
    protected List<Object> fetchSegmentRange(String segmentName, List<SegmentLogEntry> entries) {
        SegmentRange[] segmentRanges = new SegmentRange[entries.size()];
        for (int i = 0; i < entries.size(); i++) {
            SegmentLogEntry entry = entries.get(i);
            if (entry.value().kind().equals(OperationKind.DELETE)) {
                // Do not need to fetch the deleted entry, OperationKind.Delete should be
                // used for the vacuuming process.
                continue;
            }
            segmentRanges[i] = new SegmentRange(entry.value().position(), entry.value().length());
        }
        return client.connection().sync().segmentRange(volumeConfig.name(), segmentName, segmentRanges);
    }

    public void stop() {
        if (stopped) {
            return;
        }

        stopped = true;

        for (Segment segment : openSegments.values()) {
            try {
                segment.close();
            } catch (Exception e) {
                LOGGER.atError().
                        setMessage("Error while closing a segment, slotId = {}").
                        addArgument(ReplicationMetadata.stringifySlotId(slotId)).
                        setCause(e).
                        log();
            }
        }
    }

    protected boolean isStopped() {
        return stopped;
    }

    protected record IterationResult(Versionstamp latestKey, int processedKeys) {
    }
}
