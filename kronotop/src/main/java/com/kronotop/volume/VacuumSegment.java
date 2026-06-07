/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.transaction.InstrumentedTransaction;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class VacuumSegment {
    private final Context context;
    private final VolumeSubspace subspace;

    public VacuumSegment(Context context, VolumeSubspace subspace) {
        this.context = context;
        this.subspace = subspace;
    }

    private void analyzeSegment(long segmentId) {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            byte[] statusKey = subspace.packVacuumSegmentMetadataFieldKey(segmentId, VacuumMetadataField.STATUS.getValue());
            byte[] statusValue = tr.get(statusKey).join();
            if (statusValue != null) {
                return;
            }

            byte[] statsPrefix = subspace.packSegmentStatsSegmentPrefix(segmentId);
            Range statsRange = Range.startsWith(statsPrefix);

            Map<Prefix, Integer> prefixCardinalities = new HashMap<>();
            for (KeyValue keyValue : tr.getRange(statsRange)) {
                Tuple tuple = subspace.getDirectorySubspace().unpack(keyValue.getKey());
                long statType = tuple.getLong(3);
                if (statType == SegmentStatsSubspaces.CARDINALITY) {
                    byte[] prefixBytes = tuple.getBytes(2);
                    int cardinality = ByteBuffer.wrap(keyValue.getValue()).order(ByteOrder.LITTLE_ENDIAN).getInt();
                    prefixCardinalities.put(Prefix.fromBytes(prefixBytes), cardinality);
                }
            }

            VacuumSegmentMetadataUtil.save(tr, subspace, segmentId, VacuumMetadataStatus.ANALYZE, System.currentTimeMillis());

            for (Map.Entry<Prefix, Integer> entry : prefixCardinalities.entrySet()) {
                VacuumSegmentMetadataUtil.savePrefixCardinality(tr, subspace, segmentId, entry.getKey(), entry.getValue());
            }
            tr.commit().join();
        }
    }

    private byte[] keyAfter(byte[] key) {
        byte[] result = new byte[key.length + 1];
        System.arraycopy(key, 0, result, 0, key.length);
        return result;
    }

    private byte[] entryMetadataScanPrefix(long segmentId, Prefix prefix) {
        ByteBuffer data = ByteBuffer.allocate(Long.BYTES + EntryMetadata.ENTRY_PREFIX_SIZE);
        data.putLong(segmentId);
        data.put(prefix.asBytes());
        byte[] packed = subspace.packEntryMetadataKey(data.array());
        return Arrays.copyOf(packed, packed.length - 1);
    }

    private boolean hasRemainingEntries(Transaction tr, long segmentId) {
        ByteBuffer data = ByteBuffer.allocate(Long.BYTES);
        data.putLong(segmentId);
        byte[] packed = subspace.packEntryMetadataKey(data.array());
        byte[] scanPrefix = Arrays.copyOf(packed, packed.length - 1);
        Range range = Range.startsWith(scanPrefix);
        return !tr.getRange(range, 1).asList().join().isEmpty();
    }

    boolean vacuum(VacuumContext ctx, long segmentId, EntryEvacuator evacuator) throws IOException {
        analyzeSegment(segmentId);

        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            VacuumSegmentMetadataUtil.setStatus(tr, subspace, segmentId, VacuumMetadataStatus.EVACUATING);
            tr.commit().join();
        }

        Range prefixRange = VacuumSegmentMetadataUtil.getVacuumPrefixRange(subspace, segmentId);

        boolean completed = false;
        while (true) {
            if (ctx.isStopped()) break;

            byte[] cursor = prefixRange.begin;

            while (true) {
                if (ctx.isStopped()) break;

                try (InstrumentedTransaction tr = new InstrumentedTransaction(context.getFoundationDB().createTransaction())) {
                    tr.options().setPriorityBatch();
                    Range vacuumRange = new Range(keyAfter(cursor), prefixRange.end);

                    List<KeyValue> results = tr.getRange(vacuumRange, 1).asList().join();
                    if (results.isEmpty()) {
                        break;
                    }

                    cursor = results.getFirst().getKey();

                    Tuple keyTuple = subspace.getDirectorySubspace().unpack(cursor);
                    byte[] prefixBytes = keyTuple.getBytes(4);
                    Prefix prefix = Prefix.fromBytes(prefixBytes);

                    byte[] scanPrefix = entryMetadataScanPrefix(segmentId, prefix);
                    Range entryRange = Range.startsWith(scanPrefix);

                    for (KeyValue entry : tr.getRange(entryRange)) {
                        Tuple entryTuple = subspace.getDirectorySubspace().unpack(entry.getKey());
                        byte[] metadataBytes = entryTuple.getBytes(1);
                        EntryMetadata metadata = EntryMetadata.decode(metadataBytes);
                        Versionstamp versionstamp = VersionstampUtil.decodeVersionstampedValue(entry.getValue());
                        if (!evacuator.evacuate(context, tr, metadata, versionstamp)) {
                            break;
                        }
                    }
                    tr.commit().join();
                }
            }

            try (InstrumentedTransaction tr = new InstrumentedTransaction(context.getFoundationDB().createTransaction())) {
                if (!hasRemainingEntries(tr, segmentId)) {
                    VacuumSegmentMetadataUtil.setStatus(tr, subspace, segmentId, VacuumMetadataStatus.COMPLETED);
                    tr.commit().join();
                    completed = true;
                    break;
                }
            }
        }
        return completed;
    }
}
