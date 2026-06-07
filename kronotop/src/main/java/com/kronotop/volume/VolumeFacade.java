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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.volume.changelog.ChangeLog;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Standalone facade for Volume metadata operations on non-owner cluster members.
 * Performs all FoundationDB operations without requiring a local Volume instance.
 */
public class VolumeFacade {
    private static final byte[] DECREASE_BY_ONE_DELTA = new byte[]{-1, -1, -1, -1};
    private static final byte[] INCREASE_BY_ONE_DELTA = new byte[]{1, 0, 0, 0};

    private final Context context;

    public VolumeFacade(Context context) {
        this.context = context;
    }

    public void deleteByVersionstampedEntry(
            @Nonnull VolumeSubspace subspace,
            @Nonnull VolumeSession session,
            @Nonnull VersionstampedEntry... entries
    ) {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Empty entries array");
        }
        Transaction tr = session.transaction();
        ChangeLog changeLog = new ChangeLog(context, subspace.getDirectorySubspace());

        for (VersionstampedEntry entry : entries) {
            EntryMetadata metadata = entry.metadata();
            Versionstamp key = entry.key();
            byte[] encodedMetadata = metadata.encode();

            byte[] entryKey = subspace.packEntryKey(session.prefix(), key);
            tr.clear(entryKey);

            byte[] reverseKey = subspace.packEntryMetadataKey(encodedMetadata);
            tr.clear(reverseKey);

            byte[] cardinalityKey = subspace.packSegmentStatsKey(
                    metadata.segmentId(),
                    session.prefix(),
                    SegmentStatsSubspaces.CARDINALITY
            );
            tr.mutate(MutationType.ADD, cardinalityKey, DECREASE_BY_ONE_DELTA);

            byte[] usedBytesKey = subspace.packSegmentStatsKey(
                    metadata.segmentId(),
                    session.prefix(),
                    SegmentStatsSubspaces.USED_BYTES
            );
            byte[] delta = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(-metadata.length()).array();
            tr.mutate(MutationType.ADD, usedBytesKey, delta);

            changeLog.deleteOperation(tr, metadata, session.prefix(), key);
        }

        byte[] mutationTriggerKey = VolumeUtil.computeMutationTriggerKey(subspace.getDirectorySubspace());
        tr.mutate(MutationType.ADD, mutationTriggerKey, INCREASE_BY_ONE_DELTA);
    }
}
