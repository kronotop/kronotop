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

package com.kronotop.bucket.vector;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.index.IndexSubspaceMagic;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Replays every entry in the FAILED_OP_LOG subspace into the on-heap index.
 * Log entries are not removed after the replay.
 */
public final class ReplayFailedOpsLog {
    static final int PAGE_SIZE = 1000;

    /**
     * Scans the whole FAILED_OP_LOG subspace in pages and applies each entry to the on-heap index.
     * Each page is read in its own transaction. Deletes are also applied to the on-disk indexes of the group.
     *
     * @return the adds and deletes that failed to apply again
     */
    public static FailedOps replay(
            Database db,
            VectorGraphIndexGroup group,
            OnHeapVectorGraphIndex onHeap,
            DirectorySubspace indexSubspace,
            ExecutorService executor
    ) {
        byte[] prefix = indexSubspace.pack(Tuple.from(IndexSubspaceMagic.FAILED_OP_LOG.getValue()));
        KeySelector begin = KeySelector.firstGreaterOrEqual(prefix);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(prefix));

        List<RecoveredState.FailedAdd> failedAdds = new ArrayList<>();
        List<RecoveredState.FailedDelete> failedDeletes = new ArrayList<>();
        boolean replayed = false;
        while (true) {
            List<KeyValue> page;
            try (Transaction tr = db.createTransaction()) {
                page = tr.getRange(begin, end, PAGE_SIZE).asList().join();
            }
            if (page.isEmpty()) {
                break;
            }
            FailedOps failedOps = VectorIndexCrashRecovery.replayMutationLog(
                    group,
                    onHeap,
                    page,
                    indexSubspace,
                    executor
            );
            failedAdds.addAll(failedOps.adds());
            failedDeletes.addAll(failedOps.deletes());
            replayed = true;
            if (page.size() < PAGE_SIZE) {
                break;
            }
            begin = KeySelector.firstGreaterThan(page.getLast().getKey());
        }

        if (replayed) {
            for (OnDiskVectorGraphIndex onDisk : group.getOnDiskIndexes()) {
                onDisk.flushMetadata();
            }
        }
        return new FailedOps(failedAdds, failedDeletes);
    }
}
