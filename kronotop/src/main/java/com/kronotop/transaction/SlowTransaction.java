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

package com.kronotop.transaction;

import com.kronotop.bucket.bql.ast.BqlExpr;

/**
 * Snapshot of a closed transaction that ran longer than the slow log threshold.
 * The query is kept as an AST and rendered when the log is read.
 */
public record SlowTransaction(
        long id,
        long timestamp,
        long durationMs,
        long commitLatencyMs,
        long clientId,
        String namespace,
        String bucket,
        String model,
        String command,
        BqlExpr query,
        boolean committed,
        boolean conflicted,
        long reads,
        long rangeReads,
        long writes,
        long deletes,
        long rangeDeletes,
        long mutations,
        long bytesRead,
        long bytesWritten
) {
    static SlowTransaction of(InstrumentedTransaction tr) {
        TransactionMetrics metrics = tr.getMetrics();
        return new SlowTransaction(
                tr.getId(),
                System.currentTimeMillis(),
                metrics.getTotalDurationNanos() / 1_000_000,
                metrics.getCommitLatencyNanos() / 1_000_000,
                tr.getClientId(),
                tr.getNamespace(),
                tr.getBucket(),
                tr.getModel(),
                tr.getCommand(),
                tr.getQuery(),
                metrics.isCommitted(),
                metrics.isConflicted(),
                metrics.getReads(),
                metrics.getRangeReads(),
                metrics.getWrites(),
                metrics.getDeletes(),
                metrics.getRangeDeletes(),
                metrics.getMutations(),
                metrics.getBytesRead(),
                metrics.getBytesWritten()
        );
    }
}
