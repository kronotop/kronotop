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

package com.kronotop.bucket.vector.changelog;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.index.MutationLogMarker;
import com.kronotop.task.TaskService;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A single per-shard, objectId-only changelog of vector index mutations. One log holds the
 * mutations of every vector index hosted on the shard, ordered by FoundationDB versionstamp.
 * It feeds vector crash recovery and standby streaming; the vector itself is never stored
 * here and is re-read from the index ENTRIES subspace during replay.
 *
 * <p>The changelog lives directly under the bucket shard directory
 * ({@code metadata/shards/bucket/<shardId>}), a namespace disjoint from per-index subspaces
 * and the shard Volume, so its magic byte never collides with those.
 */
public class ShardVectorChangelog {

    /** Magic byte for the changelog subspace within the (otherwise empty) bucket shard directory. */
    static final long CHANGELOG_MAGIC = 0x01L;

    private final Context context;
    private final DirectorySubspace shardSubspace;

    public ShardVectorChangelog(Context context, DirectorySubspace shardSubspace) {
        this.context = context;
        this.shardSubspace = shardSubspace;
    }

    private Range changelogRange() {
        return Range.startsWith(shardSubspace.pack(Tuple.from(CHANGELOG_MAGIC)));
    }

    /**
     * Appends one objectId-only mutation entry. The caller supplies {@code userVersion} so that
     * multiple entries written in the same transaction (one document touching several vector
     * indexes) get distinct, ordered keys. The current wall-clock time is stored in the value
     * for retention.
     */
    public void append(Transaction tr, long indexId, MutationLogMarker marker,
                       byte[] objectId, int userVersion) {
        Tuple keyTuple = Tuple.from(CHANGELOG_MAGIC, Versionstamp.incomplete(userVersion));
        byte[] key = shardSubspace.packWithVersionstamp(keyTuple);
        byte[] value = ShardVectorChangelogValue.encode(context.now(), indexId, marker, objectId);
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
    }

    /**
     * Reads all changelog entries in versionstamp (chronological) order.
     */
    public List<KeyValue> getRange(Transaction tr) {
        return tr.getRange(changelogRange()).asList().join();
    }

    /**
     * Truncates entries older than the configured retention window, reading
     * {@code bucket.vector.changelog.retention_period} and {@code .timeunit} from config.
     *
     * @return number of entries cleared
     */
    public int truncate(Transaction tr) {
        long retentionPeriod = context.getConfig().getLong("bucket.vector.changelog.retention_period");
        String timeunit = context.getConfig().getString("bucket.vector.changelog.timeunit");
        return truncate(tr, retentionPeriod, TaskService.timeUnitOf(timeunit));
    }

    /**
     * Truncates entries older than the retention window. Scans forward from the head and stops
     * at the first entry whose stored timestamp is within the window, then issues a single clear
     * over the expired prefix. Stopping at the first in-window entry is the safe direction: a
     * non-monotonic wall clock can only cause a few already-expired entries to be retained until
     * the next run, never the deletion of an in-window entry.
     *
     * @return number of entries cleared
     */
    public int truncate(Transaction tr, long retentionPeriod, TimeUnit timeUnit) {
        if (retentionPeriod <= 0) {
            throw new IllegalArgumentException("retention period must be greater than zero");
        }
        long cutoff = context.now() - timeUnit.toMillis(retentionPeriod);

        byte[] head = shardSubspace.pack(Tuple.from(CHANGELOG_MAGIC));
        byte[] firstSurvivorKey = null;
        int cleared = 0;

        for (KeyValue kv : tr.getRange(changelogRange())) {
            ShardVectorChangelogValue value = ShardVectorChangelogValue.decode(kv.getValue());
            if (value.timestamp() >= cutoff) {
                firstSurvivorKey = kv.getKey();
                break;
            }
            cleared++;
        }

        if (cleared == 0) {
            return 0;
        }
        byte[] end = (firstSurvivorKey != null) ? firstSurvivorKey : ByteArrayUtil.strinc(head);
        tr.clear(head, end);
        return cleared;
    }
}
