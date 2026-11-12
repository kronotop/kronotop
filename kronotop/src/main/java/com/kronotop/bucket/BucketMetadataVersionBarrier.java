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

package com.kronotop.bucket;

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BarrierNotSatisfiedException;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import io.github.resilience4j.core.functions.CheckedRunnable;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Makes dependent operations wait until every shard in the cluster has observed a target bucket
 * metadata version. Each shard records the latest version it has seen under a {@code lastSeenVersions}
 * witness key, which {@link BucketEventsWatcher} updates as metadata events arrive. The barrier polls
 * those keys with snapshot reads, so it does not block concurrent transactions. Propagation can lag
 * behind network or shard processing, so the caller sets the number of attempts and the wait between
 * them. Threads can create their own barriers concurrently, but one instance must not be shared
 * between threads.
 */
public class BucketMetadataVersionBarrier {
    private final Context context;
    private final BucketMetadata metadata;
    private final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();

    public BucketMetadataVersionBarrier(Context context, BucketMetadata metadata) {
        this.context = context;
        this.metadata = metadata;
    }

    private DirectorySubspace openLastSeenVersionsSubspace(ReadTransaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                shard(shardId).
                buckets().
                lastSeenVersions();
        return subspaces.computeIfAbsent(shardId,
                (ignored) -> context.getDirectoryLayer().open(tr, directory.toList()).join()
        );
    }

    /**
     * Waits until all shards report a metadata version at or above the target.
     *
     * @param targetVersion the metadata version that all shards must observe
     * @param maxAttempts   the maximum number of polling attempts
     * @param waitDuration  the wait between polling attempts
     * @throws BarrierNotSatisfiedException if a shard has still not observed the target version after
     *                                      {@code maxAttempts} attempts
     * @throws KronotopException            if any other error occurs while polling
     */
    public void await(long targetVersion, int maxAttempts, Duration waitDuration) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .waitDuration(waitDuration)
                .retryExceptions(BarrierNotSatisfiedException.class)
                .build();
        final Retry retry = Retry.of("versionBarrier", config);
        List<Integer> shardIds = context.getShardRegistry().getShardIds();
        int shards = shardIds.size();

        AtomicInteger attempts = new AtomicInteger();
        CheckedRunnable runnable = Retry.decorateCheckedRunnable(retry, () -> {
            attempts.getAndIncrement();
            int satisfies = 0;
            try (Transaction tr = context.getTransactionManager().create()) {
                ReadTransaction readTr = tr.snapshot();
                for (int shardId : shardIds) {
                    DirectorySubspace subspace = openLastSeenVersionsSubspace(readTr, shardId);
                    byte[] key = subspace.pack(Tuple.from(metadata.uuid()));
                    byte[] value = readTr.get(key).join();
                    if (value == null) {
                        continue;
                    }
                    long version = ByteBuffer.wrap(value).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    if (version >= targetVersion) {
                        satisfies++;
                    }
                }
            }
            if (satisfies != shards) {
                throw new BarrierNotSatisfiedException("Barrier not satisfied: not all shards observed version "
                        + targetVersion + " within " + attempts.get() + " attempts");
            }
        });

        try {
            runnable.run();
        } catch (Throwable th) {
            if (th instanceof BarrierNotSatisfiedException exp) {
                throw exp;
            }
            throw new KronotopException(th);
        }
    }
}
