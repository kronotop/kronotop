/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Synchronization barrier for bucket metadata version propagation across cluster shards.
 * <p>
 * This barrier ensures that metadata changes (index additions, etc.) have been
 * observed by all shards before allowing dependent operations to proceed. It polls FoundationDB's
 * lastSeenVersions witness keys for each shard until all shards report observing the target version.
 * <p>
 * The barrier uses a retry mechanism with configurable attempts and wait duration, making it suitable
 * for eventual consistency scenarios where metadata propagation may take time due to network latency
 * or shard processing delays.
 * <p>
 * <strong>How it works:</strong>
 * <ol>
 *   <li>Each shard maintains a lastSeenVersions key in FoundationDB tracking the latest metadata version observed</li>
 *   <li>When metadata changes occur, the version is incremented and propagated via BucketMetadataWatcher</li>
 *   <li>The barrier polls all shard lastSeenVersions keys in snapshot reads</li>
 *   <li>Once all shards report a version >= target, the barrier releases</li>
 *   <li>If shards don't converge within maxAttempts, the barrier throws an exception</li>
 * </ol>
 * <p>
 * Thread-safe for concurrent barrier creation, but individual await() calls should not be shared across threads.
 */
public class BucketMetadataVersionBarrier {
    private final Context context;
    private final BucketMetadata metadata;
    private final Map<Integer, DirectorySubspace> subspaces = new HashMap<>();

    /**
     * Creates a barrier for the specified bucket metadata.
     *
     * @param context  the Kronotop context providing cluster configuration and FoundationDB access
     * @param metadata the bucket metadata to synchronize on
     */
    public BucketMetadataVersionBarrier(Context context, BucketMetadata metadata) {
        this.context = context;
        this.metadata = metadata;
    }

    /**
     * Opens or retrieves the cached lastSeenVersions subspace for a shard.
     *
     * @param tr      the read transaction to use for directory operations
     * @param shardId the shard identifier
     * @return the directory subspace containing lastSeenVersions witness keys
     */
    private DirectorySubspace openLastSeenVersionsSubspace(ReadTransaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                bucket().
                shard(shardId).
                lastSeenVersions();
        return subspaces.computeIfAbsent(shardId,
                (ignored) -> DirectoryLayer.getDefault().open(tr, directory.toList()).join()
        );
    }

    /**
     * Waits until all shards have observed the target metadata version.
     * <p>
     * Polls each shard's lastSeenVersions key using snapshot reads and retries until either all shards
     * report a version >= targetVersion or maxAttempts is exhausted. Uses snapshot isolation to avoid
     * blocking concurrent transactions.
     *
     * @param targetVersion the metadata version that all shards must observe
     * @param maxAttempts   maximum number of polling attempts before failing
     * @param waitDuration  duration to wait between polling attempts
     * @throws BarrierNotSatisfiedException if not all shards observe the target version within maxAttempts
     * @throws KronotopException if other errors occur during barrier synchronization
     */
    public void await(long targetVersion, int maxAttempts, Duration waitDuration) {
        RetryConfig config = RetryConfig.custom()
                .maxAttempts(maxAttempts)
                .waitDuration(waitDuration)
                .retryExceptions(BarrierNotSatisfiedException.class)
                .build();
        final Retry retry = Retry.of("versionBarrier", config);
        int shards = context.getConfig().getInt("bucket.shards");

        AtomicInteger attempts = new AtomicInteger();
        CheckedRunnable runnable = Retry.decorateCheckedRunnable(retry, () -> {
            attempts.getAndIncrement();
            int satisfies = 0;
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                ReadTransaction readTr = tr.snapshot();
                for (int shardId = 0; shardId < shards; shardId++) {
                    DirectorySubspace subspace = openLastSeenVersionsSubspace(readTr, shardId);
                    byte[] key = subspace.pack(Tuple.from(metadata.id()));
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
