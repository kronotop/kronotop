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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.KronotopException;
import com.kronotop.MemberAttributes;
import com.kronotop.MetadataVersion;
import com.kronotop.cluster.ClusterConstants;
import com.kronotop.cluster.MembershipUtils;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.ShardUtils;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletionException;

class InitializeClusterSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    InitializeClusterSubcommand(RoutingService service) {
        super(service);
    }

    private void initializeRedisSection(Transaction tr, DirectorySubspace subspace) {
        int numberOfRedisShards = membership.getContext().getConfig().getInt("redis.shards");
        for (int shardId = 0; shardId < numberOfRedisShards; shardId++) {
            KronotopDirectoryNode directory = KronotopDirectory.
                    kronotop().
                    cluster(membership.getContext().getClusterName()).
                    metadata().
                    shards().
                    redis().
                    shard(shardId);
            DirectorySubspace shardSubspace = subspace.create(tr, directory.excludeSubspace(subspace)).join();
            ShardUtils.setShardStatus(tr, ShardStatus.INOPERABLE, shardSubspace);
        }
    }


    private void initializeIndexMetadata(Transaction tr, int shardId) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                shards().bucket().shard(shardId).maintenance().
                index().tasks().toList();
        DirectoryLayer.getDefault().create(tr, layout).join();
    }

    private void initializeIndexTaskCounter(Transaction tr) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                buckets().
                maintenance().
                index().
                counter();
        DirectoryLayer.getDefault().create(tr, directory.toList()).join();
    }

    private void initializeBucketMetadataVersionWitness(Transaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                bucket().
                shard(shardId).
                lastSeenVersions();
        DirectoryLayer.getDefault().create(tr, directory.toList()).join();
    }

    private void initializeBucketSection(Transaction tr, DirectorySubspace subspace) {
        int numberOfBucketShards = context.getConfig().getInt("bucket.shards");
        for (int shardId = 0; shardId < numberOfBucketShards; shardId++) {
            KronotopDirectoryNode directory = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    shards().
                    bucket().
                    shard(shardId);
            DirectorySubspace shardSubspace = subspace.create(tr, directory.excludeSubspace(subspace)).join();
            initializeIndexMetadata(tr, shardId);
            initializeBucketMetadataVersionWitness(tr, shardId);
            ShardUtils.setShardStatus(tr, ShardStatus.INOPERABLE, shardSubspace);
        }
        initializeIndexTaskCounter(tr);
    }

    private void setClusterInitializedTrue(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_INITIALIZED));
        tr.set(key, MembershipUtils.TRUE);
    }

    private void initializeCluster() {
        DirectorySubspace clusterMetadataSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);

        try (Transaction tr = membership.getContext().getFoundationDB().createTransaction()) {
            if (MembershipUtils.isClusterInitialized(tr, clusterMetadataSubspace)) {
                throw new KronotopException("cluster has already been initialized");
            }
            MetadataVersion.write(context, tr, MetadataVersion.CURRENT);
            initializeRedisSection(tr, clusterMetadataSubspace);
            initializeBucketSection(tr, clusterMetadataSubspace);
            setClusterInitializedTrue(tr, clusterMetadataSubspace);
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException ex) {
                throw new KronotopException(
                        String.format("KronotopDirectory: '%s' has already been created", String.join(".", ex.path))
                );
            }
            throw e;
        }

        context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED).set(true);
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void execute(Request request, Response response) {
        AsyncCommandExecutor.runAsync(context, response, () -> {
            try {
                initializeCluster();
            } catch (CompletionException e) {
                if (e.getCause() instanceof FDBException ex) {
                    // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                    if (ex.getCode() == 1020) {
                        // retry
                        initializeCluster();
                        return;
                    }
                }
                throw new KronotopException(e);
            }
        }, response::writeOK);
    }
}
