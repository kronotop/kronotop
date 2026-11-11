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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.KronotopException;
import com.kronotop.MemberAttributes;
import com.kronotop.MetadataVersion;
import com.kronotop.cluster.ClusterConstants;
import com.kronotop.cluster.MembershipUtil;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.ShardUtil;
import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.*;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;
import io.netty.buffer.ByteBuf;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionException;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class InitializeClusterSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {
    private static final String SHARDS_KEYWORD = "SHARDS";
    private static final int DEFAULT_SHARD_COUNT = 1;

    InitializeClusterSubcommand(RoutingService service) {
        super(service);
    }

    private void initializeIndexMetadata(Transaction tr, int shardId) {
        List<String> layout = KronotopDirectory.
                kronotop().cluster(context.getClusterName()).metadata().
                shards().shard(shardId).maintenance().
                index().tasks().toList();
        context.getDirectoryLayer().create(tr, layout).join();
    }

    private void initializeIndexTaskCounter(Transaction tr) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                buckets().
                maintenance().
                index();
        context.getDirectoryLayer().create(tr, directory.toList()).join();
    }

    private void initializeBucketMetadataVersionWitness(Transaction tr, int shardId) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                shards().
                shard(shardId).
                lastSeenVersions();
        context.getDirectoryLayer().create(tr, directory.toList()).join();
    }

    private void initializeBucketSection(Transaction tr, DirectorySubspace subspace, int shardCount) {
        for (int shardId = 0; shardId < shardCount; shardId++) {
            KronotopDirectoryNode directory = KronotopDirectory.
                    kronotop().
                    cluster(context.getClusterName()).
                    metadata().
                    shards().
                    shard(shardId);
            DirectorySubspace shardSubspace = subspace.create(tr, directory.excludeSubspace(subspace)).join();
            initializeIndexMetadata(tr, shardId);
            initializeBucketMetadataVersionWitness(tr, shardId);
            ShardUtil.setShardStatus(tr, ShardStatus.INOPERABLE, shardSubspace);
        }
        initializeIndexTaskCounter(tr);
    }

    private void setClusterInitializedTrue(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(Tuple.from(ClusterConstants.CLUSTER_INITIALIZED));
        tr.set(key, MembershipUtil.TRUE);
    }

    private void initializeNamespaces(Transaction tr, DirectorySubspace subspace) {
        KronotopDirectoryNode directory = KronotopDirectory.
                kronotop().
                cluster(context.getClusterName()).
                metadata().
                namespaceTombstones();
        subspace.create(tr, directory.excludeSubspace(subspace)).join();
    }

    private void initializeCluster(Session session, InitializeClusterArguments arguments) {
        DirectorySubspace clusterMetadataSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);

        try (Transaction tr = context.getTransactionManager().create(session)) {
            if (MembershipUtil.isClusterInitialized(tr, clusterMetadataSubspace)) {
                throw new KronotopException("Cluster has already been initialized");
            }

            MetadataVersion.write(context, tr, MetadataVersion.CURRENT);
            initializeNamespaces(tr, clusterMetadataSubspace);
            initializeBucketSection(tr, clusterMetadataSubspace, arguments.shardCount);
            setClusterInitializedTrue(tr, clusterMetadataSubspace);
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException ex) {
                throw new KronotopException("Directory already exists: '" + String.join(".", ex.path) + "'");
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
        InitializeClusterArguments arguments = new InitializeClusterArguments(request.getArguments());
        runAsync(context, response, () -> {
            Retry retry = TransactionUtil.retry(10, Duration.ofMillis(100));
            retry.executeRunnable(() -> initializeCluster(request.getSession(), arguments));
        }, response::writeOK);
    }

    private static class InitializeClusterArguments {
        private final int shardCount;

        private InitializeClusterArguments(List<ByteBuf> args) {
            // kr.admin initialize-cluster shards 3
            if (args.size() > 3) {
                throw new WrongNumberOfArgumentsException(KrAdminMessage.COMMAND, KrAdminSubcommand.INITIALIZE_CLUSTER);
            }
            if (args.size() == 1) {
                shardCount = DEFAULT_SHARD_COUNT;
                return;
            }

            String raw = ProtocolMessageUtil.readAsString(args.get(1));
            if (!StringUtil.toUpperCaseAscii(raw).equals(SHARDS_KEYWORD)) {
                throw ProtocolMessageUtil.unknownArgument(raw);
            }
            ByteBuf value = ProtocolMessageUtil.requireValue(
                    args, 1, SHARDS_KEYWORD, ProtocolMessageUtil.POSITIVE_INTEGER);
            shardCount = ProtocolMessageUtil.readAsInteger(value);
            if (shardCount <= 0) {
                throw ProtocolMessageUtil.illegalValue(SHARDS_KEYWORD, ProtocolMessageUtil.POSITIVE_INTEGER);
            }
        }
    }
}
