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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.DirectorySubspaceCache;
import com.kronotop.cluster.ClusterNotInitializedException;
import com.kronotop.cluster.MembershipUtils;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Set;

@Command(KrAdminMessage.COMMAND)
@MinimumParameterCount(KrAdminMessage.MINIMUM_PARAMETER_COUNT)
public class KrAdminHandler implements Handler {
    private final Context context;

    private final EnumMap<KrAdminSubcommand, SubcommandHandler> handlers = new EnumMap<>(KrAdminSubcommand.class);
    private final Set<KrAdminSubcommand> subcommandsRequireInitializedCluster = new HashSet<>();

    public KrAdminHandler(RoutingService service) {
        this.context = service.getContext();
        handlers.put(KrAdminSubcommand.LIST_MEMBERS, new ListMembersSubcommand(service));
        handlers.put(KrAdminSubcommand.INITIALIZE_CLUSTER, new InitializeClusterSubcommand(service));
        handlers.put(KrAdminSubcommand.DESCRIBE_CLUSTER, new DescribeClusterSubcommand(service));
        handlers.put(KrAdminSubcommand.SET_MEMBER_STATUS, new SetMemberStatusSubcommand(service));
        handlers.put(KrAdminSubcommand.FIND_MEMBER, new FindMemberSubcommand(service));
        handlers.put(KrAdminSubcommand.REMOVE_MEMBER, new RemoveMemberSubcommand(service));
        handlers.put(KrAdminSubcommand.LIST_SILENT_MEMBERS, new ListSilentMembers(service));
        handlers.put(KrAdminSubcommand.ROUTE, new RouteHandler(service));
        handlers.put(KrAdminSubcommand.SET_SHARD_STATUS, new SetShardStatusSubcommand(service));
        handlers.put(KrAdminSubcommand.DESCRIBE_SHARD, new DescribeShardSubcommand(service));
        handlers.put(KrAdminSubcommand.SYNC_STANDBY, new SyncStandbySubcommand(service));

        subcommandsRequireInitializedCluster.add(KrAdminSubcommand.DESCRIBE_CLUSTER);
        subcommandsRequireInitializedCluster.add(KrAdminSubcommand.DESCRIBE_SHARD);
        subcommandsRequireInitializedCluster.add(KrAdminSubcommand.ROUTE);
        subcommandsRequireInitializedCluster.add(KrAdminSubcommand.SET_SHARD_STATUS);
        subcommandsRequireInitializedCluster.add(KrAdminSubcommand.SYNC_STANDBY);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.KRADMIN).set(new KrAdminMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        KrAdminMessage message = request.attr(MessageTypes.KRADMIN).get();
        SubcommandHandler executor = handlers.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }

        if (subcommandsRequireInitializedCluster.contains(message.getSubcommand())) {
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                DirectorySubspace clusterMetadataSubspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
                if (!MembershipUtils.isClusterInitialized(tr, clusterMetadataSubspace)) {
                    throw new ClusterNotInitializedException();
                }
            }
        }
        executor.execute(request, response);
    }
}
