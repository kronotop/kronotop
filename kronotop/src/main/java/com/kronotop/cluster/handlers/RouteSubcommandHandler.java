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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.KronotopException;
import com.kronotop.cluster.*;
import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.WrongNumberOfArgumentsException;
import com.kronotop.volume.VolumeMetadataUtil;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.VolumeStatus;
import com.kronotop.volume.VolumeSubspace;
import com.kronotop.volume.replication.ReplicationUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Set;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class RouteSubcommandHandler extends BaseKrAdminSubcommandHandler implements SubcommandHandler {
    public RouteSubcommandHandler(RoutingService routing) {
        super(routing);
    }

    private void setPrimaryMemberId(Transaction tr, DirectorySubspace shardSubspace, RouteArguments arguments, int shardId) {
        String primaryMemberId = MembershipUtil.loadPrimaryMemberId(tr, shardSubspace);

        // Setting the route first time
        if (primaryMemberId == null) {
            byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.ROUTE_PRIMARY_MEMBER_KEY));
            tr.set(key, arguments.memberId.getBytes());
            return;
        }

        Member nextPrimaryOwner = membership.findMember(tr, arguments.memberId);
        if (nextPrimaryOwner == null) {
            throw new MemberNotRegisteredException(arguments.memberId);
        }

        Member primaryOwner = membership.findMember(tr, primaryMemberId);
        if (primaryOwner == null) {
            throw new KronotopException("Primary shard owner could not be found: " + primaryMemberId);
        }

        // Check shard status first
        ShardStatus shardStatus = ShardUtil.getShardStatus(context, tr, shardId);
        if (shardStatus.equals(ShardStatus.READWRITE)) {
            throw new KronotopException("Shard status must not be " + ShardStatus.READWRITE);
        }

        Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
        if (!standbyMemberIds.contains(nextPrimaryOwner.getId())) {
            throw new KronotopException("Member is not a standby: '" + nextPrimaryOwner.getId() + "'");
        }

        VolumeService volumeService = context.getService(VolumeService.NAME);
        VolumeSubspace subspace = new VolumeSubspace(volumeService.openSubspace(shardId));
        VolumeStatus status = VolumeMetadataUtil.readVolumeStatus(tr, subspace);
        if (status != VolumeStatus.READONLY) {
            throw new KronotopException("Volume status must be " + VolumeStatus.READONLY);
        }

        ReplicationUtil.assertStandbyCaughtUp(context, tr, nextPrimaryOwner, subspace.getDirectorySubspace(), arguments.shardId);

        // Ready to assign a new primary
        byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.ROUTE_PRIMARY_MEMBER_KEY));
        tr.set(key, arguments.memberId.getBytes());

        // Cleanup

        standbyMemberIds.remove(arguments.memberId);
        MembershipUtil.setStandbyMemberIds(tr, shardSubspace, standbyMemberIds);
    }

    private void appendStandbyMemberId(Transaction tr, DirectorySubspace shardSubspace, RouteArguments arguments) {
        String primaryMemberId = MembershipUtil.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryMemberId == null) {
            throw new KronotopException("No primary member assigned yet");
        }

        if (primaryMemberId.equals(arguments.memberId)) {
            throw new KronotopException("Primary cannot be assigned as a standby");
        }

        Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
        if (standbyMemberIds.contains(arguments.memberId)) {
            throw new KronotopException("Already assigned as a standby");
        }

        standbyMemberIds.add(arguments.memberId);
        byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.ROUTE_STANDBY_MEMBER_KEY));
        byte[] value = JSONUtil.writeValueAsBytes(standbyMemberIds);
        tr.set(key, value);
    }

    private void removeStandbyMemberId(Transaction tr, DirectorySubspace shardSubspace, RouteArguments arguments) {
        String primaryMemberId = MembershipUtil.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryMemberId == null) {
            throw new KronotopException("No primary member assigned yet");
        }

        Set<String> standbyMemberIds = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
        if (!standbyMemberIds.contains(arguments.memberId)) {
            throw new KronotopException("Member is not a standby: '" + arguments.memberId + "'");
        }

        standbyMemberIds.remove(arguments.memberId);
        MembershipUtil.setStandbyMemberIds(tr, shardSubspace, standbyMemberIds);
    }

    private void setRouteForShard(Transaction tr, RouteArguments arguments, int shardId) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardId);
        if (arguments.routeKind.equals(RouteKind.PRIMARY)) {
            setPrimaryMemberId(tr, shardSubspace, arguments, shardId);
        } else if (arguments.routeKind.equals(RouteKind.STANDBY)) {
            appendStandbyMemberId(tr, shardSubspace, arguments);
        } else {
            // This should be impossible!
            throw new KronotopException("Unknown route kind: " + arguments.routeKind);
        }
    }

    private void unsetRouteForShard(Transaction tr, RouteArguments arguments, int shardId) {
        if (arguments.routeKind.equals(RouteKind.PRIMARY)) {
            throw new KronotopException("UNSET PRIMARY is not supported");
        }
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardId);
        removeStandbyMemberId(tr, shardSubspace, arguments);
    }

    @Override
    public void execute(Request request, Response response) {
        RouteArguments arguments = new RouteArguments(request.getArguments());
        runAsync(context, response, () -> {
            try (Transaction tr = context.getTransactionManager().create(request.getSession())) {
                if (!membership.isMemberRegistered(tr, arguments.memberId)) {
                    throw new MemberNotRegisteredException(arguments.memberId);
                }
                switch (arguments.operationKind) {
                    case SET -> {
                        if (arguments.allShards) {
                            for (int shardId : getShardIds()) {
                                setRouteForShard(tr, arguments, shardId);
                            }
                        } else {
                            setRouteForShard(tr, arguments, arguments.shardId);
                        }
                    }
                    case UNSET -> {
                        if (arguments.allShards) {
                            for (int shardId : getShardIds()) {
                                unsetRouteForShard(tr, arguments, shardId);
                            }
                        } else {
                            unsetRouteForShard(tr, arguments, arguments.shardId);
                        }
                    }
                }
                ClusterUtil.signalTopologyChange(context, tr);
                tr.commit().join();
            }
        }, response::writeOK);
    }

    enum OperationKind {
        SET,
        UNSET
    }

    private class RouteArguments {
        private final OperationKind operationKind;
        private final RouteKind routeKind;
        private final int shardId;
        private final String memberId;

        private final boolean allShards;

        RouteArguments(ArrayList<ByteBuf> args) {
            // kr.admin route set primary * 12b3cf60
            if (args.size() != 5) {
                throw new WrongNumberOfArgumentsException(KrAdminMessage.COMMAND, KrAdminSubcommand.ROUTE);
            }

            operationKind = ProtocolMessageUtil.readEnum(OperationKind.class, args.get(1), "operation kind");
            routeKind = ProtocolMessageUtil.readEnum(RouteKind.class, args.get(2), "route kind");

            String rawShardId = ProtocolMessageUtil.readAsString(args.get(3));
            allShards = rawShardId.equals("*");
            if (!allShards) {
                shardId = ProtocolMessageUtil.readShardId(context.getShardRegistry(), rawShardId);
            } else {
                shardId = -1; // dummy assignment due to final declaration
            }

            memberId = ProtocolMessageUtil.readMemberId(context, args.get(4));
        }
    }
}
