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
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.JSONUtils;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.VolumeConfigGenerator;
import com.kronotop.volume.replication.ReplicationConfig;
import com.kronotop.volume.replication.ReplicationMetadata;
import com.kronotop.volume.replication.ReplicationSlot;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Set;

class RouteHandler extends BaseKrAdminSubcommandHandler implements SubcommandHandler {
    public RouteHandler(MembershipService service) {
        super(service);
    }

    private void setPrimaryMemberId(Transaction tr, DirectorySubspace shardSubspace, RouteParameters parameters, int shardId) {
        String primaryMemberId = MembershipUtils.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryMemberId == null) {
            // Setting the route first time
            byte[] key = shardSubspace.pack(Tuple.from(MembershipConstants.ROUTE_PRIMARY_MEMBER_KEY));
            tr.set(key, parameters.memberId.getBytes());
            return;
        }

        // Already have a primary owner. The current status of the primary owner is not the case.
        Member primaryOwner = membership.findMember(tr, primaryMemberId);
        if (primaryOwner == null) {
            throw new IllegalStateException("Primary owner not found: " + primaryMemberId);
        }
        if (primaryOwner.getStatus().equals(MemberStatus.RUNNING)) {
            // Check shard status first
            ShardStatus shardStatus = ShardUtils.getShardStatus(context, tr, parameters.shardKind, shardId);
            if (shardStatus.equals(ShardStatus.READWRITE)) {
                throw new IllegalStateException("Shard status is READWRITE");
            }

            // ShardStatus: READONLY or INOPERABLE
            Member nextPrimaryOwner = membership.findMember(tr, parameters.memberId);
            if (nextPrimaryOwner == null) {
                throw new IllegalStateException("Next primary owner not found: " + primaryMemberId);
            }
            // Load the route
            Route route = routing.findRoute(parameters.shardKind, shardId);
            if (!route.standbys().contains(nextPrimaryOwner)) {
                throw new IllegalStateException("The next primary owner not an existing standby: " + nextPrimaryOwner.getId());
            }

            // Check replication status
            DirectorySubspace volumeSubspace = new VolumeConfigGenerator(context, parameters.shardKind, parameters.shardId).createOrOpenVolumeSubspace();
            Versionstamp slotId = ReplicationMetadata.findSlotId(tr, volumeSubspace, parameters.memberId, parameters.shardKind, shardId);
            if (slotId == null) {
                throw new IllegalStateException("Replication Slot not found: " + parameters.memberId);
            }
            ReplicationSlot replicationSlot = ReplicationSlot.load(tr, parameters.shardKind, shardId, slotId, volumeSubspace);
            // TODO: Continue from here
        }
    }

    private void appendStandbyMemberId(Transaction tr, DirectorySubspace subspace, RouteParameters parameters) {
        String primaryMemberId = MembershipUtils.loadPrimaryMemberId(tr, subspace);
        if (primaryMemberId == null) {
            throw new KronotopException("no primary member assigned yet");
        }

        if (primaryMemberId.equals(parameters.memberId)) {
            throw new KronotopException("primary cannot be assigned as a standby");
        }

        Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, subspace);
        if (standbyMemberIds.contains(parameters.memberId)) {
            throw new KronotopException("already assigned as a standby");
        }

        standbyMemberIds.add(parameters.memberId);
        byte[] key = subspace.pack(Tuple.from(MembershipConstants.ROUTE_STANDBY_MEMBER_KEY));
        byte[] value = JSONUtils.writeValueAsBytes(standbyMemberIds);
        tr.set(key, value);
    }

    private void setRouteForShard(Transaction tr, RouteParameters parameters, int shardId) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(parameters.shardKind, shardId);
        if (parameters.routeKind.equals(RouteKind.PRIMARY)) {
            setPrimaryMemberId(tr, shardSubspace, parameters, shardId);
        } else if (parameters.routeKind.equals(RouteKind.STANDBY)) {
            appendStandbyMemberId(tr, shardSubspace, parameters);
        } else {
            // This should be impossible!
            throw new KronotopException("Unknown route kind: " + parameters.routeKind);
        }
    }

    @Override
    public void execute(Request request, Response response) {
        RouteParameters parameters = new RouteParameters(request.getParams());
        if (parameters.operationKind.equals(OperationKind.SET)) {
            try (Transaction tr = membership.getContext().getFoundationDB().createTransaction()) {
                if (!membership.isMemberRegistered(parameters.memberId)) {
                    throw new KronotopException("member not found");
                }
                if (parameters.allShards) {
                    int numberOfShards = getNumberOfShards(parameters.shardKind);
                    for (int shardId = 0; shardId < numberOfShards; shardId++) {
                        setRouteForShard(tr, parameters, shardId);
                    }
                } else {
                    setRouteForShard(tr, parameters, parameters.shardId);
                }
                membership.triggerClusterTopologyWatcher(tr);
                tr.commit().join();
            }
        } else {
            throw new KronotopException("Unknown operation kind: " + parameters.operationKind);
        }
        response.writeOK();
    }

    enum OperationKind {
        SET,
        UNSET
    }

    private class RouteParameters {
        private final OperationKind operationKind;
        private final RouteKind routeKind;
        private final ShardKind shardKind;
        private final int shardId;
        private final String memberId;

        private final boolean allShards;

        RouteParameters(ArrayList<ByteBuf> params) {
            // kr.admin route set primary redis * 12b3cf60
            if (params.size() != 6) {
                throw new InvalidNumberOfParametersException();
            }

            String rawOperationKind = readAsString(params.get(1));
            try {
                operationKind = OperationKind.valueOf(rawOperationKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid operation kind: " + rawOperationKind);
            }

            String rawRouteKind = readAsString(params.get(2));
            try {
                routeKind = RouteKind.valueOf(rawRouteKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid route kind: " + rawRouteKind);
            }

            shardKind = readShardKind(params.get(3));

            String rawShardId = readAsString(params.get(4));
            allShards = rawShardId.equals("*");
            if (!allShards) {
                shardId = readShardId(shardKind, rawShardId);
            } else {
                shardId = -1; // dummy assignment due to final declaration
            }

            memberId = readMemberId(params.get(5));
        }
    }
}
