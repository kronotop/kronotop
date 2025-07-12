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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.KronotopException;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.JSONUtils;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.volume.VolumeConfigGenerator;
import com.kronotop.volume.VolumeMetadata;
import com.kronotop.volume.VolumeStatus;
import com.kronotop.volume.replication.ReplicationMetadata;
import com.kronotop.volume.replication.ReplicationSlot;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

class RouteSubcommandHandler extends BaseKrAdminSubcommandHandler implements SubcommandHandler {
    public RouteSubcommandHandler(RoutingService routing) {
        super(routing);
    }

    private void setPrimaryMemberId(Transaction tr, DirectorySubspace shardSubspace, RouteParameters parameters, int shardId) {
        String primaryMemberId = MembershipUtils.loadPrimaryMemberId(tr, shardSubspace);

        // Setting the route first time
        if (primaryMemberId == null) {
            byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.ROUTE_PRIMARY_MEMBER_KEY));
            tr.set(key, parameters.memberId.getBytes());
            return;
        }

        Member nextPrimaryOwner = membership.findMember(tr, parameters.memberId);
        if (nextPrimaryOwner == null) {
            throw new KronotopException("Member could not be found: " + parameters.memberId);
        }

        Member primaryOwner = membership.findMember(tr, primaryMemberId);
        if (primaryOwner == null) {
            throw new KronotopException("Primary shard owner could not be found: " + primaryMemberId);
        }

        // Check shard status first
        ShardStatus shardStatus = ShardUtils.getShardStatus(context, tr, parameters.shardKind, shardId);
        if (shardStatus.equals(ShardStatus.READWRITE)) {
            throw new KronotopException("Shard status must not be " + ShardStatus.READWRITE);
        }

        // Load the route
        Route route = routing.findRoute(parameters.shardKind, shardId);
        if (!route.standbys().contains(nextPrimaryOwner)) {
            throw new KronotopException("Member id: " + nextPrimaryOwner.getId() + " is not a standby");
        }

        VolumeConfigGenerator volumeConfigGenerator = new VolumeConfigGenerator(context, parameters.shardKind, shardId);
        DirectorySubspace volumeSubspace = volumeConfigGenerator.openVolumeSubspace();
        VolumeMetadata volumeMetadata = VolumeMetadata.load(tr, volumeSubspace);
        if (!volumeMetadata.getStatus().equals(VolumeStatus.READONLY)) {
            throw new KronotopException("Volume status must be " + VolumeStatus.READONLY);
        }

        // Find the replication slot
        Versionstamp slotId = ReplicationMetadata.findSlotId(tr, volumeSubspace, nextPrimaryOwner.getId(), parameters.shardKind, shardId);
        ReplicationSlot slot = ReplicationSlot.load(tr, parameters.shardKind, shardId, slotId, volumeSubspace);

        // Check the replication status
        Versionstamp latestVersionstampedKey = ReplicationMetadata.findLatestVersionstampedKey(context, volumeSubspace);
        if (latestVersionstampedKey == null) {
            throw new KronotopException("Latest versionstamped key not found");
        }

        // Caught up?
        if (!Arrays.equals(latestVersionstampedKey.getBytes(), slot.getReceivedVersionstampedKey())) {
            throw new KronotopException("Primary owner and standby does not match");
        }

        // Ready to assign a new primary
        byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.ROUTE_PRIMARY_MEMBER_KEY));
        tr.set(key, parameters.memberId.getBytes());

        // Cleanup

        // Standbys
        Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
        standbyMemberIds.remove(parameters.memberId);
        MembershipUtils.setStandbyMemberIds(tr, shardSubspace, standbyMemberIds);

        // Sync standbys
        Set<String> syncStandbyMemberIds = MembershipUtils.loadSyncStandbyMemberIds(tr, shardSubspace);
        syncStandbyMemberIds.remove(parameters.memberId);
        MembershipUtils.setSyncStandbyMemberIds(tr, shardSubspace, syncStandbyMemberIds);

        // Mark the replication slot as STALE
        ReplicationSlot.compute(tr, parameters.shardKind, shardId, slotId, volumeSubspace, (staleSlot) -> {
            staleSlot.setActive(false);
            staleSlot.setStale();
        });
    }

    private void appendStandbyMemberId(Transaction tr, DirectorySubspace shardSubspace, RouteParameters parameters) {
        String primaryMemberId = MembershipUtils.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryMemberId == null) {
            throw new KronotopException("no primary member assigned yet");
        }

        if (primaryMemberId.equals(parameters.memberId)) {
            throw new KronotopException("primary cannot be assigned as a standby");
        }

        Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
        if (standbyMemberIds.contains(parameters.memberId)) {
            throw new KronotopException("already assigned as a standby");
        }

        standbyMemberIds.add(parameters.memberId);
        byte[] key = shardSubspace.pack(Tuple.from(ShardConstants.ROUTE_STANDBY_MEMBER_KEY));
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
        AsyncCommandExecutor.runAsync(context, response, () -> {
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
        }, response::writeOK);
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

            String rawOperationKind = ProtocolMessageUtil.readAsString(params.get(1));
            try {
                operationKind = OperationKind.valueOf(rawOperationKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid operation kind: " + rawOperationKind);
            }

            String rawRouteKind = ProtocolMessageUtil.readAsString(params.get(2));
            try {
                routeKind = RouteKind.valueOf(rawRouteKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid route kind: " + rawRouteKind);
            }

            shardKind = readShardKind(params.get(3));

            String rawShardId = ProtocolMessageUtil.readAsString(params.get(4));
            allShards = rawShardId.equals("*");
            if (!allShards) {
                shardId = readShardId(shardKind, rawShardId);
            } else {
                shardId = -1; // dummy assignment due to final declaration
            }

            memberId = ProtocolMessageUtil.readMemberId(context, params.get(5));
        }
    }
}
