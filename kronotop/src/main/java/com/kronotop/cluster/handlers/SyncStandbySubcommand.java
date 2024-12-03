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
import com.kronotop.JSONUtils;
import com.kronotop.cluster.MembershipConstants;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.MembershipUtils;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Set;

public class SyncStandbySubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    public SyncStandbySubcommand(MembershipService membership) {
        super(membership);
    }

    private void setSyncStandby(Transaction tr, SyncStandbyParameters parameters, DirectorySubspace subspace) {
        Set<String> standbyMemberIds = MembershipUtils.loadStandbyMemberIds(tr, subspace);
        if (!standbyMemberIds.contains(parameters.memberId)) {
            throw new KronotopException("member is not a standby");
        }
        Set<String> syncStandbyMemberIds = MembershipUtils.loadSyncStandbyMemberIds(tr, subspace);
        if (syncStandbyMemberIds.contains(parameters.memberId)) {
            throw new KronotopException("member is already a sync standby");
        }
        syncStandbyMemberIds.add(parameters.memberId);
        byte[] key = subspace.pack(Tuple.from(MembershipConstants.ROUTE_SYNC_STANDBY_MEMBERS));
        byte[] value = JSONUtils.writeValueAsBytes(syncStandbyMemberIds);
        tr.set(key, value);
    }

    private void unsetSyncStandby(Transaction tr, SyncStandbyParameters parameters, DirectorySubspace subspace) {
        Set<String> syncStandbyMemberIds = MembershipUtils.loadSyncStandbyMemberIds(tr, subspace);
        if (!syncStandbyMemberIds.contains(parameters.memberId)) {
            throw new KronotopException("member is not a sync standby");
        }
        syncStandbyMemberIds.remove(parameters.memberId);
        byte[] key = subspace.pack(Tuple.from(MembershipConstants.ROUTE_SYNC_STANDBY_MEMBERS));
        byte[] value = JSONUtils.writeValueAsBytes(syncStandbyMemberIds);
        tr.set(key, value);
    }

    @Override
    public void execute(Request request, Response response) {
        // kr.admin sync-standby set/unset <shard-kind> <shard-id> <member-id>
        SyncStandbyParameters parameters = new SyncStandbyParameters(request.getParams());
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(parameters.shardKind, parameters.shardId);
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            if (parameters.operationKind.equals(OperationKind.SET)) {
                setSyncStandby(tr, parameters, shardSubspace);
            } else if (parameters.operationKind.equals(OperationKind.UNSET)) {
                unsetSyncStandby(tr, parameters, shardSubspace);
            } else {
                throw new KronotopException("Unknown operation kind: " + parameters.operationKind);
            }
            membership.triggerRoutingEventsWatcher(tr);
            tr.commit().join();
        }
        response.writeOK();
    }

    enum OperationKind {
        SET,
        UNSET
    }

    class SyncStandbyParameters {
        private final OperationKind operationKind;
        private final ShardKind shardKind;
        private final int shardId;
        private final String memberId;

        SyncStandbyParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 5) {
                throw new InvalidNumberOfParametersException();
            }

            String rawOperationKind = readAsString(params.get(1));
            try {
                operationKind = OperationKind.valueOf(rawOperationKind.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid operation kind: " + rawOperationKind);
            }

            shardKind = readShardKind(params.get(2));

            String rawShardId = readAsString(params.get(3));
            shardId = readShardId(shardKind, rawShardId);

            memberId = readMemberId(params.get(4));
        }
    }
}
