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
import com.kronotop.Context;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.volume.VolumeNames;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Base class of the KR.ADMIN subcommand handlers. It holds the shared services and the helpers
 * that read command arguments and build replies.
 */
public class BaseKrAdminSubcommandHandler {
    protected final Context context;
    protected final MembershipService membership;
    protected final RoutingService routing;

    /**
     * Creates the handler with the services it shares with its subclasses.
     *
     * @param service the routing service
     */
    public BaseKrAdminSubcommandHandler(RoutingService service) {
        this.context = service.getContext();
        this.routing = service;
        // Membership service has been started before RoutingService.
        this.membership = service.getContext().getService(MembershipService.NAME);
    }

    protected static FullBulkStringRedisMessage bulkString(String input) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(input.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Returns the primary, standbys, status and linked volumes of a shard as reply fields.
     *
     * @param tr      the transaction to read with
     * @param shardId the shard ID
     * @return the reply fields in output order
     */
    protected Map<RedisMessage, RedisMessage> describeShard(Transaction tr, int shardId) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardId);
        Map<RedisMessage, RedisMessage> shard = new LinkedHashMap<>();

        String primaryRouteMemberId = MembershipUtil.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryRouteMemberId == null) {
            primaryRouteMemberId = "";
        }
        shard.put(bulkString("primary"), bulkString(primaryRouteMemberId));

        List<RedisMessage> standbyMessages = new ArrayList<>();
        Set<String> standbys = MembershipUtil.loadStandbyMemberIds(tr, shardSubspace);
        if (standbys != null) {
            for (String standby : standbys) {
                standbyMessages.add(bulkString(standby));
            }
        }
        shard.put(bulkString("standbys"), new ArrayRedisMessage(standbyMessages));

        ShardStatus status = ShardUtil.getShardStatus(tr, shardSubspace);
        shard.put(bulkString("status"), bulkString(status.name()));

        // TODO: This is not sufficient but it works for now.
        // Ideally, we should get this info from FDB, re-generating the volume name is not good.
        List<RedisMessage> linkedVolumes = new ArrayList<>();
        linkedVolumes.add(bulkString(VolumeNames.format(shardId)));
        shard.put(bulkString("linked_volumes"), new ArrayRedisMessage(linkedVolumes));

        return shard;
    }

    /**
     * Returns the shard IDs of the cluster.
     *
     * @return the sorted shard IDs
     */
    protected List<Integer> getShardIds() {
        return context.getShardRegistry().getShardIds();
    }

    /**
     * Reads a shard status from a command argument.
     *
     * @param shardStatusBuf the raw bytes of the argument
     * @return the shard status
     * @throws IllegalCommandArgumentException if the shard status is unknown
     */
    protected ShardStatus readShardStatus(ByteBuf shardStatusBuf) {
        return ProtocolMessageUtil.readEnum(ShardStatus.class, shardStatusBuf, "shard status");
    }

    /**
     * Reads a member status from a command argument.
     *
     * @param memberStatusBuf the raw bytes of the argument
     * @return the member status
     * @throws IllegalCommandArgumentException if the member status is unknown
     */
    protected MemberStatus readMemberStatus(ByteBuf memberStatusBuf) {
        return ProtocolMessageUtil.readEnum(MemberStatus.class, memberStatusBuf, "member status");
    }

    /**
     * Adds the status, process ID, advertise addresses and latest heartbeat of a member to a reply.
     *
     * @param member  the member to describe
     * @param current the reply fields to add to
     */
    protected void memberToRedisMessage(Member member, Map<RedisMessage, RedisMessage> current) {
        current.put(bulkString("status"), bulkString(member.getStatus().toString()));

        String processId = VersionstampUtil.base32HexEncode(member.getProcessId());
        current.put(bulkString("process_id"), bulkString(processId));

        current.put(bulkString("external_advertise"), advertiseToRedisMessage(member.getExternalAdvertise()));
        current.put(bulkString("internal_advertise"), advertiseToRedisMessage(member.getInternalAdvertise()));

        long latestHeartbeat = membership.getLatestHeartbeat(member);
        current.put(bulkString("latest_heartbeat"), new IntegerRedisMessage(latestHeartbeat));
    }

    private ArrayRedisMessage advertiseToRedisMessage(List<Address> addresses) {
        List<RedisMessage> children = new ArrayList<>(addresses.size());
        for (Address address : addresses) {
            children.add(bulkString(address.toString()));
        }
        return new ArrayRedisMessage(children);
    }
}
