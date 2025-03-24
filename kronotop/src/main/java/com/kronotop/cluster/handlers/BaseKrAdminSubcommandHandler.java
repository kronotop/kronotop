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
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.internal.ByteBufUtils;
import com.kronotop.internal.VersionstampUtils;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.volume.VolumeConfigGenerator;
import io.netty.buffer.ByteBuf;

import java.util.*;

public class BaseKrAdminSubcommandHandler {
    protected final Context context;
    protected final MembershipService membership;
    protected final RoutingService routing;

    public BaseKrAdminSubcommandHandler(RoutingService service) {
        this.context = service.getContext();
        this.routing = service;
        // Membership service has been started before RoutingService.
        this.membership = service.getContext().getService(MembershipService.NAME);
    }

    /**
     * Describes the metadata and status of a specific shard.
     * Retrieves information about the primary member, standby members,
     * synchronized standby members, and the status of the shard based on its type and ID.
     *
     * @param tr        The transaction object used for accessing the database.
     * @param shardKind The kind of shard, represented as a {@link ShardKind}.
     * @param shardId   The ID of the shard to describe.
     * @return A map where the keys are {@link RedisMessage} instances representing metadata
     * descriptors (e.g., "primary", "standbys", "sync_standbys", "status") and the
     * values are {@link RedisMessage} instances containing the respective information.
     */
    protected Map<RedisMessage, RedisMessage> describeShard(Transaction tr, ShardKind shardKind, int shardId) {
        DirectorySubspace shardSubspace = context.getDirectorySubspaceCache().get(shardKind, shardId);
        Map<RedisMessage, RedisMessage> shard = new LinkedHashMap<>();

        String primaryRouteMemberId = MembershipUtils.loadPrimaryMemberId(tr, shardSubspace);
        if (primaryRouteMemberId == null) {
            primaryRouteMemberId = "";
        }
        shard.put(new SimpleStringRedisMessage("primary"), new SimpleStringRedisMessage(primaryRouteMemberId));

        List<RedisMessage> standbyMessages = new ArrayList<>();
        Set<String> standbys = MembershipUtils.loadStandbyMemberIds(tr, shardSubspace);
        if (standbys != null) {
            for (String standby : standbys) {
                standbyMessages.add(new SimpleStringRedisMessage(standby));
            }
        }
        shard.put(new SimpleStringRedisMessage("standbys"), new ArrayRedisMessage(standbyMessages));

        List<RedisMessage> syncStandbyMessages = new ArrayList<>();
        Set<String> syncStandbys = MembershipUtils.loadSyncStandbyMemberIds(tr, shardSubspace);
        if (standbys != null) {
            for (String syncStandby : syncStandbys) {
                syncStandbyMessages.add(new SimpleStringRedisMessage(syncStandby));
            }
        }
        shard.put(new SimpleStringRedisMessage("sync_standbys"), new ArrayRedisMessage(syncStandbyMessages));

        ShardStatus status = ShardUtils.getShardStatus(tr, shardSubspace);
        shard.put(new SimpleStringRedisMessage("status"), new SimpleStringRedisMessage(status.name()));

        // TODO: This is not sufficient but it works for now.
        // Ideally, we should get this info from FDB, re-generating the volume name is not good.
        List<RedisMessage> linkedVolumes = new ArrayList<>();
        linkedVolumes.add(new SimpleStringRedisMessage(VolumeConfigGenerator.volumeName(shardKind, shardId)));
        shard.put(new SimpleStringRedisMessage("linked_volumes"), new ArrayRedisMessage(linkedVolumes));

        return shard;
    }

    /**
     * Retrieves the number of shards for a given shard kind.
     *
     * @param kind The type of shard whose number is to be retrieved. Must be of type {@link ShardKind}.
     * @return The number of shards for the specified shard kind.
     * @throws IllegalArgumentException if the specified shard kind is not recognized.
     */
    protected int getNumberOfShards(ShardKind kind) {
        if (kind.equals(ShardKind.REDIS)) {
            return membership.getContext().getConfig().getInt("redis.shards");
        } else if (kind.equals(ShardKind.BUCKET)) {
            return membership.getContext().getConfig().getInt("bucket.shards");
        }
        throw new IllegalArgumentException("Unknown shard kind: " + kind);
    }

    /**
     * Reads the shard status from the provided ByteBuf and converts it into a ShardStatus enum value.
     *
     * @param shardStatusBuf the ByteBuf containing the raw bytes of the shard status
     * @return the corresponding ShardStatus enum value
     * @throws KronotopException if the shard status is invalid
     */
    protected ShardStatus readShardStatus(ByteBuf shardStatusBuf) {
        byte[] rawShardStatus = new byte[shardStatusBuf.readableBytes()];
        shardStatusBuf.readBytes(rawShardStatus);
        String stringShardStatus = new String(rawShardStatus);

        try {
            return ShardStatus.valueOf(stringShardStatus.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid shard status " + stringShardStatus);
        }
    }

    /**
     * Reads the member status from the provided ByteBuf.
     *
     * @param memberStatusBuf the ByteBuf containing the raw bytes of the member status
     * @return the corresponding MemberStatus enum value
     * @throws KronotopException if the member status is invalid
     */
    protected MemberStatus readMemberStatus(ByteBuf memberStatusBuf) {
        byte[] rawMemberStatus = new byte[memberStatusBuf.readableBytes()];
        memberStatusBuf.readBytes(rawMemberStatus);
        String stringMemberStatus = new String(rawMemberStatus);
        try {
            return MemberStatus.valueOf(stringMemberStatus.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid member status " + stringMemberStatus);
        }
    }

    /**
     * Reads the shard kind from the provided ByteBuf.
     *
     * @param shardKindBuf the ByteBuf containing the raw bytes of the shard kind
     * @return the corresponding ShardKind enum value
     * @throws KronotopException if the shard kind is invalid
     */
    protected ShardKind readShardKind(ByteBuf shardKindBuf) {
        String rawKind = ByteBufUtils.readAsString(shardKindBuf);
        try {
            return ShardKind.valueOf(rawKind.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("invalid shard kind");
        }
    }

    /**
     * Reads the shard ID from the provided ByteBuf and validates it based on the given shard kind.
     *
     * @param shardKind  The type of shard whose ID is being read. Must be of type {@link ShardKind}.
     * @param shardIdBuf The ByteBuf containing the raw bytes of the shard ID.
     * @return The validated shard ID as an integer.
     * @throws InvalidShardIdException if the shard ID is not a valid integer or is out of bounds for the specified shard kind.
     */
    protected int readShardId(ShardKind shardKind, ByteBuf shardIdBuf) {
        String rawShardId = ByteBufUtils.readAsString(shardIdBuf);
        return readShardId(shardKind, rawShardId);
    }

    /**
     * Reads and validates the shard ID based on the provided shard kind and raw shard ID string.
     *
     * @param shardKind  The type of shard whose ID is being read. Must be of type {@link ShardKind}.
     * @param rawShardId A string representation of the shard ID.
     * @return The validated shard ID as an integer.
     * @throws InvalidShardIdException if the shard ID is not a valid integer or is out of bounds for the specified shard kind.
     */
    protected int readShardId(ShardKind shardKind, String rawShardId) {
        try {
            int shardId = Integer.parseInt(rawShardId);
            // Validate
            if (shardId < 0 || shardId > getNumberOfShards(shardKind) - 1) {
                throw new InvalidShardIdException();
            }
            return shardId;
        } catch (NumberFormatException e) {
            throw new InvalidShardIdException();
        }
    }

    /**
     * Converts a {@link Member} object into a series of key-value pairs represented as {@link RedisMessage}
     * instances, and populates the provided map with these pairs.
     *
     * @param member  The {@link Member} object whose details are to be converted into Redis message key-value pairs.
     * @param current The map to store the converted key-value pairs, where both keys and values are of type {@link RedisMessage}.
     */
    protected void memberToRedisMessage(Member member, Map<RedisMessage, RedisMessage> current) {
        current.put(new SimpleStringRedisMessage("status"), new SimpleStringRedisMessage(member.getStatus().toString()));

        String processId = VersionstampUtils.base32HexEncode(member.getProcessId());
        current.put(new SimpleStringRedisMessage("process_id"), new SimpleStringRedisMessage(processId));

        current.put(new SimpleStringRedisMessage("external_host"), new SimpleStringRedisMessage(member.getExternalAddress().getHost()));
        current.put(new SimpleStringRedisMessage("external_port"), new IntegerRedisMessage(member.getExternalAddress().getPort()));

        current.put(new SimpleStringRedisMessage("internal_host"), new SimpleStringRedisMessage(member.getInternalAddress().getHost()));
        current.put(new SimpleStringRedisMessage("internal_port"), new IntegerRedisMessage(member.getInternalAddress().getPort()));

        long latestHeartbeat = membership.getLatestHeartbeat(member);
        current.put(new SimpleStringRedisMessage("latest_heartbeat"), new IntegerRedisMessage(latestHeartbeat));
    }
}
