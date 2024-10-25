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
import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.*;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.*;

public class BaseKrAdminSubcommandHandler {
    protected final MembershipService service;

    public BaseKrAdminSubcommandHandler(MembershipService service) {
        this.service = service;
    }

    /**
     * Opens a shard subspace within the metadata directory subspace for the specified shard ID.
     *
     * @param tr       The transaction to use for the operation.
     * @param metadata The DirectorySubspace representing the metadata.
     * @param shardId  The ID of the shard to open.
     * @return The DirectorySubspace representing the opened shard subspace.
     */
    protected DirectorySubspace openShardSubspace(Transaction tr, DirectorySubspace metadata, ShardKind shardKind, int shardId) {
        KronotopDirectoryNode directory;
        if (shardKind.equals(ShardKind.REDIS)) {
            directory = KronotopDirectory.
                    kronotop().
                    cluster(service.getContext().getClusterName()).
                    metadata().
                    shards().
                    redis().
                    shard(shardId);
        } else {
            throw new KronotopException("Unsupported shard kind: " + shardKind);
        }
        return metadata.open(tr, directory.excludeSubspace(metadata)).join();
    }

    /**
     * Reads the member ID from the provided buffer, validates it, and returns it as a string.
     *
     * @param memberIdBuf the buffer containing the raw member ID bytes
     * @return the validated member ID as a string
     * @throws KronotopException if the member ID is invalid or cannot be parsed as a UUID
     */
    protected String readMemberId(ByteBuf memberIdBuf) {
        String memberId = readAsString(memberIdBuf);
        try {
            // Validate the member id.
            return UUID.fromString(memberId).toString();
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid memberId: " + memberId);
        }
    }

    /**
     * Reads the content of the provided ByteBuf as a string.
     *
     * @param buf the ByteBuf containing the raw bytes to be read
     * @return a string representation of the bytes in the provided ByteBuf
     */
    protected String readAsString(ByteBuf buf) {
        byte[] raw = new byte[buf.readableBytes()];
        buf.readBytes(raw);
        return new String(raw);
    }

    /**
     * Converts a given Member object into a Map of RedisMessage key-value pairs.
     *
     * @param member the Member object to be converted
     * @return a Map containing RedisMessage key-value pairs representing the attributes of the Member object
     */
    protected Map<RedisMessage, RedisMessage> memberToRedisMessage(Member member) {
        Map<RedisMessage, RedisMessage> current = new LinkedHashMap<>();

        current.put(new SimpleStringRedisMessage("status"), new SimpleStringRedisMessage(member.getStatus().toString()));

        String processId = VersionstampUtils.base64Encode(member.getProcessId());
        current.put(new SimpleStringRedisMessage("process_id"), new SimpleStringRedisMessage(processId));

        current.put(new SimpleStringRedisMessage("external_host"), new SimpleStringRedisMessage(member.getExternalAddress().getHost()));
        current.put(new SimpleStringRedisMessage("external_port"), new IntegerRedisMessage(member.getExternalAddress().getPort()));

        current.put(new SimpleStringRedisMessage("internal_host"), new SimpleStringRedisMessage(member.getInternalAddress().getHost()));
        current.put(new SimpleStringRedisMessage("internal_port"), new IntegerRedisMessage(member.getInternalAddress().getPort()));

        long latestHeartbeat = service.getLatestHeartbeat(member);
        current.put(new SimpleStringRedisMessage("latest_heartbeat"), new IntegerRedisMessage(latestHeartbeat));

        return current;
    }

    /**
     * Checks if the cluster has already been initialized.
     *
     * @param tr       The transaction used to read from the database.
     * @param subspace The directory subspace containing the initialization state.
     * @return A boolean indicating whether the cluster is initialized (true) or not (false).
     */
    protected boolean isClusterInitialized(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(Tuple.from(MembershipConstants.CLUSTER_INITIALIZED));
        return MembershipUtils.isTrue(tr.get(key).join());
    }

    /**
     * Loads the primary member ID for a shard from the specified subspace within a transaction.
     *
     * @param tr       The transaction used to read from the database.
     * @param subspace The specific directory subspace containing the primary member information.
     * @return The primary member ID as a string, or null if no primary member information is found.
     */
    protected String loadShardPrimary(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(Tuple.from("ROUTE_PRIMARY"));
        return tr.get(key).thenApply((value) -> {
            if (value == null) {
                return null;
            }
            return new String(value);
        }).join();
    }

    /**
     * Loads the standby members for a shard from the specified subspace within a transaction.
     *
     * @param tr       The transaction used to read from the database.
     * @param subspace The specific directory subspace containing the standby information.
     * @return A list of standby member IDs as strings, or null if no standby information is found.
     */
    private List<String> loadShardStandbys(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(Tuple.from("ROUTE_STANDBYS"));
        return tr.get(key).thenApply((value) -> {
            if (value == null) {
                return null;
            }
            return Arrays.asList(JSONUtils.readValue(value, String[].class));
        }).join();
    }

    /**
     * Loads the shard routing information from the specified subspace within a transaction.
     *
     * @param tr       The transaction used to read from the database.
     * @param subspace The specific directory subspace containing the route information.
     * @return A {@link Route} object containing the primary and standby members,
     * or null if no route information is found.
     */
    protected Route loadRoute(Transaction tr, DirectorySubspace subspace) {
        String primaryMemberId = loadShardPrimary(tr, subspace);
        if (primaryMemberId == null) {
            // No route set
            return null;
        }

        Member primary = service.findMember(primaryMemberId);
        List<String> standbyIds = loadShardStandbys(tr, subspace);
        if (standbyIds == null) {
            return new Route(primary);
        }

        List<Member> standbys = new ArrayList<>();
        for (String standbyId : standbyIds) {
            Member standby = service.findMember(standbyId);
            standbys.add(standby);
        }

        return new Route(primary, standbys);
    }
}
