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

package com.kronotop.volume.replication;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.volume.VolumeConfig;
import com.kronotop.volume.VolumeConfigGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentHashMap;

public class ReplicationService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Replication";
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);

    private final RoutingService routing;
    private final ConcurrentHashMap<Versionstamp, Replication> replications = new ConcurrentHashMap<>();

    public ReplicationService(Context context) {
        super(context, NAME);

        this.routing = context.getService(RoutingService.NAME);
        this.routing.registerHook(RoutingEventKind.CREATE_REPLICATION_SLOT, new CreateReplicationSlotHook(context));
        this.routing.registerHook(RoutingEventKind.STOP_REPLICATION, new StopReplicationHook(context));
        this.routing.registerHook(RoutingEventKind.PRIMARY_OWNER_CHANGED, new PrimaryOwnerChangedHook(context));
    }

    private void startReplicationTasks(ShardKind shardKind, int shards) {
        for (int shardId = 0; shardId < shards; shardId++) {
            Route route = routing.findRoute(shardKind, shardId);
            if (route == null) {
                // Not assigned yet
                continue;
            }

            if (route.standbys().contains(context.getMember())) {
                VolumeConfigGenerator generator = new VolumeConfigGenerator(context, shardKind, shardId);
                VolumeConfig volumeConfig = generator.volumeConfig();
                ReplicationConfig replicationConfig = new ReplicationConfig(volumeConfig, shardKind, shardId, ReplicationStage.SNAPSHOT);
                Versionstamp slotId = ReplicationMetadata.findSlotId(context, replicationConfig);
                if (slotId == null) {
                    LOGGER.warn("Replication slot not found for ShardKind: {} ShardId: {}", shardKind, shardId);
                    continue;
                }

                try (Transaction tr = context.getFoundationDB().createTransaction()) {
                    ReplicationSlot slot = ReplicationSlot.load(tr, replicationConfig, slotId);
                    if (slot.isStale()) {
                        LOGGER.warn("Replication slot: {} is stale for ShardKind: {} ShardId: {}", VersionstampUtil.base32HexEncode(slotId), shardKind, shardId);
                        continue;
                    }
                }

                Replication replication = newReplication(slotId, replicationConfig);
                try {
                    replication.start();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    public Replication newReplication(Versionstamp slotId, ReplicationConfig replicationConfig) {
        return replications.computeIfAbsent(slotId,
                (versionstamp) -> new Replication(context, versionstamp, replicationConfig)
        );
    }

    public void start() {
        for (ShardKind shardKind : ShardKind.values()) {
            if (shardKind.equals(ShardKind.REDIS)) {
                int shards = context.getConfig().getInt("redis.shards");
                startReplicationTasks(shardKind, shards);
            } else if (shardKind.equals(ShardKind.BUCKET)) {
                // TODO: BUCKET-IMPLEMENTATION
            } else {
                throw new IllegalArgumentException("Unsupported shard kind: " + shardKind);
            }
        }
    }

    protected synchronized void stopReplication(Versionstamp slotId) {
        Replication replication = replications.get(slotId);
        if (replication != null) {
            replication.stop();
        }
        replications.remove(slotId);
    }

    protected Replication getReplication(Versionstamp slotId) {
        return replications.get(slotId);
    }

    @Override
    public void shutdown() {
        for (Replication replication : replications.values()) {
            replication.stop();
        }
    }
}
