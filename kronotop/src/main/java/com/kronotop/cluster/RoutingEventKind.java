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

package com.kronotop.cluster;

/**
 * Defines the types of routing events that can occur in a Kronotop cluster
 * These events are used to manage and coordinate various operations such as
 * data initialization, shard ownership transitions, and replication processes.
 */
public enum RoutingEventKind {
    /**
     * Represents the event triggered to load or initialize data associated
     * with a specific Redis shard. This event is typically used in
     * a Kronotop cluster to prepare a shard for usage by loading state
     * or configuration from storage, ensuring it is ready to handle
     * operations effectively.
     */
    LOAD_REDIS_SHARD,

    /**
     * Represents the event triggered to initialize a bucket shard.
     * This type of event is typically used to prepare a specific shard for operation.
     * The initialization process may involve setting up data structures, loading configuration,
     * or ensuring the shard is ready to handle requests efficiently.
     */
    INITIALIZE_BUCKET_SHARD,

    /**
     * Represents the event triggered to create a replication slot.
     * This slot ensures that replication streams are maintained effectively
     * in a Kronotop cluster by coordinating data transfer between nodes,
     * enabling fault tolerance and consistency in the cluster.
     */
    CREATE_REPLICATION_SLOT,

    /**
     * Represents the event triggered to terminate or stop the replication process
     * between nodes in a distributed system or cluster. This event is used
     * to halt the data synchronization activities, which may be necessary
     * during reconfiguration, maintenance, or shutdown operations.
     */
    STOP_REPLICATION,

    /**
     * Represents the event triggered when the primary owner of a shard changes.
     * This event is typically used in a Kronotop cluster to handle transitions of ownership,
     * ensuring that updates to the primary owner are consistently propagated and operations
     * are routed correctly.
     */
    PRIMARY_OWNER_CHANGED,

    /**
     * Represents an event where the ownership of a shard is handed over
     * to another cluster member. This is typically triggered during
     * cluster rebalancing or failover scenarios to ensure continuity
     * and load distribution across the cluster.
     */
    HAND_OVER_SHARD_OWNERSHIP
}
