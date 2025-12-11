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
 * Routing events that trigger hooks during cluster topology changes.
 *
 * <p>These events coordinate shard initialization, replication management, and ownership
 * transitions. Services register hooks via {@code RoutingService.registerHook()} to respond
 * to specific events.
 */
public enum RoutingEventKind {
    /**
     * Triggered when this member becomes the primary owner of a Redis shard.
     * Loads shard state from local storage to prepare for serving requests.
     */
    LOAD_REDIS_SHARD,

    /**
     * Triggered when this member becomes the primary owner of a Bucket shard.
     * Initializes data structures and loads configuration for document operations.
     */
    INITIALIZE_BUCKET_SHARD,

    /**
     * Triggered when this member is assigned as a standby for a shard.
     * Initiates replication from the primary owner to maintain data consistency.
     */
    START_REPLICATION,

    /**
     * Triggered when this member is removed from a shard's standby list.
     * Terminates the replication stream and cleans up associated resources.
     */
    STOP_REPLICATION,

    /**
     * Triggered on standby members when the shard's primary owner changes.
     * Reconnects replication to the new primary to continue data synchronization.
     */
    PRIMARY_OWNER_CHANGED,

    /**
     * Triggered on the previous primary when ownership transfers to another member.
     * Performs cleanup and graceful handoff of shard responsibilities.
     */
    HAND_OVER_SHARD_OWNERSHIP
}
