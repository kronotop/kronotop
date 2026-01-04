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

package com.kronotop;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Service-specific context interface for managing shard-level state within a Kronotop service.
 * Each service (e.g., BucketService, RedisService) implements this interface to maintain
 * isolated, shard-partitioned state while retaining access to the root application context.
 *
 * @param <T> the type of shard-specific state managed by this service context
 */
public interface ServiceContext<T> {

    /**
     * Returns the map of shard states indexed by shard ID.
     *
     * @return concurrent map of shard ID to shard state
     */
    ConcurrentHashMap<Integer, T> shards();

    /**
     * Returns the root application context.
     *
     * @return the root {@link Context}
     */
    Context root();
}
