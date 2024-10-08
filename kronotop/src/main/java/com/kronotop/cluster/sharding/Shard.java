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

package com.kronotop.cluster.sharding;

import java.util.concurrent.atomic.AtomicReference;

public interface Shard {

    AtomicReference<ShardStatus> status = new AtomicReference<>(ShardStatus.INOPERABLE);

    default ShardStatus status() {
        return this.status.get();
    }

    default void setStatus(ShardStatus status) {
        this.status.set(status);
    }

    ShardLocator getShardLocator();

    ShardStatistics getShardStatistics();

    Integer id();

    void start();

    void shutdown();
}
