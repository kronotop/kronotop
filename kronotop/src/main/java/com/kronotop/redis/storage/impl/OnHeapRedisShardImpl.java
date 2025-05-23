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

package com.kronotop.redis.storage.impl;

import com.kronotop.Context;
import com.kronotop.redis.storage.RedisShard;

/**
 * This class represents an on-heap Redis shard implementation that extends the AbstractRedisShard class and implements the Shard interface.
 * It provides a concurrent map with additional functionality.
 */
public class OnHeapRedisShardImpl extends AbstractRedisShard implements RedisShard {
    public OnHeapRedisShardImpl(Context context, Integer id) {
        super(context, id);
    }
}
