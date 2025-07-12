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

/**
 * The ShardOwnerService class is a generic service designed to manage shard ownership
 * and facilitate interactions with those shards through a defined service context. It extends the
 * functionality of CommandHandlerService, inheriting its core features while adding shard-specific
 * operations that are crucial in distributed systems or clustered environments.
 *
 * @param <T> the type of resource or object managed by the service context
 */
public class ShardOwnerService<T> extends CommandHandlerService {
    private final ServiceContext<T> serviceContext;

    public ShardOwnerService(Context context, String name) {
        super(context, name);

        this.serviceContext = context.getServiceContext(name);
    }

    public ServiceContext<T> getServiceContext() {
        return serviceContext;
    }
}
