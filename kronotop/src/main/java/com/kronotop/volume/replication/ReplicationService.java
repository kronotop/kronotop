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

package com.kronotop.volume.replication;

import com.kronotop.BaseKronotopService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.cluster.RoutingEventKind;
import com.kronotop.cluster.RoutingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicationService extends BaseKronotopService implements KronotopService {
    public static final String NAME = "Replication";
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationService.class);

    public ReplicationService(Context context) {
        super(context, NAME);

        RoutingService routing = context.getService(RoutingService.NAME);
        routing.registerHook(RoutingEventKind.CREATE_REPLICATION_SLOT, new CreateReplicationSlotHook(context));
    }

    public void start() {

    }
}
