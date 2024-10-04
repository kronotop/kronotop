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

package com.kronotop.cluster.coordinator;

import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.cluster.BroadcastEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumMap;

public class CoordinatorService implements KronotopService {
    public static final String NAME = "Coordinator";
    private static final Logger LOGGER = LoggerFactory.getLogger(CoordinatorService.class);
    private final Context context;
    private final EnumMap<Coordinator.Service, Coordinator> coordinators = new EnumMap<>(Coordinator.Service.class);

    public CoordinatorService(Context context) {
        this.context = context;
    }

    public void submitEvent(BroadcastEvent event) {
        coordinators.forEach((service, value) -> {
            LOGGER.debug("Submitting broadcast event for {}: {}", service, event.kind());
            value.submit(event);
        });
    }

    public void register(Coordinator.Service name, Coordinator coordinator) {
        coordinators.put(name, coordinator);
    }

    public void start() {
        coordinators.forEach((service, coordinator) -> {
            coordinator.start();
        });
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {
        coordinators.values().forEach(Coordinator::shutdown);
    }
}
