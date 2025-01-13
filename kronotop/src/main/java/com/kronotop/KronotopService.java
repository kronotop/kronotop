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
 * KronotopService is an interface that represents a service in the Kronotop database system.
 * It provides methods to get the service name, the global context, and to get shutdown the service.
 */
public interface KronotopService {
    /**
     * Retrieves the name of the service.
     *
     * @return the name of the service
     */
    String getName();

    /**
     * Retrieves the context associated with the KronotopService.
     *
     * @return the context associated with the KronotopService
     */
    Context getContext();

    /**
     * Shuts down the Kronotop instance.
     */
    void shutdown();
}