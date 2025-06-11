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

package com.kronotop.redis.server;

import com.kronotop.server.Request;
import com.kronotop.server.Response;

/**
 * Represents a handler for subcommands in a Redis-like protocol system.
 * Implementations of this interface handle specific subcommands by providing
 * their logic to process requests and generate corresponding responses.
 */
public interface SubcommandHandler {
    /**
     * Determines whether the implementation requires the cluster to be initialized
     * before executing this subcommand handler.
     *
     * @return true if the cluster must be initialized, otherwise false
     */
    default boolean requiresClusterInitialization() {
        return true;
    }

    /**
     * Executes the logic of the subcommand using the provided request and response objects.
     * This method processes the input command and parameters from the request
     * and writes the appropriate response back to the client.
     *
     * @param request the request object containing the command, parameters, and session details
     * @param response the response object used to send responses back to the client
     */
    void execute(Request request, Response response);
}

