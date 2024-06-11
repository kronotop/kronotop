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

package com.kronotop.server;

import java.util.Collections;
import java.util.List;

/**
 * The Handler interface represents a handler for processing Redis requests.
 */
public interface Handler {
    /**
     * Checks if the handler is watchable.
     *
     * @return true if the handler is watchable, false otherwise
     */
    default boolean isWatchable() {
        return false;
    }

    /**
     * Retrieves the list of keys associated with a given request.
     *
     * @param request the Redis request object
     * @return the list of keys associated with the request
     */
    default List<String> getKeys(Request request) {
        return Collections.emptyList();
    }

    /**
     * Executes the necessary operations before executing a Redis request.
     *
     * @param request the Redis request object
     */
    void beforeExecute(Request request);

    /**
     * Executes the given Redis request.
     * <p>
     * This method is used to execute a Redis request. It takes a Request object and a Response object as parameters.
     * The response object is used to write the response messages back to the client.
     *
     * @param request  the Redis request object to be executed
     * @param response the Response object used to write response messages back to the client
     * @throws Exception if an error occurs during execution
     */
    void execute(Request request, Response response) throws Exception;
}
