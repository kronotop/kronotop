/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.typesafe.config.Config;

/**
 * Netty tuning parameters for RESP servers.
 *
 * @param workerThreads number of worker threads (0 = Netty default)
 * @param soBacklog     SO_BACKLOG value for the server socket
 * @param soReusePort   whether to enable SO_REUSEPORT (epoll only)
 */
public record NettyConfig(int workerThreads, int soBacklog, boolean soReusePort) {

    /**
     * Reads Netty tuning parameters from the application config for the given server kind.
     */
    public static NettyConfig fromConfig(Config config, ServerKind kind) {
        String prefix = String.format("network.%s.netty", kind.toString().toLowerCase());
        int workerThreads = config.getInt(prefix + ".worker_threads");
        int soBacklog = config.getInt(prefix + ".so_backlog");
        boolean soReusePort = config.getBoolean(prefix + ".so_reuseport");
        return new NettyConfig(workerThreads, soBacklog, soReusePort);
    }
}
