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

package com.kronotop.instance;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.network.Address;
import com.kronotop.network.NetworkConfigUtil;
import com.kronotop.server.*;
import com.typesafe.config.Config;

import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The KronotopInstanceWithRESP class represents a running instance of Kronotop with RESP support.
 * It extends the KronotopInstance class.
 *
 * @see KronotopInstance
 */
public class KronotopInstanceWithRESP extends KronotopInstance {
    public KronotopInstanceWithRESP(Config config) {
        super(config);
    }

    /**
     * Initializes and returns a RESP server of the specified kind, using the appropriate network transport
     * method based on configuration.
     *
     * @param kind The kind of server to initialize, typically either INTERNAL or EXTERNAL.
     * @return The initialized RESPServer instance.
     * @throws KronotopException If an invalid network transport method is specified in the configuration.
     */
    private RESPServer initializeRESPServer(ServerKind kind) {
        checkNotNull(member);
        String nettyTransport = NetworkConfigUtil.getNettyTransport(config, kind.toString().toLowerCase());

        CommandHandlerRegistry handlers = context.getHandlers(kind);
        TLSConfig tlsConfig = TLSConfig.fromConfig(config, kind);
        NettyConfig nettyConfig = NettyConfig.fromConfig(config, kind);
        if (nettyTransport.equals(NetworkConfigUtil.NETTY_TRANSPORT_EPOLL)) {
            return new EpollRESPServer(context, handlers, tlsConfig, nettyConfig, kind);
        }
        return new NioRESPServer(context, handlers, tlsConfig, nettyConfig, kind);
    }

    @Override
    public void start() throws UnknownHostException, InterruptedException {
        super.start();

        // Register internal FIRST -> shuts down LAST
        RESPServerContainer internalServer = new RESPServerContainer("InternalRESPServer");
        internalServer.register(member.getInternalAddress(), initializeRESPServer(ServerKind.INTERNAL));
        context.registerService(internalServer.getName(), internalServer);
        internalServer.start();

        // Register external SECOND -> shuts down FIRST
        RESPServerContainer externalServer = new RESPServerContainer("ExternalRESPServer");
        externalServer.register(member.getExternalAddress(), initializeRESPServer(ServerKind.EXTERNAL));
        context.registerService(externalServer.getName(), externalServer);
        externalServer.start();
    }

    /**
     * RESPServerContainer is a private static class that manages a collection of RESPServer instances associated
     * with specific addresses. This class implements the KronotopService interface, allowing it to integrate
     * within the Kronotop service lifecycle.
     */
    private static class RESPServerContainer implements KronotopService {
        private final Map<Address, RESPServer> servers = new LinkedHashMap<>();
        private final String name;

        RESPServerContainer(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        protected void register(Address address, RESPServer server) {
            servers.put(address, server);
        }

        @Override
        public Context getContext() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void start() {
            for (Map.Entry<Address, RESPServer> entry : servers.entrySet()) {
                entry.getValue().start(entry.getKey());
            }
        }

        @Override
        public void shutdown() {
            for (RESPServer server : servers.values()) {
                server.shutdown();
            }
        }
    }
}
