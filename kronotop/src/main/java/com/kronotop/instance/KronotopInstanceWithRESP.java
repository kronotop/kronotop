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

package com.kronotop.instance;

import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.KronotopService;
import com.kronotop.network.Address;
import com.kronotop.server.*;

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
    private static final String NETTY_TRANSPORT_NIO = "nio";
    private static final String NETTY_TRANSPORT_EPOLL = "epoll";
    private static final String DEFAULT_NETTY_TRANSPORT = NETTY_TRANSPORT_NIO;

    public KronotopInstanceWithRESP() {
        super();
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
        String nettyTransport = DEFAULT_NETTY_TRANSPORT;

        String transportConfigPath = String.format("network.%s.netty.transport", kind.toString().toLowerCase());
        if (config.hasPath(transportConfigPath)) {
            nettyTransport = config.getString(transportConfigPath);
        }

        CommandHandlerRegistry handlers = context.getHandlers(kind);
        RESPServer server;
        if (nettyTransport.equals(NETTY_TRANSPORT_NIO)) {
            server = new NioRESPServer(context, handlers);
        } else if (nettyTransport.equals(NETTY_TRANSPORT_EPOLL)) {
            server = new EpollRESPServer(context, handlers);
        } else {
            throw new KronotopException(String.format("invalid %s: %s", transportConfigPath, nettyTransport));
        }
        return server;
    }

    @Override
    public void start() throws UnknownHostException, InterruptedException {
        super.start();

        RESPServerContainer container = new RESPServerContainer();
        container.register(member.getInternalAddress(), initializeRESPServer(ServerKind.INTERNAL));
        container.register(member.getExternalAddress(), initializeRESPServer(ServerKind.EXTERNAL));

        context.registerService(container.getName(), container);
        container.start();
    }

    /**
     * RESPServerContainer is a private static class that manages a collection of RESPServer instances associated
     * with specific addresses. This class implements the KronotopService interface, allowing it to integrate
     * within the Kronotop service lifecycle.
     */
    private static class RESPServerContainer implements KronotopService {
        private final Map<Address, RESPServer> servers = new LinkedHashMap<>();

        @Override
        public String getName() {
            return "RESPServerContainer";
        }

        protected void register(Address address, RESPServer server) {
            servers.put(address, server);
        }

        @Override
        public Context getContext() {
            throw new UnsupportedOperationException();
        }

        protected void start() throws InterruptedException {
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
