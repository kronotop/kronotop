/*
 * Copyright (c) 2023 Kronotop
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

import com.kronotop.common.KronotopException;
import com.kronotop.server.EpollRESP2Server;
import com.kronotop.server.NioRESP2Server;
import com.kronotop.server.RESP2Server;
import com.typesafe.config.Config;

import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The KronotopInstanceWithRESP2 class represents a running instance of Kronotop with RESP2 support.
 * It extends the KronotopInstance class.
 *
 * @see KronotopInstance
 */
public class KronotopInstanceWithRESP2 extends KronotopInstance {
    private static final String NETTY_TRANSPORT_NIO = "nio";
    private static final String NETTY_TRANSPORT_EPOLL = "epoll";
    private static final String DEFAULT_NETTY_TRANSPORT = NETTY_TRANSPORT_NIO;

    public KronotopInstanceWithRESP2() {
        super();
    }

    public KronotopInstanceWithRESP2(Config config) {
        super(config);
    }

    /**
     * Starts the TCP server for handling RESP2 requests.
     * The server is started based on the configured network transport.
     * If the network transport is not specified, the default is used.
     * The server is registered as a service in the context and started.
     *
     * @throws InterruptedException if the thread is interrupted while starting the server
     * @throws KronotopException    if an invalid network transport is specified in the configuration
     */
    private void startTCPServer() throws InterruptedException {
        checkNotNull(member);
        String nettyTransport = DEFAULT_NETTY_TRANSPORT;
        if (config.hasPath("network.netty.transport")) {
            nettyTransport = config.getString("network.netty.transport");
        }

        RESP2Server server;
        if (nettyTransport.equals(NETTY_TRANSPORT_NIO)) {
            server = new NioRESP2Server(context, handlers);
        } else if (nettyTransport.equals(NETTY_TRANSPORT_EPOLL)) {
            server = new EpollRESP2Server(context, handlers);
        } else {
            throw new KronotopException(String.format("invalid network.netty.transport: %s", nettyTransport));
        }

        context.registerService(server.getName(), server);
        server.start(member);
    }

    @Override
    public void start() throws UnknownHostException, InterruptedException {
        super.start();
        startTCPServer();
    }
}
