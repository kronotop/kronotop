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

import com.kronotop.Context;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * RESP server using NIO transport with optional TLS support.
 *
 * @see RESPServer
 */
public class NioRESPServer extends RESPServer {
    public NioRESPServer(Context context, CommandHandlerRegistry registry, TLSConfig tlsConfig, NettyConfig nettyConfig, ServerKind serverKind) {
        super(context, registry, tlsConfig, nettyConfig, NioServerSocketChannel.class,
                new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory()),
                nettyConfig.workerThreads() > 0
                        ? new MultiThreadIoEventLoopGroup(nettyConfig.workerThreads(), NioIoHandler.newFactory())
                        : new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory()),
                serverKind
        );
    }
}
