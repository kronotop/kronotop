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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.epoll.EpollServerSocketChannel;

/**
 * RESP server using Epoll transport with optional TLS support.
 */
public class EpollRESPServer extends RESPServer {
    private final NettyConfig nettyConfig;

    public EpollRESPServer(Context context, CommandHandlerRegistry commands, TLSConfig tlsConfig, NettyConfig nettyConfig, ServerKind serverKind) {
        super(context, commands, tlsConfig, nettyConfig, EpollServerSocketChannel.class,
                new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory()),
                nettyConfig.workerThreads() > 0
                        ? new MultiThreadIoEventLoopGroup(nettyConfig.workerThreads(), EpollIoHandler.newFactory())
                        : new MultiThreadIoEventLoopGroup(EpollIoHandler.newFactory()),
                serverKind
        );
        this.nettyConfig = nettyConfig;
    }

    @Override
    protected void configureBootstrap(ServerBootstrap b) {
        if (nettyConfig.soReusePort()) {
            b.option(EpollChannelOption.SO_REUSEPORT, true);
        }
    }
}
