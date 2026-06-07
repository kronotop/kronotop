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
import com.kronotop.KronotopService;
import com.kronotop.network.Address;
import com.kronotop.server.resp3.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;

/**
 * Abstract RESP server with optional one-way TLS support.
 */
public abstract class RESPServer implements KronotopService {
    private final CommandHandlerRegistry commands;
    private final EventLoopGroup parentGroup;
    private final EventLoopGroup childGroup;
    private final Context context;
    private final Class<? extends ServerSocketChannel> channel;
    private final TLSConfig tlsConfig;
    private final NettyConfig nettyConfig;
    private final ServerKind serverKind;
    private ChannelFuture channelFuture;

    public RESPServer(
            Context context,
            CommandHandlerRegistry commands,
            TLSConfig tlsConfig,
            NettyConfig nettyConfig,
            Class<? extends ServerSocketChannel> channel,
            EventLoopGroup parentGroup,
            EventLoopGroup childGroup,
            ServerKind serverKind
    ) {
        this.commands = commands;
        this.context = context;
        this.serverKind = serverKind;
        this.tlsConfig = tlsConfig;
        this.nettyConfig = nettyConfig;
        this.parentGroup = parentGroup;
        this.childGroup = childGroup;
        this.channel = channel;
    }

    private SslContext buildSslContext() throws SSLException {
        return SslContextBuilder.forServer(tlsConfig.certFile(), tlsConfig.keyFile()).build();
    }

    public void start(Address address) throws InterruptedException {
        final SslContext sslContext;
        if (tlsConfig.enabled()) {
            try {
                sslContext = buildSslContext();
            } catch (SSLException e) {
                throw new RuntimeException("Failed to initialize TLS for RESP server", e);
            }
        } else {
            sslContext = null;
        }

        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
                .channel(channel)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (sslContext != null) {
                            p.addLast(sslContext.newHandler(ch.alloc()));
                        }
                        p.addLast(new RedisDecoder());
                        p.addLast(new RedisBulkStringAggregator());
                        p.addLast(new RedisArrayAggregator());
                        p.addLast(new RedisMapAggregator());
                        p.addLast(new RedisEncoder());
                        p.addLast(new KronotopChannelDuplexHandler(context, commands, serverKind));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, nettyConfig.soBacklog())
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        configureBootstrap(b);

        channelFuture = b.bind(address.getHost(), address.getPort()).sync();
    }

    @Override
    public String getName() {
        return "RESP";
    }

    @Override
    public Context getContext() {
        return context;
    }

    /**
     * Hook for subclasses to apply transport-specific bootstrap options.
     */
    protected void configureBootstrap(ServerBootstrap b) {
        // No-op by default
    }

    public void shutdown() {
        if (channelFuture != null) {
            channelFuture.channel().close();
        }
        childGroup.shutdownGracefully();
        parentGroup.shutdownGracefully();
    }
}
