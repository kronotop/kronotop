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

package com.kronotop.server;

import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.network.Address;
import com.kronotop.server.resp3.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;

/**
 * This abstract class represents a RESP2 server that implements the KronotopService interface.
 * It provides the basic functionality for starting and shutting down a server.
 */
public abstract class RESP2Server implements KronotopService {
    private final Handlers commands;
    private final EventLoopGroup parentGroup;
    private final EventLoopGroup childGroup;
    private final Context context;
    private final Class<? extends ServerSocketChannel> channel;
    private ChannelFuture channelFuture;

    public RESP2Server(Context context, Handlers commands, Class<? extends ServerSocketChannel> channel, EventLoopGroup parentGroup, EventLoopGroup childGroup) {
        this.commands = commands;
        this.context = context;
        this.parentGroup = parentGroup;
        this.childGroup = childGroup;
        this.channel = channel;
    }

    public void start(Member member) throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(parentGroup, childGroup)
                .channel(channel)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new RedisDecoder());
                        p.addLast(new RedisBulkStringAggregator());
                        p.addLast(new RedisArrayAggregator());
                        p.addLast(new RedisMapAggregator());
                        p.addLast(new RedisEncoder());
                        p.addLast(new Router(context, commands));
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 1 << 9)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        Address address = member.getAddress();
        channelFuture = b.bind(address.getHost(), address.getPort()).sync();
    }

    @Override
    public String getName() {
        return "RESP2";
    }

    @Override
    public Context getContext() {
        return context;
    }

    public void shutdown() {
        if (channelFuture != null) {
            channelFuture.channel().close();
        }
        childGroup.shutdownGracefully();
        parentGroup.shutdownGracefully();
    }
}
