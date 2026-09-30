/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.server;

import com.kronotop.metrics.RuntimeMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

public class NettyTrafficCounter extends ChannelDuplexHandler {
    private final RuntimeMetrics runtimeMetrics;
    private final ServerKind serverKind;

    public NettyTrafficCounter(ServerKind serverKind, RuntimeMetrics runtimeMetrics) {
        this.serverKind = serverKind;
        this.runtimeMetrics = runtimeMetrics;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
        if (message instanceof ByteBuf msg) {
            runtimeMetrics.getNetworkMetrics().increaseReadBytes(serverKind, msg.readableBytes());
        }
        super.channelRead(ctx, message);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object message, ChannelPromise promise) throws Exception {
        if (message instanceof ByteBuf msg) {
            runtimeMetrics.getNetworkMetrics().increaseWrittenBytes(serverKind, msg.readableBytes());
        }
        super.write(ctx, message, promise);
    }
}
