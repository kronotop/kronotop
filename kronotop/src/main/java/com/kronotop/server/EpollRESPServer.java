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

import com.kronotop.Context;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;

/**
 * The EpollRESPServer class represents a RESP server that uses the Epoll network transport.
 * It extends the RESPServer abstract class and implements the KronotopService interface.
 */
public class EpollRESPServer extends RESPServer {
    public EpollRESPServer(Context context, Handlers commands) {
        super(context, commands, EpollServerSocketChannel.class, new EpollEventLoopGroup(), new EpollEventLoopGroup());
    }
}
