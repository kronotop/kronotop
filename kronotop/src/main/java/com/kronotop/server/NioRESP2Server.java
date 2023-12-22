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
import com.kronotop.server.resp.Handlers;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * The NioRESP2Server class represents a RESP2 server that uses NIO for network transport.
 * It extends the RESP2Server class.
 *
 * @see RESP2Server
 */
public class NioRESP2Server extends RESP2Server {
    public NioRESP2Server(Context context, Handlers commands) {
        super(context, commands, NioServerSocketChannel.class, new NioEventLoopGroup(), new NioEventLoopGroup());
    }
}
