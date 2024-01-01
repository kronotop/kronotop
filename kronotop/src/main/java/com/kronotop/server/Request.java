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

import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeMap;

import java.util.ArrayList;

/**
 * The Request interface represents a request in the Redis protocol.
 * It extends the AttributeMap interface for attribute management.
 */
public interface Request extends AttributeMap {
    /**
     * Retrieves the command associated with the Request.
     *
     * @return the command associated with the Request
     */
    String getCommand();

    /**
     * Retrieves the list of parameters associated with the Request.
     *
     * @return the list of parameters as an ArrayList of ByteBuf objects
     */
    ArrayList<ByteBuf> getParams();

    /**
     * Retrieves the Redis message associated with the Request.
     *
     * @return the RedisMessage object associated with the Request
     */
    RedisMessage getRedisMessage();

    /**
     * Retrieves the ChannelHandlerContext associated with the Request.
     *
     * @return the ChannelHandlerContext associated with the Request
     */
    ChannelHandlerContext getContext();
}
