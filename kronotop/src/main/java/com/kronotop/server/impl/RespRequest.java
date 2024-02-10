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

package com.kronotop.server.impl;

import com.kronotop.common.Preconditions;
import com.kronotop.server.Request;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.util.CharsetUtil;
import io.netty.util.DefaultAttributeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * RespRequest represents a request in the Redis protocol.
 * It extends the DefaultAttributeMap class for attribute management and implements the Request interface.
 */
public class RespRequest extends DefaultAttributeMap implements Request {
    private final RedisMessage msg;
    private final ChannelHandlerContext ctx;
    private ArrayList<ByteBuf> params;
    private String command;

    public RespRequest(ChannelHandlerContext ctx, Object msg) {
        this.msg = (RedisMessage) msg;
        this.ctx = ctx;
    }

    /**
     * Retrieves the Redis command from the RedisMessage.
     *
     * @return the Redis command as a string
     * @throws CodecException if the RedisMessage is unknown or corrupt
     */
    public String getCommand() {
        if (command != null) {
            return command;
        }

        Preconditions.checkNotNull(msg, "RedisMessage cannot be null");
        if (msg instanceof ArrayRedisMessage) {
            RedisMessage redisMessage = ((ArrayRedisMessage) msg).children().get(0);
            if (redisMessage instanceof FullBulkStringRedisMessage) {
                command = ((FullBulkStringRedisMessage) redisMessage).content().toString(CharsetUtil.US_ASCII).toUpperCase();
                return command;
            }
        } else if (msg instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) msg).content().toUpperCase();
        }
        throw new CodecException("unknown or corrupt message: " + msg);
    }

    /**
     * Returns the parameters of the Redis command represented by this RespRequest.
     *
     * @return the parameters of the Redis command
     */
    public ArrayList<ByteBuf> getParams() {
        if (params != null) {
            return params;
        }
        params = new ArrayList<>();

        Preconditions.checkNotNull(msg, "RedisMessage cannot be null");
        if (msg instanceof ArrayRedisMessage) {
            List<RedisMessage> rawParams = ((ArrayRedisMessage) msg).children();
            ListIterator<RedisMessage> iterator = rawParams.listIterator(1);

            while (iterator.hasNext()) {
                RedisMessage redisMessage = iterator.next();
                if (redisMessage instanceof FullBulkStringRedisMessage) {
                    ByteBuf param = ((FullBulkStringRedisMessage) redisMessage).content();
                    params.add(param);
                }
            }
        }
        return params;
    }

    /**
     * Retrieves the Redis message associated with the Request.
     *
     * @return the RedisMessage object associated with the Request
     */
    public RedisMessage getRedisMessage() {
        return msg;
    }

    /**
     * Retrieves the ChannelHandlerContext associated with the Request.
     *
     * @return the ChannelHandlerContext associated with the Request
     */
    public ChannelHandlerContext getChannelContext() {
        return ctx;
    }
}
