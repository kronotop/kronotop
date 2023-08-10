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

package com.kronotop.server.resp.impl;

import com.kronotop.common.Preconditions;
import com.kronotop.server.resp.ChannelAttributes;
import com.kronotop.server.resp.KronotopMessage;
import com.kronotop.server.resp.Request;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import io.netty.util.DefaultAttributeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class RespRequest extends DefaultAttributeMap implements Request {
    private final RedisMessage msg;
    private final ChannelHandlerContext ctx;
    private ArrayList<ByteBuf> params;
    private String command;

    public RespRequest(ChannelHandlerContext ctx, Object msg) {
        this.msg = (RedisMessage) msg;
        this.ctx = ctx;
    }

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

    public RedisMessage getRedisMessage() {
        return msg;
    }

    public ChannelHandlerContext getContext() {
        return ctx;
    }

    @Override
    public KronotopMessage<?> getKronotopMessage() {
        return this.attr(ChannelAttributes.CURRENT_KRONOTOP_MESSAGE).get();
    }

    @Override
    public void setKronotopMessage(KronotopMessage<?> kronotopMessage) {
        this.attr(ChannelAttributes.CURRENT_KRONOTOP_MESSAGE).set(kronotopMessage);
    }
}
