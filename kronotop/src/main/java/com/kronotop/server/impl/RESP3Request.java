/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.server.Session;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;
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
public class RESP3Request extends DefaultAttributeMap implements Request {
    private final RedisMessage message;
    private final Session session;
    private ArrayList<ByteBuf> params;
    private String command;

    public RESP3Request(Session session, Object message) {
        this.message = (RedisMessage) message;
        this.session = session;
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

        Preconditions.checkNotNull(message, "RedisMessage cannot be null");
        if (message instanceof ArrayRedisMessage) {
            RedisMessage redisMessage = ((ArrayRedisMessage) message).children().get(0);
            if (redisMessage instanceof FullBulkStringRedisMessage) {
                command = ((FullBulkStringRedisMessage) redisMessage).content().toString(CharsetUtil.US_ASCII).toUpperCase();
                return command;
            }
        } else if (message instanceof SimpleStringRedisMessage) {
            return ((SimpleStringRedisMessage) message).content().toUpperCase();
        }
        throw new CodecException("unknown or corrupt message: " + message);
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

        Preconditions.checkNotNull(message, "RedisMessage cannot be null");
        if (message instanceof ArrayRedisMessage) {
            List<RedisMessage> rawParams = ((ArrayRedisMessage) message).children();
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
        return message;
    }

    /**
     * Retrieves the session associated with this RESP3Request.
     *
     * @return the Session object associated with this request
     */
    public Session getSession() {
        return session;
    }
}
