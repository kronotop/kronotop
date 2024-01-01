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

import com.kronotop.common.resp.RESPError;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The TransactionResponse class represents a response message for a Redis transaction.
 * It implements the Response interface and provides methods to write different types of Redis messages.
 */
public class TransactionResponse implements Response {
    private final ChannelHandlerContext ctx;
    private final List<RedisMessage> messages = new ArrayList<>();

    public TransactionResponse(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Writes an "OK" Redis response message to the client.
     */
    @Override
    public void writeOK() {
        writeSimpleString("OK");
    }

    /**
     * Writes a "QUEUED" Redis response message to the client.
     *
     * This method is used to write a "QUEUED" message to the client as part of a Redis transaction.
     * It should be called after adding a request to the queued commands and before flushing the response.
     */
    @Override
    public void writeQUEUED() {
        writeSimpleString("QUEUED");
    }

    /**
     * Writes a long integer value as a Redis response message to the client.
     *
     * @param value the long integer value to be written
     */
    @Override
    public void writeInteger(long value) {
        messages.add(new IntegerRedisMessage(value));
    }

    /**
     * Writes an array of Redis messages as a response message to the client.
     *
     * This method is used to write an array of Redis messages to the client as a response.
     * Each Redis message is represented by an instance of the RedisMessage interface.
     * The array of Redis messages is represented by a List<RedisMessage>.
     *
     * @param children the array of Redis messages to be written
     * @param children the array of Redis messages to be written
     * @see RedisMessage
     */
    @Override
    public void writeArray(List<RedisMessage> children) {
        messages.add(new ArrayRedisMessage(children));
    }

    /**
     * Adds a simple string message to the list of response messages.
     *
     * @param msg the simple string message to be added
     */
    @Override
    public void writeSimpleString(String msg) {
        messages.add(new SimpleStringRedisMessage(msg));
    }

    /**
     * Adds a {@link FullBulkStringRedisMessage} to the list of messages.
     *
     * @param content the content of the {@link FullBulkStringRedisMessage} to be added
     */
    @Override
    public void write(ByteBuf content) {
        messages.add(new FullBulkStringRedisMessage(content));
    }

    /**
     * Adds a {@link FullBulkStringRedisMessage} to the list of messages.
     *
     * @param msg the {@link FullBulkStringRedisMessage} to be added
     */
    @Override
    public void writeFullBulkString(FullBulkStringRedisMessage msg) {
        messages.add(msg);
    }

    /**
     * Returns the ChannelHandlerContext associated with this Response object.
     *
     * @return the ChannelHandlerContext associated with this Response object
     */
    @Override
    public ChannelHandlerContext getContext() {
        return this.ctx;
    }

    /**
     * Writes an error message to the client.
     *
     * @param content the content of the error message
     */
    @Override
    public void writeError(String content) {
        this.writeError(RESPError.ERR, content);
    }

    /**
     * Writes an error message to the client.
     *
     * @param prefix  the prefix of the error message
     * @param content the content of the error message
     * @param <T>     the type of the prefix
     */
    @Override
    public <T> void writeError(T prefix, String content) {
        messages.add(new ErrorRedisMessage(String.format("%s %s", prefix, content)));
    }

    /**
     * Flushes the messages in the TransactionResponse object.
     * If the messages list is empty, it writes a NULL_INSTANCE message to the channel and flushes it.
     * If the messages list is not empty, it writes an ArrayRedisMessage containing the messages list to the channel and flushes it.
     */
    @Override
    public void flush() {
        if (messages.isEmpty()) {
            ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
            return;
        }
        ctx.writeAndFlush(new ArrayRedisMessage(messages));
    }
}
