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

import java.util.List;

/**
 * The RespResponse class is an implementation of the Response interface.
 * It provides methods to write different types of Redis messages using the RESP (REdis Serialization Protocol).
 */
public class RespResponse implements Response {
    private final ChannelHandlerContext ctx;

    public RespResponse(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Writes an "OK" Redis response message to the client.
     * <p>
     * This method is used to write an "OK" message to the client.
     * It calls the writeSimpleString method with the content "OK".
     */
    @Override
    public void writeOK() {
        writeSimpleString("OK");
    }

    /**
     * Writes a "QUEUED" Redis response message to the client.
     * <p>
     * This method is used to write a "QUEUED" message to the client as part of a Redis transaction.
     * It should be called after adding a request to the queued commands and before flushing the response.
     */
    @Override
    public void writeQUEUED() {
        ctx.write(new SimpleStringRedisMessage("QUEUED"));
    }

    /**
     * Writes a long integer value as a Redis response message to the client.
     *
     * @param value the long integer value to be written
     *
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeInteger(long value) {
        ctx.writeAndFlush(new IntegerRedisMessage(value));
    }

    /**
     * Writes an array of Redis messages as a response message to the client.
     * <p>
     * This method is used to write an array of Redis messages to the client as a response.
     * Each Redis message is represented by an instance of the RedisMessage interface.
     * The array of Redis messages is represented by a List<RedisMessage>.
     *
     * @param children the array of Redis messages to be written
     * @throws NullPointerException if the children list is null
     */
    @Override
    public void writeArray(List<RedisMessage> children) {
        ctx.writeAndFlush(new ArrayRedisMessage(children));
    }

    /**
     * Writes a simple string message to the client.
     *
     * @param msg the simple string message to be written
     * @throws NullPointerException if the msg is null
     */
    @Override
    public void writeSimpleString(String msg) {
        ctx.writeAndFlush(new SimpleStringRedisMessage(msg));
    }

    /**
     * Writes the given {@link ByteBuf} content as a Redis response message to the client.
     *
     * @param content the content to be written
     * @throws NullPointerException if the content is null
     */
    @Override
    public void write(ByteBuf content) {
        ctx.writeAndFlush(new FullBulkStringRedisMessage(content));
    }

    /**
     * Writes a full bulk string Redis message to the client.
     *
     * @param msg the full bulk string message to be written
     * @throws NullPointerException if the msg is null
     */
    @Override
    public void writeFullBulkString(FullBulkStringRedisMessage msg) {
        ctx.writeAndFlush(msg);
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
     * @param prefix  the content to be written before the error message
     * @param content the content of the error message
     * @throws NullPointerException if the prefix or content is null
     */
    @Override
    public <T> void writeError(T prefix, String content) {
        ctx.writeAndFlush(new ErrorRedisMessage(String.format("%s %s", prefix, content)));
    }

    /**
     * Flushes the response messages to the client.
     * <p>
     * This method is used to flush the response messages to the client. It sends the buffered messages to the client
     * for processing. If there are no response messages to flush, it sends a NULL_INSTANCE message.
     * <p>
     * If the response messages are an array of Redis messages, it sends an ArrayRedisMessage. If the response messages
     * are an "OK" Redis message, it sends a FullBulkStringRedisMessage with the value "OK". If the response messages are
     * a "QUEUED" Redis message, it sends a FullBulkStringRedisMessage with the value "QUEUED".
     */
    @Override
    public void flush() {
        ctx.flush();
    }
}
