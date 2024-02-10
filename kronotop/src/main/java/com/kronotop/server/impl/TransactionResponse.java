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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
     * Adds a Redis message to the list of messages to be written.
     *
     * @param redisMessage the Redis message to be added
     */
    @Override
    public void writeRedisMessage(RedisMessage redisMessage) {
        messages.add(redisMessage);
    }

    /**
     * Writes an "OK" Redis response message to the client.
     */
    @Override
    public void writeOK() {
        writeSimpleString(Response.OK);
    }

    /**
     * Writes a "QUEUED" Redis response message to the client.
     * <p>
     * This method is used to write a "QUEUED" message to the client as part of a Redis transaction.
     * It should be called after adding a request to the queued commands and before flushing the response.
     */
    @Override
    public void writeQUEUED() {
        writeSimpleString(Response.QUEUED);
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
     * Writes a double value as a Redis response message to the client.
     *
     * @param value the double value to be written
     */
    @Override
    public void writeDouble(long value) {
        messages.add(new DoubleRedisMessage(value));
    }

    /**
     * Writes an array of Redis messages as a response message to the client.
     * <p>
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

    @Override
    public void writeMap(Map<RedisMessage, RedisMessage> children) {
        messages.add(new MapRedisMessage(children));
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
     * Adds a NULL Redis response message to the list of response messages.
     * <p>
     * The method does not return a value.
     */
    @Override
    public void writeNULL() {
        messages.add(NullRedisMessage.INSTANCE);
    }

    /**
     * Writes a boolean value as a Redis response message to the client.
     * <p>
     * If the value is true, it adds BooleanRedisMessage.TRUE to the list of response messages.
     * If the value is false, it adds BooleanRedisMessage.FALSE to the list of response messages.
     *
     * @param value the boolean value to be written
     * @see BooleanRedisMessage
     */
    @Override
    public void writeBoolean(boolean value) {
        if (value) {
            messages.add(BooleanRedisMessage.TRUE);
        } else {
            messages.add(BooleanRedisMessage.FALSE);
        }
    }

    /**
     * Adds a BigInteger value as a Redis response message to the list of response messages.
     *
     * @param value the BigInteger value to be added
     */
    @Override
    public void writeBigNumber(BigInteger value) {
        messages.add(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a big number value as a Redis response message to the client.
     *
     * @param value the big number value to be written
     */
    @Override
    public void writeBigNumber(String value) {
        messages.add(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a byte array value as a Redis response message to the client.
     * This method creates a BigNumberRedisMessage object with the given byte array value and adds it to the list of messages.
     *
     * @param value the byte array value to be written
     * @see BigNumberRedisMessage
     */
    @Override
    public void writeBigNumber(byte[] value) {
        messages.add(new BigNumberRedisMessage(value));
    }

    /**
     * Returns the ChannelHandlerContext associated with this Response object.
     *
     * @return the ChannelHandlerContext associated with this Response object
     */
    @Override
    public ChannelHandlerContext getChannelContext() {
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
     * Writes a bulk error message to the client.
     *
     * @param content the content of the error message
     */
    @Override
    public void writeBulkError(String content) {
        this.writeBulkError(RESPError.ERR, content);
    }

    /**
     * Writes a bulk error message to the client.
     *
     * @param prefix  the prefix of the error message
     * @param content the content of the error message
     * @param <T>     the type of the prefix
     */
    @Override
    public <T> void writeBulkError(T prefix, String content) {
        String error = String.format("%s %s", prefix, content);
        ByteBuf buf = Unpooled.buffer().alloc().buffer(error.length());
        buf.writeBytes(error.getBytes());
        messages.add(new FullBulkStringRedisMessage(buf));
    }

    /**
     * Adds a {@link FullBulkVerbatimStringRedisMessage} to the list of response messages.
     * <p>
     * The verbatim string message includes a format type and the real content.
     *
     * @param content the content of the {@link FullBulkVerbatimStringRedisMessage} to be added. Must not be {@code null}.
     * @see FullBulkVerbatimStringRedisMessage
     */
    @Override
    public void writeVerbatimString(ByteBuf content) {
        messages.add(new FullBulkVerbatimStringRedisMessage(content));
    }

    /**
     * Adds a SetRedisMessage to the list of messages.
     * <p>
     * This method is used to write a SetRedisMessage to the client. The SetRedisMessage contains a set of RedisMessage objects.
     *
     * @param children the set of RedisMessage objects to be written
     * @see SetRedisMessage
     * @see RedisMessage
     */
    @Override
    public void writeSet(Set<RedisMessage> children) {
        messages.add(new SetRedisMessage(children));
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
