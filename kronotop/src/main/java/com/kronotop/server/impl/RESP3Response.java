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

import com.kronotop.KronotopException;
import com.kronotop.server.RESPError;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;

/**
 * The RespResponse class is an implementation of the Response interface.
 * It provides methods to write different types of Redis messages using the RESP (REdis Serialization Protocol).
 */
public class RESP3Response implements Response {
    private final ChannelHandlerContext ctx;

    public RESP3Response(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    /**
     * Writes a Redis message to the client.
     *
     * @param message the Redis message to be written
     * @throws NullPointerException if the message is null
     */
    @Override
    public void writeRedisMessage(RedisMessage message) {
        ctx.writeAndFlush(message);
    }

    /**
     * Writes an "OK" Redis response message to the client.
     * <p>
     * This method is used to write an "OK" message to the client.
     * It calls the writeSimpleString method with the content "OK".
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
        ctx.write(new SimpleStringRedisMessage(Response.QUEUED));
    }

    /**
     * Writes a long integer value as a Redis response message to the client.
     *
     * @param value the long integer value to be written
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeInteger(long value) {
        ctx.writeAndFlush(new IntegerRedisMessage(value));
    }

    /**
     * Writes a double value as a Redis response message to the client.
     *
     * @param value the double value to be written
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeDouble(long value) {
        ctx.writeAndFlush(new DoubleRedisMessage(value));
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
     * Writes a map of Redis messages as a response message to the client.
     * <p>
     * This method is used to write a map of Redis messages to the client as a response.
     * Each key-value pair in the map represents a Redis message, where both the key and value are instances of the RedisMessage interface.
     * The map of Redis messages is represented by a Map<RedisMessage, RedisMessage>.
     *
     * @param children the map of Redis messages to be written
     * @throws NullPointerException if the children map is null
     */
    @Override
    public void writeMap(Map<RedisMessage, RedisMessage> children) {
        ctx.writeAndFlush(new MapRedisMessage(children));
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
     * Writes a NULL Redis message to the client.
     * <p>
     * This method is used to write a NULL message to the client as a response.
     * It calls the {@link ChannelHandlerContext#writeAndFlush(Object)} method with the {@link NullRedisMessage#INSTANCE}.
     */
    @Override
    public void writeNULL() {
        ctx.writeAndFlush(NullRedisMessage.INSTANCE);
    }

    /**
     * Writes a boolean value as a Redis response message to the client.
     * <p>
     * This method is used to write a boolean value as a Redis response message to the client.
     * If the value is true, it writes the BooleanRedisMessage.TRUE message to the client.
     * If the value is false, it writes the BooleanRedisMessage.FALSE message to the client.
     *
     * @param value the boolean value to be written
     */
    @Override
    public void writeBoolean(boolean value) {
        if (value) {
            ctx.writeAndFlush(BooleanRedisMessage.TRUE);
        } else {
            ctx.writeAndFlush(BooleanRedisMessage.FALSE);
        }
    }

    /**
     * Writes a big number as a Redis response message to the client.
     *
     * @param value the big number value to be written
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeBigNumber(BigInteger value) {
        ctx.writeAndFlush(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a big number as a Redis response message to the client.
     *
     * @param value the big number value to be written as a string
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeBigNumber(String value) {
        ctx.writeAndFlush(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a big number as a Redis response message to the client.
     *
     * @param value the big number value to be written as a byte array
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeBigNumber(byte[] value) {
        ctx.writeAndFlush(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a verbatim string message to the client.
     * <p>
     * This method is used to write a verbatim string message to the client.
     * The verbatim string message includes a format and the actual content.
     * The format is a 3-byte string representing the format of the content,
     * such as "txt" for plain text. The actual content follows the format.
     *
     * @param content the content to be written, must not be null
     * @throws NullPointerException if the content is null
     */
    @Override
    public void writeVerbatimString(ByteBuf content) {
        ctx.writeAndFlush(new FullBulkVerbatimStringRedisMessage(content));
    }

    /**
     * Writes a set of Redis messages as a response message to the client.
     *
     * @param children the set of Redis messages to be written
     * @throws NullPointerException if the children set is null
     */
    @Override
    public void writeSet(Set<RedisMessage> children) {
        ctx.writeAndFlush(new SetRedisMessage(children));
    }

    /**
     * Returns the ChannelHandlerContext associated with this Response object.
     *
     * @return the ChannelHandlerContext associated with this Response object
     */
    @Override
    public ChannelHandlerContext getCtx() {
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
     * Writes an error message to the client based on the provided {@link Throwable}.
     * If the Throwable is a {@link CompletionException} and contains a cause of type
     * {@link KronotopException}, it writes the error with a prefix and message from
     * the cause; otherwise, it writes the message of the cause or the Throwable itself.
     *
     * @param throwable the Throwable representing the error to be written to the client
     *                  (e.g., an exception or error that occurred during processing)
     * @throws NullPointerException if the throwable is null
     */
    @Override
    public void writeError(Throwable throwable) {
        if (throwable instanceof CompletionException completionException) {
            if (completionException.getCause() instanceof KronotopException kr) {
                writeError(kr.getPrefix(), completionException.getCause().getMessage());
            } else {
                writeError(completionException.getCause().getMessage());
            }
        } else {
            writeError(throwable.getMessage());
        }
    }

    /**
     * Writes an error message to the client with a given prefix and content.
     *
     * @param <T>     the type of the prefix
     * @param prefix  the prefix to be added to the error message
     * @param content the content of the error message
     * @throws NullPointerException if the prefix or content is null
     */
    @Override
    public <T> void writeError(T prefix, String content) {
        ctx.writeAndFlush(new ErrorRedisMessage(String.format("%s %s", prefix, content)));
    }

    /**
     * Writes a bulk error message to the client.
     *
     * @param content the content of the error message
     * @throws NullPointerException if the content is null
     */
    @Override
    public void writeBulkError(String content) {
        this.writeBulkError(RESPError.ERR, content);
    }

    /**
     * Writes a bulk error message with a prefix to the client.
     *
     * @param prefix  the prefix of the error message
     * @param content the content of the error message
     * @param <T>     the type of the prefix
     * @param prefix  the content to be written before the error message
     * @param content the content of the error message
     * @throws NullPointerException if the prefix or content is null
     */
    @Override
    public <T> void writeBulkError(T prefix, String content) {
        String error = String.format("%s %s", prefix, content);
        ByteBuf buf = Unpooled.buffer().alloc().buffer(error.length());
        buf.writeBytes(error.getBytes());
        ctx.writeAndFlush(new FullBulkErrorStringRedisMessage(buf));
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
