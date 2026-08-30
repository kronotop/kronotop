/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.apple.foundationdb.FDBException;
import com.kronotop.KronotopException;
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.TransactionUtil;
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
public class RESPResponse implements Response {
    private final ChannelHandlerContext ctx;
    private final Session session;

    public RESPResponse(ChannelHandlerContext ctx, Session session) {
        this.ctx = ctx;
        this.session = session;
    }

    /**
     * Writes a Redis message to the client. The message is rewritten first if the session speaks
     * a protocol version that does not know all of its types.
     *
     * @param message the Redis message to be written
     * @throws NullPointerException if the message is null
     */
    @Override
    public void writeRedisMessage(RedisMessage message) {
        ctx.writeAndFlush(RESPUtil.downgrade(message, session.getProtocolVersion()));
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
     * Writes a double value to the client, encoded for the negotiated protocol version.
     * RESP3 uses its own double type, RESP2 receives a bulk string.
     *
     * @param value the double value to be written
     */
    @Override
    public void writeDouble(double value) {
        ctx.writeAndFlush(RESPUtil.doubleMessage(value, session.getProtocolVersion()));
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
        writeRedisMessage(new ArrayRedisMessage(children));
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
        writeRedisMessage(new MapRedisMessage(children));
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
     * The wire form depends on the protocol version negotiated by the session.
     */
    @Override
    public void writeNULL() {
        ctx.writeAndFlush(RESPUtil.nullMessage(session.getProtocolVersion()));
    }

    /**
     * Writes a boolean value to the client, encoded for the negotiated protocol version.
     * RESP3 uses its own boolean type, RESP2 receives the integer 1 or 0.
     *
     * @param value the boolean value to be written
     */
    @Override
    public void writeBoolean(boolean value) {
        ctx.writeAndFlush(RESPUtil.booleanMessage(value, session.getProtocolVersion()));
    }

    /**
     * Writes a big number as a Redis response message to the client.
     *
     * @param value the big number value to be written
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeBigNumber(BigInteger value) {
        writeRedisMessage(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a big number as a Redis response message to the client.
     *
     * @param value the big number value to be written as a string
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeBigNumber(String value) {
        writeRedisMessage(new BigNumberRedisMessage(value));
    }

    /**
     * Writes a big number as a Redis response message to the client.
     *
     * @param value the big number value to be written as a byte array
     * @throws NullPointerException if the value is null
     */
    @Override
    public void writeBigNumber(byte[] value) {
        writeRedisMessage(new BigNumberRedisMessage(value));
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
        writeRedisMessage(new FullBulkVerbatimStringRedisMessage(content));
    }

    /**
     * Writes a set of Redis messages as a response message to the client.
     *
     * @param children the set of Redis messages to be written
     * @throws NullPointerException if the children set is null
     */
    @Override
    public void writeSet(Set<RedisMessage> children) {
        writeRedisMessage(new SetRedisMessage(children));
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
     * Writes the error that best describes a {@link Throwable} to the client.
     * <p>
     * Failures from async commands arrive wrapped in a {@link CompletionException}, so the cause
     * chain is unwrapped first. An {@link FDBException} is reported with its own prefix and a
     * registered message, which lets clients act on the FoundationDB error code. Otherwise the
     * prefix and message come from a {@link KronotopException} in the chain when there is one,
     * and from the root cause with a generic prefix when there is none.
     *
     * @param throwable the Throwable representing the error to be written to the client
     *                  (e.g., an exception or error that occurred during processing)
     * @throws NullPointerException if the throwable is null
     * @see RESPError#preferKronotopCause(Throwable)
     */
    @Override
    public void writeError(Throwable throwable) {
        if (throwable instanceof CompletionException) {
            Throwable cause = TransactionUtil.getRootCause(throwable);
            if (cause instanceof KronotopException kr) {
                writeError(kr.getPrefix(), RESPError.getMessage(cause));
            } else if (cause instanceof FDBException fdbEx) {
                RESPError.FDBErrorResult result = RESPError.extractFDBError(fdbEx);
                writeError(result.prefix(), result.message());
            } else {
                Throwable preferredCause = RESPError.preferKronotopCause(throwable);
                if (preferredCause instanceof KronotopException kr) {
                    writeError(kr.getPrefix(), RESPError.getMessage(kr));
                } else {
                    writeError(RESPError.getMessage(preferredCause));
                }
            }
        } else if (throwable instanceof FDBException fdbEx) {
            RESPError.FDBErrorResult result = RESPError.extractFDBError(fdbEx);
            writeError(result.prefix(), result.message());
        } else {
            writeError(RESPError.getMessage(throwable));
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
     * Flushes any pending messages to the client.
     */
    @Override
    public void flush() {
        ctx.flush();
    }
}
