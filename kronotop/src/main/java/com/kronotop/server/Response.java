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

import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The `Response` interface represents a response message for the Redis protocol.
 * It provides methods to write different types of Redis messages.
 */
public interface Response {
    /**
     * Writes an "OK" Redis response message to the client.
     */
    void writeOK();

    /**
     * Writes a "QUEUED" Redis response message to the client.
     * <p>
     * This method is used to write a "QUEUED" message to the client as part of a Redis transaction.
     * It should be called after adding a request to the queued commands and before flushing the response.
     */
    void writeQUEUED();

    /**
     * Writes a long integer value as a Redis response message to the client.
     *
     * @param value the long integer value to be written
     */
    void writeInteger(long value);

    /**
     * Writes a double value as a Redis response message to the client.
     *
     * @param value the double value to be written
     */
    void writeDouble(long value);

    /**
     * Writes an array of Redis messages as a response message to the client.
     * <p>
     * This method is used to write an array of Redis messages to the client as a response.
     * Each Redis message is represented by an instance of the RedisMessage interface.
     * The array of Redis messages is represented by a List<RedisMessage>.
     *
     * @param children the array of Redis messages to be written
     */
    void writeArray(List<RedisMessage> children);

    void writeMap(Map<RedisMessage, RedisMessage> children);

    /**
     * Writes a simple string message to the client.
     *
     * @param msg the simple string message to be written
     */
    void writeSimpleString(String msg);

    /**
     * Writes an error message to the client.
     *
     * @param content the content of the error message
     */
    void writeError(String content);

    /**
     * Writes an error message to the client.
     *
     * @param prefix  the prefix of the error message
     * @param content the content of the error message
     * @param <T>     the type of the prefix
     */
    <T> void writeError(T prefix, String content);

    /**
     * Writes a bulk error message to the client.
     *
     * @param content the content of the error message
     */
    void writeBulkError(String content);

    /**
     * Writes a bulk error message to the client.
     *
     * @param prefix  the prefix of the error message
     * @param content the content of the error message
     * @param <T>     the type of the prefix
     */
    <T> void writeBulkError(T prefix, String content);

    /**
     * Writes the content of a ByteBuf as a response message to the client.
     *
     * @param content the content to be written
     */
    void write(ByteBuf content);

    /**
     * Writes a FullBulkStringRedisMessage to the client as a response message.
     *
     * @param msg the FullBulkStringRedisMessage to be written
     */
    void writeFullBulkString(FullBulkStringRedisMessage msg);

    /**
     * Writes a NULL Redis response message to the client.
     * <p>
     * The method does not return a value.
     */
    void writeNULL();

    /**
     * Writes a boolean value as a Redis response message to the client.
     *
     * @param value the boolean value to be written
     */
    void writeBoolean(boolean value);

    /**
     * Writes a BigInteger value as a Redis response message to the client.
     *
     * @param value the BigInteger value to be written
     */
    void writeBigNumber(BigInteger value);

    /**
     * Writes a BigInteger value as a Redis response message to the client.
     *
     * @param value the BigInteger value to be written
     */
    void writeBigNumber(String value);

    /**
     * Writes a byte array as a Redis response message to the client.
     *
     * @param value the byte array value to be written
     * @throws NullPointerException if the value is null
     */
    void writeBigNumber(byte[] value);

    /**
     * Writes the content of a ByteBuf as a verbatim string to the client.
     *
     * @param content the content to be written as a verbatim string
     */
    void writeVerbatimString(ByteBuf content);

    /**
     * Writes a set of Redis messages as a response message to the client.
     * <p>
     * This method is used to write a set of Redis messages to the client as a response.
     *
     * @param children the set of Redis messages to be written
     */
    void writeSet(Set<RedisMessage> children);

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
    void flush();

    /**
     * Returns the ChannelHandlerContext associated with this Response object.
     *
     * @return the ChannelHandlerContext associated with this Response object
     */
    ChannelHandlerContext getContext();
}
