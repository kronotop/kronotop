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

package com.kronotop.server;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.network.ClientIDGenerator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a session that manages context, channel, and session-specific attributes.
 * A session is primarily used to interact with a client's connection and maintain its state
 * throughout the lifecycle of the connection.
 * <p>
 * The session is responsible for initializing and managing attributes such as client ID,
 * Redis transaction tracking, and other context-specific settings. It also provides methods
 * for cleanup and resource management, such as handling transaction closure and session
 * post-commit processing.
 */
public class Session {
    private final ChannelHandlerContext ctx;
    private final Channel channel;
    private final Context context;
    private volatile RESPVersion protocolVersion;

    private Session(Context context, ChannelHandlerContext ctx) {
        this.context = context;
        this.ctx = ctx;
        this.channel = ctx.channel();

        initialize();
    }

    /**
     * Registers a new session and associates it with the specified {@link ChannelHandlerContext}.
     * <p>
     * This method creates a {@link Session} instance using the provided {@link Context} and
     * {@link ChannelHandlerContext}. The created session is then stored in the {@link Channel}'s
     * attributes using {@link SessionAttributes#SESSION}.
     *
     * @param context the {@link Context} instance representing the environment of the session.
     * @param ctx     the {@link ChannelHandlerContext} used to manage the channel where the session
     *                will be associated. This context allows interaction with the underlying channel.
     */
    public static void registerSession(Context context, ChannelHandlerContext ctx) {
        Session session = new Session(context, ctx);
        ctx.channel().attr(SessionAttributes.SESSION).set(session);
    }

    /**
     * Extracts the {@link Session} object associated with a given {@link Channel}.
     *
     * @param channel the {@link Channel} from which the session is to be extracted.
     *                This channel is expected to have an associated {@link Session}
     *                attribute stored using {@link SessionAttributes#SESSION}.
     * @return the {@link Session} object associated with the given channel, or null
     * if no session is associated with the channel.
     */
    public static Session extractSessionFromChannel(Channel channel) {
        return channel.attr(SessionAttributes.SESSION).get();
    }

    public Long getClientId() {
        return channel.attr(SessionAttributes.CLIENT_ID).get();
    }

    private void initialize() {
        setProtocolVersion(RESPVersion.RESP2);
        Long clientId = ClientIDGenerator.getAndIncrement();
        channel.attr(SessionAttributes.CLIENT_ID).set(clientId);
        channel.attr(SessionAttributes.OPEN_NAMESPACES).set(new HashMap<>());
        channel.attr(SessionAttributes.CURRENT_NAMESPACE).set(context.getDefaultNamespace());
        channel.attr(SessionAttributes.CLIENT_ATTRIBUTES).set(new HashMap<>());
        channel.attr(SessionAttributes.READONLY).set(false);
        channel.attr(SessionAttributes.QUEUED_COMMANDS).set(new LinkedList<>());
        channel.attr(SessionAttributes.MULTI).set(false);
        channel.attr(SessionAttributes.MULTI_DISCARDED).set(false);

        String replyType = context.getConfig().getString("session_attributes.reply_type");
        channel.attr(SessionAttributes.REPLY_TYPE).set(ReplyType.valueOf(replyType.toUpperCase()));

        String inputType = context.getConfig().getString("session_attributes.input_type");
        channel.attr(SessionAttributes.INPUT_TYPE).set(InputType.valueOf(inputType.toUpperCase()));

        Integer bucketBatchSize = context.getConfig().getInt("session_attributes.limit");
        channel.attr(SessionAttributes.LIMIT).set(bucketBatchSize);

        Boolean pinReadVersion = context.getConfig().getBoolean("session_attributes.pin_read_version");
        channel.attr(SessionAttributes.PIN_READ_VERSION).set(pinReadVersion);
    }

    /**
     * Sets the current transaction for the session and initializes transactional session attributes.
     * <p>
     * This method establishes a new transaction for the session by setting the provided
     * {@link Transaction} object to the session's {@code TRANSACTION} attribute. It also initializes
     * various session attributes to ensure proper transactional state setup, such as
     * marking the transaction as started, and preparing hooks and counters related to
     * transaction processing.
     *
     * @param tr the {@link Transaction} object to be associated with the session. This object
     *           represents the transaction details to be used in subsequent operations.
     */
    public void setTransaction(Transaction tr) {
        Transaction previousTransaction = attr(SessionAttributes.TRANSACTION).get();
        if (previousTransaction != null) {
            throw new KronotopException("Transaction already set");
        }
        attr(SessionAttributes.BEGIN).set(true);
        attr(SessionAttributes.POST_COMMIT_HOOKS).set(new LinkedList<>());
        attr(SessionAttributes.ASYNC_RETURNING).set(new LinkedList<>());
        attr(SessionAttributes.USER_VERSION_COUNTER).set(new AtomicInteger());
        attr(SessionAttributes.TRANSACTION).set(tr);
    }

    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return channel.attr(key);
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    /**
     * Closes any active transaction associated with the session and releases its resources.
     * <p>
     * If there is an active transaction stored under the TRANSACTION attribute in the session,
     * this method ensures that it is properly closed and resources are released. After the
     * transaction is closed, the TRANSACTION attribute is reset to null.
     * <p>
     * This method is a utility to safely handle transaction cleanup without throwing exceptions
     * if the transaction fails to close. It silently ignores any exceptions that occur during
     * the closing process.
     * <p>
     * It should be called after the transaction is no longer in use to ensure proper resource
     * management and prevent resource leaks.
     */
    protected void closeTransactionIfAny() {
        // Close the Transaction object and release any associated resources.
        // This must be called at least once after the Transaction object is no longer in use.
        Attribute<Transaction> transactionAttr = attr(SessionAttributes.TRANSACTION);
        if (transactionAttr.get() == null) {
            return;
        }

        Transaction tr = transactionAttr.get();
        try {
            tr.close();
        } catch (Exception e) {
            // Ignore it
        } finally {
            attr(SessionAttributes.TRANSACTION).set(null);
        }
    }

    /**
     * Cleans up the session's transactional state if auto-commit mode is enabled.
     * <p>
     * This method checks the {@code SessionAttributes.AUTO_COMMIT} attribute associated with the session.
     * If auto-commit mode is enabled (attribute value is {@code true}), it calls {@code unsetTransaction()}
     * to reset the session's transactional state and release any associated resources.
     * <p>
     * It ensures that sessions operating in auto-commit mode do not retain any leftover transaction state
     * that could interfere with subsequent operations.
     */
    public void cleanupIfAutoCommitEnabled() {
        Attribute<Boolean> autoCommitAttr = attr(SessionAttributes.AUTO_COMMIT);
        if (Boolean.TRUE.equals(autoCommitAttr.get())) {
            unsetTransaction();
        }
    }

    protected void channelUnregistered() {
        // Netty channel is closed, close the transaction if there is any.
        closeTransactionIfAny();
    }

    /**
     * Resets the transactional state of the session.
     * <p>
     * This method ensures that any active transaction within the session is properly closed and releases
     * related resources by invoking the {@code closeTransactionIfAny()} method. It then resets various
     * session attributes related to transaction management, including:
     * <p>
     * - Sets the {@code SessionAttributes.BEGIN} attribute to {@code false} indicating that no transaction
     * is currently active.
     * - Sets the {@code SessionAttributes.AUTO_COMMIT} attribute to {@code false} disabling auto-commit mode.
     * - Resets the {@code SessionAttributes.POST_COMMIT_HOOKS} attribute to an empty {@code LinkedList}.
     * - Resets the {@code SessionAttributes.ASYNC_RETURNING} attribute to an empty {@code LinkedList}.
     * - Resets the {@code SessionAttributes.USER_VERSION_COUNTER} attribute to a new {@code AtomicInteger}.
     * <p>
     * This method is typically called when a transaction needs to be explicitly terminated, ensuring the session
     * is cleaned up and ready for subsequent operations without leftover stale transaction state.
     */
    public void unsetTransaction() {
        closeTransactionIfAny();
        attr(SessionAttributes.BEGIN).set(false);
        attr(SessionAttributes.AUTO_COMMIT).set(false);
        attr(SessionAttributes.POST_COMMIT_HOOKS).set(new LinkedList<>());
        attr(SessionAttributes.ASYNC_RETURNING).set(new LinkedList<>());
        attr(SessionAttributes.USER_VERSION_COUNTER).set(new AtomicInteger());
    }

    /**
     * Sets the protocol version for the session.
     *
     * @param version the {@link RESPVersion} to be set for the session. This determines the
     *                version of the Redis Serialization Protocol (RESP) that the session will use.
     */
    public void setProtocolVersion(RESPVersion version) {
        protocolVersion = version;
    }

    /**
     * Retrieves the protocol version being used by the session.
     *
     * @return the {@link RESPVersion} representing the version of the Redis Serialization Protocol (RESP)
     * currently set for the session.
     */
    public RESPVersion protocolVersion() {
        return protocolVersion;
    }
}
