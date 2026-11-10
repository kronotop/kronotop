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

package com.kronotop.server;

import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.network.ClientIDGenerator;
import com.kronotop.transaction.InstrumentedTransaction;
import com.kronotop.transaction.TransactionState;
import com.typesafe.config.Config;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the state of one client connection, such as the protocol version, the open transaction
 * and the cursors. A session is created when the channel is registered and removed from the
 * session store when the channel is closed.
 */
public class Session {
    private final ChannelHandlerContext ctx;
    private final Channel channel;
    private final Context context;
    private final AtomicInteger cursorId = new AtomicInteger(1);
    private volatile RESPVersion protocolVersion;

    // Metadata of the running command, shown by TRANSACTION LIST. Arguments are never kept.
    // The pending fields are copied to a transaction when the command binds one, see bind().
    private volatile String pendingCommand;
    private volatile String pendingModel;
    private volatile String pendingNamespace;
    private volatile String pendingBucket;
    private volatile BqlExpr pendingQuery;
    // The transaction bound by the running command, or null.
    private volatile InstrumentedTransaction bound;

    private Session(Context context, ChannelHandlerContext ctx) {
        this.context = context;
        this.ctx = ctx;
        this.channel = ctx.channel();

        initialize();
    }

    /**
     * Creates a session for the channel and adds it to the session store.
     *
     * @param context the context of the Kronotop instance
     * @param ctx     the handler context of the client channel
     */
    public static void registerSession(Context context, ChannelHandlerContext ctx) {
        Session session = new Session(context, ctx);
        ctx.channel().attr(SessionAttributes.SESSION).set(session);
        context.getSessionStore().put(session.getClientId(), session);
    }

    /**
     * Reports whether the configuration requires clients to authenticate.
     *
     * @param config the server configuration
     * @return {@code true} if a password or a user list is configured, {@code false} otherwise
     */
    public static boolean isAuthEnabled(Config config) {
        return config.hasPath("auth.requirepass") || config.hasPath("auth.users");
    }

    /**
     * Returns the session of the given channel.
     *
     * @param channel the client channel
     * @return the session, or {@code null} if the channel has no session
     */
    public static Session extractSessionFromChannel(Channel channel) {
        return channel.attr(SessionAttributes.SESSION).get();
    }

    public int nextCursorId() {
        return cursorId.getAndIncrement();
    }

    public Long getClientId() {
        return channel.attr(SessionAttributes.CLIENT_ID).get();
    }

    public boolean isAuthenticated() {
        Attribute<Boolean> auth = channel.attr(SessionAttributes.AUTH);
        return auth.get();
    }

    private void initialize() {
        setProtocolVersion(RESPVersion.RESP2);
        Long clientId = ClientIDGenerator.getAndIncrement();
        channel.attr(SessionAttributes.CLIENT_ID).set(clientId);
        channel.attr(SessionAttributes.AUTH).set(false);
        channel.attr(SessionAttributes.OPEN_NAMESPACES).set(new ConcurrentHashMap<>());
        channel.attr(SessionAttributes.CLIENT_ATTRIBUTES).set(new HashMap<>());
        channel.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).set(new HashMap<>());
        channel.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).set(new HashMap<>());
        channel.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).set(new HashMap<>());
        channel.attr(SessionAttributes.POST_COMMIT_HOOKS).set(new ArrayList<>());

        restoreDefaults();
    }

    /**
     * Stores the transaction on the session and gives it empty post-commit hooks and a new user version counter.
     *
     * @param tr the transaction to store
     * @throws KronotopException if the session already has a transaction
     */
    public void setTransaction(Transaction tr) {
        Transaction previousTransaction = attr(SessionAttributes.TRANSACTION).get();
        if (previousTransaction != null) {
            throw new KronotopException("Transaction already set");
        }
        attr(SessionAttributes.BEGIN).set(true);
        attr(SessionAttributes.POST_COMMIT_HOOKS).set(new ArrayList<>());
        attr(SessionAttributes.USER_VERSION_COUNTER).set(new AtomicInteger());
        attr(SessionAttributes.TRANSACTION).set(tr);
    }

    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return channel.attr(key);
    }

    /**
     * Records the command that is about to run and clears the metadata left by the previous command.
     *
     * @param command the command name
     * @param model   the data model of the command, or {@code null} if it has none
     */
    public void startCommand(String command, String model) {
        this.pendingCommand = command;
        this.pendingModel = model;
        this.pendingNamespace = null;
        this.pendingBucket = null;
        this.pendingQuery = null;
        this.bound = null;
    }

    /**
     * Binds a transaction to the running command and copies the client id and the command metadata to it,
     * including metadata that the same command sets later.
     *
     * @param tr the transaction to bind
     */
    public void bind(InstrumentedTransaction tr) {
        tr.setClientId(getClientId());
        tr.setCommand(pendingCommand);
        tr.setModel(pendingModel);
        tr.setNamespace(pendingNamespace);
        tr.setBucket(pendingBucket);
        tr.setQuery(pendingQuery);
        this.bound = tr;
    }

    public void setNamespace(String namespace) {
        this.pendingNamespace = namespace;
        InstrumentedTransaction tr = bound;
        if (tr != null) {
            tr.setNamespace(namespace);
        }
    }

    public void setBucket(String bucket) {
        this.pendingBucket = bucket;
        InstrumentedTransaction tr = bound;
        if (tr != null) {
            tr.setBucket(bucket);
        }
    }

    public void setQuery(BqlExpr query) {
        this.pendingQuery = query;
        InstrumentedTransaction tr = bound;
        if (tr != null) {
            tr.setQuery(query);
        }
    }

    /**
     * Marks the open transaction as idle, which can only be one opened with BEGIN because the auto-commit
     * cleanup has already closed the others.
     */
    public void markTransactionIdle() {
        Transaction tr = attr(SessionAttributes.TRANSACTION).get();
        if (tr != null) {
            ((InstrumentedTransaction) tr).setState(TransactionState.IDLE);
        }
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    /**
     * Closes the open transaction, if any, and removes it from the session, ignoring any error from the close.
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
     * Closes the transaction and resets the transaction state when the session is in auto-commit mode,
     * so no transaction is left behind for the next command.
     */
    public void cleanupIfAutoCommitEnabled() {
        Attribute<Boolean> autoCommitAttr = attr(SessionAttributes.AUTO_COMMIT);
        if (Boolean.TRUE.equals(autoCommitAttr.get())) {
            closeAndCleanupTransaction();
        }
    }

    protected void channelUnregistered() {
        // Netty channel is closed, close the transaction if there is any.
        closeTransactionIfAny();
        context.getSessionStore().remove(getClientId());
    }

    /**
     * Closes the open transaction, if any, and resets the transaction state of the session: the BEGIN and
     * auto-commit flags, the post-commit hooks and the user version counter.
     */
    public void closeAndCleanupTransaction() {
        closeTransactionIfAny();
        attr(SessionAttributes.BEGIN).set(false);
        attr(SessionAttributes.AUTO_COMMIT).set(false);
        attr(SessionAttributes.POST_COMMIT_HOOKS).set(new ArrayList<>());
        attr(SessionAttributes.USER_VERSION_COUNTER).set(new AtomicInteger());
    }

    public RESPVersion getProtocolVersion() {
        return protocolVersion;
    }

    public void setProtocolVersion(RESPVersion version) {
        protocolVersion = version;
    }

    public void resetCursorId() {
        cursorId.set(1);
    }

    /**
     * Restores the configurable attributes, the snapshot read flag and the current namespace to their
     * defaults, and empties the open namespace cache.
     */
    public void restoreDefaults() {
        String replyType = context.getConfig().getString("session_attributes.reply_type");
        channel.attr(SessionAttributes.REPLY_TYPE).set(ReplyType.valueOf(replyType.toUpperCase()));

        String inputType = context.getConfig().getString("session_attributes.input_type");
        channel.attr(SessionAttributes.INPUT_TYPE).set(InputType.valueOf(inputType.toUpperCase()));

        Integer batch = context.getConfig().getInt("session_attributes.batch");
        channel.attr(SessionAttributes.BATCH).set(batch);

        String objectIdFormat = context.getConfig().getString("bucket.object_id_format");
        channel.attr(SessionAttributes.OBJECT_ID_FORMAT).set(ObjectIdFormat.valueOf(objectIdFormat.toUpperCase()));

        // SNAPSHOTREAD OFF stores null, so the reset uses the same value.
        channel.attr(SessionAttributes.SNAPSHOT_READ).set(null);
        channel.attr(SessionAttributes.CURRENT_NAMESPACE).set(context.getDefaultNamespace());

        // Empty the maps in place. SessionStore.invalidateOpenNamespaces walks all sessions and
        // touches this map, so replacing it here would leave that scan on a discarded instance.
        channel.attr(SessionAttributes.OPEN_NAMESPACES).get().clear();
    }

    public void clearCursors() {
        channel.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).get().clear();
        channel.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get().clear();
        channel.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get().clear();
    }
}
