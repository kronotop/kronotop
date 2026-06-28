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

import com.apple.foundationdb.FDBException;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.MemberAttributes;
import com.kronotop.core.CoreService;
import com.kronotop.instance.KronotopInstanceStatus;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.impl.RESP3Request;
import com.kronotop.server.impl.RESP3Response;
import com.kronotop.server.impl.TransactionResponse;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.stash.StashService;
import com.kronotop.stash.handlers.transactions.protocol.DiscardMessage;
import com.kronotop.stash.handlers.transactions.protocol.ExecMessage;
import com.kronotop.stash.handlers.transactions.protocol.MultiMessage;
import com.kronotop.stash.handlers.transactions.protocol.WatchMessage;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.watcher.Watcher;
import com.kronotop.zmap.ZMapService;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * KronotopChannelDuplexHandler is a specialized implementation of {@link ChannelDuplexHandler}
 * designed to handle duplex communication within the Kronotop Redis-like framework. This
 * handler manages incoming and outgoing requests at the channel level, and extends
 * functionality to support authentication, command execution, and transaction handling.
 * <p>
 * This class interacts with Redis-like command handling and transaction flows, while
 * providing additional utilities for command validation and watcher mechanisms to monitor
 * key events.
 * <p>
 * The class integrates with the Kronotop's context to access configuration,
 * service dependencies, and custom registry of command handlers.
 * <p>
 * Key Features:
 * - Handles the life-cycle of network channels, such as registration and unregistration.
 * - Supports authentication if properly enabled in the configuration.
 * - Processes and executes Redis-like commands with parameter validation.
 * - Provides utility methods to assist in transaction handling with redis-like `MULTI`, `EXEC`,
 * and `DISCARD` commands.
 * - Monitors and handles watched keys during transactions.
 * - Logs commands for debugging purposes if configured.
 * <p>
 * Preconditions:
 * - Requires `Context` and `CommandHandlerRegistry` objects during instantiation.
 * - Dependent on configuration-based settings for authentication and command logging.
 * <p>
 * Thread-Safety:
 * - A `ReadWriteLock` is used to synchronize command execution and transaction queuing.
 * - Locking ensures consistency during reading and modification of shared resources.
 * <p>
 * Error Handling:
 * - Handles incorrect number of arguments for commands through validations.
 * - Custom exceptions are converted to RESP-formatted errors for proper client response.
 * - System exceptions and unforeseen errors during transaction execution are gracefully addressed.
 */
public class KronotopChannelDuplexHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopChannelDuplexHandler.class);

    private final Context context;
    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock(true);
    private final Watcher watcher;
    private final StashService stashService;
    private final ZMapService zmapService;
    private final CoreService coreService;
    private final CommandHandlerRegistry commands;
    private final ServerKind serverKind;
    private final boolean logCommandForDebugging;
    private boolean authEnabled = false;

    public KronotopChannelDuplexHandler(Context context, CommandHandlerRegistry commands, ServerKind serverKind) {
        this.context = context;
        this.commands = commands;
        this.serverKind = serverKind;
        this.watcher = context.getService(Watcher.NAME);
        this.stashService = context.getService(StashService.NAME);
        this.zmapService = context.getService(ZMapService.NAME);
        this.coreService = context.getService(CoreService.NAME);

        Config config = context.getConfig();
        this.logCommandForDebugging = config.hasPath("log_command_for_debugging") && config.getBoolean("log_command_for_debugging");

        if (config.hasPath("auth.requirepass") || config.hasPath("auth.users")) {
            authEnabled = true;
        }
    }

    private void checkMaximumParameterCount(HandlerEntry entry, Request request) throws WrongNumberOfArgumentsException {
        if (entry.hasMaximumParameterCount()) {
            if (request.getParams().size() > entry.maximumParameterCount()) {
                throw new WrongNumberOfArgumentsException(
                        String.format("wrong number of arguments for '%s' command", request.getCommand())
                );
            }
        }
    }

    private void checkMinimumParameterCount(HandlerEntry entry, Request request) throws WrongNumberOfArgumentsException {
        if (entry.hasMinimumParameterCount()) {
            if (request.getParams().size() < entry.minimumParameterCount()) {
                throw new WrongNumberOfArgumentsException(
                        String.format("wrong number of arguments for '%s' command", request.getCommand())
                );
            }
        }
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        // Session life-cycle starts here
        Session.registerSession(context, ctx);
        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        Session session = Session.extractSessionFromChannel(ctx.channel());
        watcher.unwatchWatchedKeys(session);
        releaseZWatch(session);
        coreService.getSubscriptionRegistry().unsubscribeAll(session);
        session.channelUnregistered();
        super.channelUnregistered(ctx);
    }

    /**
     * Releases this client's demand on the key it is blocked on with ZWATCH, if any. Removing the
     * client from the key's waiter set unblocks its handler and cancels the underlying watch once that
     * set drains to empty.
     */
    private void releaseZWatch(Session session) {
        byte[] packedKey = session.attr(SessionAttributes.ZWATCH_KEY).get();
        if (packedKey != null) {
            zmapService.getZWatcher().leave(packedKey, session.getClientId());
        }
    }

    private void exceptionToRespError(Request request, Response response, Exception exception) {
        switch (exception) {
            case KronotopException exp -> {
                if (exp.getCause() != null) {
                    response.writeError(exp.getPrefix(), exp.getCause().getMessage());
                } else {
                    response.writeError(exp.getPrefix(), exp.getMessage());
                }
            }
            case CompletionException compExp -> {
                Throwable cause = TransactionUtil.getRootCause(compExp);
                if (cause instanceof FDBException fdbEx) {
                    RESPError.FDBErrorResult result = RESPError.extractFDBError(fdbEx);
                    response.writeError(result.prefix(), result.message());
                } else if (cause instanceof KronotopException krEx) {
                    response.writeError(krEx.getPrefix(), krEx.getMessage());
                } else {
                    String message;
                    message = RESPError.decapitalize(Objects.requireNonNullElse(cause, compExp).getMessage());
                    response.writeError(message);
                }
            }
            case FDBException fdbEx -> {
                RESPError.FDBErrorResult result = RESPError.extractFDBError(fdbEx);
                response.writeError(result.prefix(), result.message());
            }
            default -> {
                StringBuilder command = new StringBuilder();
                command.append(request.getCommand()).append(" ");
                for (ByteBuf buf : request.getParams()) {
                    byte[] rawParam = ProtocolMessageUtil.readAsByteArray(buf);
                    buf.resetReaderIndex();
                    String param = new String(rawParam);
                    command.append(param).append(" ");
                }
                LOGGER.debug("Unhandled error while serving command: {}: '{}'", command, exception.getMessage());
                response.writeError(exception.getMessage());
            }
        }
    }

    private static boolean isAllowedInSubscriptionMode(String command) {
        return switch (command) {
            case "SSUBSCRIBE", "SUNSUBSCRIBE", "SUBSCRIBE", "UNSUBSCRIBE",
                 "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET" -> true;
            default -> false;
        };
    }

    private void beforeExecute(HandlerEntry entry, Request request) {
        checkMinimumParameterCount(entry, request);
        checkMaximumParameterCount(entry, request);
        try {
            entry.handler().beforeExecute(request);
        } catch (Exception e) {
            for (ByteBuf param : request.getParams()) {
                // Reset the reader index to re-construct the received command for debugging purposes.
                param.resetReaderIndex();
            }
            throw e;
        }
    }

    private void execute(Handler handler, Request request, Response response) throws Exception {
        handler.execute(request, response);

        if (watcher.hasWatchers()) {
            if (handler.isWatchable()) {
                for (String key : handler.getKeys(request)) {
                    watcher.increaseWatchedKeyVersion(key);
                }
            }
        }
    }

    private void executeCommand(Handler handler, Request request, Response response) {
        try {
            if (authEnabled) {
                Attribute<Boolean> authAttr = request.getSession().attr(SessionAttributes.AUTH);
                if (Boolean.TRUE.equals(authAttr.get())) {
                    // Already authenticated
                    execute(handler, request, response);
                } else {
                    // Not authenticated yet
                    if (request.getCommand().equals("AUTH") || request.getCommand().equals("HELLO")) {
                        // Execute AUTH command.
                        execute(handler, request, response);
                    } else {
                        response.writeError(RESPError.NOAUTH, "Authentication required.");
                    }
                }
            } else {
                // Authentication disabled
                execute(handler, request, response);
            }
        } catch (Exception e) {
            exceptionToRespError(request, response, e);
        }
    }

    private void executeRedisTransaction(Session session) {
        Response response = new RESP3Response(session.getCtx());
        TransactionResponse transactionResponse = new TransactionResponse(session.getCtx());
        transactionLock.writeLock().lock();
        try {
            Attribute<Boolean> redisMultiDiscarded = session.attr(SessionAttributes.MULTI_DISCARDED);
            if (redisMultiDiscarded.get()) {
                throw new ExecAbortException();
            }

            HashMap<String, Long> watchedKeys = session.attr(SessionAttributes.WATCHED_KEYS).get();
            if (watchedKeys != null) {
                for (String key : watchedKeys.keySet()) {
                    Long version = watchedKeys.get(key);
                    if (watcher.isModified(key, version)) {
                        // If keys were modified between when they were WATCHed
                        // and when the EXEC was received, the entire transaction
                        // will be aborted instead.
                        response.writeFullBulkString(FullBulkStringRedisMessage.NULL_INSTANCE);
                        return;
                    }
                }
            }

            Attribute<List<Request>> queuedCommands = session.attr(SessionAttributes.QUEUED_COMMANDS);
            for (Request request : queuedCommands.get()) {
                HandlerEntry entry = commands.get(request.getCommand());
                if (!entry.handler().isRedisCompatible()) {
                    throw new KronotopException("Redis compatibility required");
                }
                executeCommand(entry.handler(), request, transactionResponse);
            }
            transactionResponse.flush();
        } catch (ExecAbortException e) {
            response.writeError(e.getPrefix(), e.getMessage());
        } catch (Exception e) {
            response.writeError(
                    String.format("Unhandled exception during transaction handling: %s", e.getMessage())
            );
        } finally {
            transactionLock.writeLock().unlock();
        }
    }

    private void queueCommandsForRedisTransaction(Request request, Response response) {
        transactionLock.readLock().lock();
        try {
            try {
                HandlerEntry entry = commands.get(request.getCommand());
                beforeExecute(entry, request);
            } catch (Exception e) {
                Attribute<Boolean> redisMultiDiscarded = request.getSession().attr(SessionAttributes.MULTI_DISCARDED);
                redisMultiDiscarded.set(true);
                exceptionToRespError(request, response, e);
                return;
            }

            Attribute<List<Request>> queuedCommands = request.getSession().attr(SessionAttributes.QUEUED_COMMANDS);
            ReferenceCountUtil.retain(request.getRedisMessage());
            queuedCommands.get().add(request);
            response.writeQUEUED();
            response.flush();
        } finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Reads the command and its parameters from the given {@link Request} object
     * and converts them into a single string representation, where the command
     * and its parameters are separated by spaces.
     *
     * @param request the {@link Request} object containing the command and its parameters
     * @return a single string representing the command and its parameters
     */
    private String readCommandAsString(Request request) {
        List<String> command = new ArrayList<>(List.of(request.getCommand()));
        for (ByteBuf buf : request.getParams()) {
            String parameter = ProtocolMessageUtil.readAsString(buf);
            buf.resetReaderIndex();
            command.add(parameter);
        }
        return String.join(" ", command);
    }

    /**
     * Executes a Redis-compatible command within the context of a transaction.
     * Depending on the command, the method performs actions such as queuing the command,
     * ending the transaction, or throwing appropriate errors for invalid operations.
     *
     * @param session  the session associated with the current transaction
     * @param request  the request object containing the command and its parameters
     * @param response the response object used to send results or errors back to the client
     * @return a boolean indicating whether the transaction is still ongoing (true)
     * or has been discarded (false)
     */
    private boolean executeRedisCompatibleCommandInTransaction(Session session, Request request, Response response) {
        switch (request.getCommand()) {
            case MultiMessage.COMMAND:
                response.writeError("MULTI calls can not be nested");
                break;
            case WatchMessage.COMMAND:
                response.writeError("WATCH inside MULTI is not allowed");
                break;
            case ExecMessage.COMMAND:
                try {
                    executeRedisTransaction(session);
                } finally {
                    if (stashService != null) {
                        stashService.cleanupRedisTransaction(request.getSession());
                    }
                }
                break;
            default:
                if (request.getCommand().equals(DiscardMessage.COMMAND)) {
                    return false; // discard the transaction
                }
                queueCommandsForRedisTransaction(request, response);
        }
        return true; // still in the transaction boundaries
    }

    /**
     * Executes a Redis-compatible command by preparing the handler, executing the command,
     * and handling any potential resource cleanup. This method ensures thread-safe access
     * and releases resources if necessary, such as reference-counted messages.
     *
     * @param request  the request object containing the command, its parameters, and context
     * @param response the response object used to send back the result or error to the client
     * @param entry    the handler entry containing the handler and cached metadata
     */
    private void executeRedisCompatibleCommand(Request request, Response response, HandlerEntry entry) {
        transactionLock.readLock().lock();
        try {
            beforeExecute(entry, request);
            executeCommand(entry.handler(), request, response);
        } finally {
            transactionLock.readLock().unlock();
        }
    }

    /**
     * Executes a Kronotop command by preparing the handler, processing the command,
     * and handling any exceptions that occur during execution.
     *
     * @param request  the request object containing the command information and parameters
     * @param response the response object used to send the result or error back to the client
     * @param entry    the handler entry containing the handler and cached metadata
     */
    private void executeKronotopCommand(Request request, Response response, HandlerEntry entry) {
        beforeExecute(entry, request);
        executeCommand(entry.handler(), request, response);
    }

    private void channelRead0(ChannelHandlerContext ctx, Object message) {
        Session session = Session.extractSessionFromChannel(ctx.channel());

        Request request = new RESP3Request(session, message);
        Response response = new RESP3Response(ctx);

        if (logCommandForDebugging) {
            String command = readCommandAsString(request);
            LOGGER.debug("Received command: {}", command);
        }

        Attribute<Boolean> clusterInitialized = context.getMemberAttributes().attr(MemberAttributes.CLUSTER_INITIALIZED);
        if (clusterInitialized.get() == null || !clusterInitialized.get()) {
            try {
                HandlerEntry entry = commands.get(request.getCommand());
                if (entry.handler().requiresClusterInitialization()) {
                    throw new ClusterNotInitializedException();
                }
            } catch (Exception e) {
                exceptionToRespError(request, response, e);
                return;
            }
        }

        if (serverKind == ServerKind.EXTERNAL) {
            Attribute<KronotopInstanceStatus> instanceStatus = context.getMemberAttributes().attr(MemberAttributes.INSTANCE_STATUS);
            if (instanceStatus.get() != null && instanceStatus.get().equals(KronotopInstanceStatus.STOPPED)) {
                exceptionToRespError(request, response, new ServerShuttingDownException());
                return;
            }
        }

        Attribute<Boolean> multiAttr = session.attr(SessionAttributes.MULTI);
        if (Boolean.TRUE.equals(multiAttr.get())) {
            if (executeRedisCompatibleCommandInTransaction(session, request, response)) {
                return;
            }
        }

        // While a RESP2 connection is in subscription mode it may only issue a small set of
        // commands. RESP3 connections are never restricted.
        Attribute<Boolean> subscriptionMode = session.attr(SessionAttributes.SUBSCRIPTION_MODE);
        if (Boolean.TRUE.equals(subscriptionMode.get()) && session.protocolVersion() == RESPVersion.RESP2) {
            if (!isAllowedInSubscriptionMode(request.getCommand())) {
                response.writeError(String.format(
                        "Can't execute '%s': only (S)SUBSCRIBE / (S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                        request.getCommand().toLowerCase()));
                return;
            }
        }

        try {
            HandlerEntry entry = commands.get(request.getCommand());
            if (entry.handler().isRedisCompatible()) {
                executeRedisCompatibleCommand(request, response, entry);
            } else {
                executeKronotopCommand(request, response, entry);
            }
        } catch (Exception e) {
            exceptionToRespError(request, response, e);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        try {
            channelRead0(ctx, message);
        } finally {
            ReferenceCountUtil.release(message);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (cause instanceof DecoderException && cause.getCause() instanceof NotSslRecordException) {
            LOGGER.warn("Rejected non-TLS connection from {}", ctx.channel().remoteAddress());
        } else if (cause instanceof DecoderException && cause.getCause() instanceof SSLHandshakeException) {
            LOGGER.warn("TLS handshake failed from {}: {}", ctx.channel().remoteAddress(), cause.getCause().getMessage());
        } else if (!(Objects.requireNonNull(cause) instanceof SocketException)) {
            LOGGER.error("Unhandled exception caught in channel handler", cause);
        }
        ctx.close();
    }
}
