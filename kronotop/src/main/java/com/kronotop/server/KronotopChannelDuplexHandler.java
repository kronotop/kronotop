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

import com.apple.foundationdb.FDBException;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.handlers.transactions.protocol.DiscardMessage;
import com.kronotop.redis.handlers.transactions.protocol.ExecMessage;
import com.kronotop.redis.handlers.transactions.protocol.MultiMessage;
import com.kronotop.redis.handlers.transactions.protocol.WatchMessage;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.impl.RESP3Request;
import com.kronotop.server.impl.RESP3Response;
import com.kronotop.server.impl.TransactionResponse;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.watcher.Watcher;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final RedisService redisService;
    private final CommandHandlerRegistry commands;
    private final boolean logCommandForDebugging;
    private boolean authEnabled = false;

    public KronotopChannelDuplexHandler(Context context, CommandHandlerRegistry commands) {
        this.context = context;
        this.commands = commands;
        this.watcher = context.getService(Watcher.NAME);
        this.redisService = context.getService(RedisService.NAME);

        Config config = context.getConfig();
        this.logCommandForDebugging = config.hasPath("log_command_for_debugging") && config.getBoolean("log_command_for_debugging");

        if (config.hasPath("auth.requirepass") || config.hasPath("auth.users")) {
            authEnabled = true;
        }
    }

    private void checkMaximumParameterCount(Handler handler, Request request) throws WrongNumberOfArgumentsException {
        MaximumParameterCount annotation = handler.getClass().getAnnotation(MaximumParameterCount.class);
        if (annotation != null) {
            if (request.getParams().size() > annotation.value()) {
                throw new WrongNumberOfArgumentsException(
                        String.format("wrong number of arguments for '%s' command", request.getCommand())
                );
            }
        }
    }

    private void checkMinimumParameterCount(Handler handler, Request request) throws WrongNumberOfArgumentsException {
        MinimumParameterCount annotation = handler.getClass().getAnnotation(MinimumParameterCount.class);
        if (annotation != null) {
            if (request.getParams().size() < annotation.value()) {
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
        session.channelUnregistered();
        super.channelUnregistered(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        Session session = Session.extractSessionFromChannel(ctx.channel());
        session.channelReadComplete();
        super.channelReadComplete(ctx);
    }

    private void exceptionToRespError(Request request, Response response, Exception exception) {
        if (exception instanceof KronotopException exp) {
            if (exp.getCause() != null) {
                response.writeError(exp.getPrefix(), exp.getCause().getMessage());
            } else {
                response.writeError(exp.getPrefix(), exp.getMessage());
            }
        } else if (exception instanceof CompletionException) {
            if (exception.getCause() instanceof FDBException) {
                String message = RESPError.decapitalize(exception.getCause().getMessage());
                if (message.equalsIgnoreCase(RESPError.TRANSACTION_TOO_OLD_MESSAGE)) {
                    response.writeError(RESPError.TRANSACTIONOLD, message);
                } else if (message.equalsIgnoreCase(RESPError.TRANSACTION_BYTE_LIMIT_MESSAGE)) {
                    response.writeError(RESPError.TRANSACTION, message);
                } else {
                    response.writeError(message);
                }
                return;
            }
            throw new KronotopException(exception);
        } else {
            StringBuilder command = new StringBuilder();
            command.append(request.getCommand()).append(" ");
            for (ByteBuf buf : request.getParams()) {
                byte[] rawParam = new byte[buf.readableBytes()];
                buf.readBytes(rawParam);
                String param = new String(rawParam);
                command.append(param).append(" ");
            }
            LOGGER.debug("Unhandled error while serving command: {}", command, exception);
            response.writeError(exception.getMessage());
        }
    }

    private void beforeExecute(Handler handler, Request request) {
        checkMinimumParameterCount(handler, request);
        checkMaximumParameterCount(handler, request);
        try {
            handler.beforeExecute(request);
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
                Handler handler = commands.get(request.getCommand());
                if (!handler.isRedisCompatible()) {
                    throw new KronotopException("Redis compatibility required");
                }
                executeCommand(handler, request, transactionResponse);
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
                Handler handler = commands.get(request.getCommand());
                beforeExecute(handler, request);
            } catch (Exception e) {
                Attribute<Boolean> redisMultiDiscarded = request.getSession().attr(SessionAttributes.MULTI_DISCARDED);
                redisMultiDiscarded.set(true);
                exceptionToRespError(request, response, e);
                return;
            }

            Attribute<List<Request>> queuedCommands = request.getSession().attr(SessionAttributes.QUEUED_COMMANDS);
            queuedCommands.get().add(request);
            response.writeQUEUED();
            response.flush();
        } finally {
            transactionLock.readLock().unlock();
        }
    }

    private void logCommandForDebugging(Request request) {
        List<String> command = new ArrayList<>(List.of(request.getCommand()));
        for (ByteBuf buf : request.getParams()) {
            byte[] parameter = new byte[buf.readableBytes()];
            buf.readBytes(parameter);
            buf.resetReaderIndex();
            command.add(new String(parameter));
        }
        LOGGER.debug("Received command: {}", String.join(" ", command));
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
                    redisService.cleanupRedisTransaction(request.getSession());
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
     * @param handler  the handler responsible for processing the specific command
     */
    private void executeRedisCompatibleCommand(Request request, Response response, Handler handler) {
        transactionLock.readLock().lock();
        try {
            beforeExecute(handler, request);
            executeCommand(handler, request, response);
        } finally {
            transactionLock.readLock().unlock();
            if (request.getRedisMessage() instanceof ReferenceCounted message) {
                message.release();
            }
        }
    }

    /**
     * Executes a Kronotop command by preparing the handler, processing the command,
     * and handling any exceptions that occur during execution.
     *
     * @param request  the request object containing the command information and parameters
     * @param response the response object used to send the result or error back to the client
     * @param handler  the handler responsible for processing the specific command
     */
    private void executeKronotopCommand(Request request, Response response, Handler handler) {
        try {
            beforeExecute(handler, request);
            executeCommand(handler, request, response);
        } finally {
            if (request.getRedisMessage() instanceof ReferenceCounted message) {
                message.release();
            }
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        Session session = Session.extractSessionFromChannel(ctx.channel());

        Request request = new RESP3Request(session, message);
        Response response = new RESP3Response(ctx);

        if (logCommandForDebugging) {
            logCommandForDebugging(request);
        }

        Attribute<Boolean> multiAttr = session.attr(SessionAttributes.MULTI);
        if (Boolean.TRUE.equals(multiAttr.get())) {
            if (executeRedisCompatibleCommandInTransaction(session, request, response)) {
                return;
            }
        }

        try {
            Handler handler = commands.get(request.getCommand());
            if (handler.isRedisCompatible()) {
                executeRedisCompatibleCommand(request, response, handler);
            } else {
                executeKronotopCommand(request, response, handler);
            }
        } catch (Exception e) {
            exceptionToRespError(request, response, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (!(Objects.requireNonNull(cause) instanceof SocketException)) {
            LOGGER.error("Unhandled exception caught in channel handler", cause);
        }
        ctx.close();
    }
}
