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
import com.kronotop.instance.KronotopInstanceStatus;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.metrics.NetworkMetrics;
import com.kronotop.server.impl.RESPRequest;
import com.kronotop.server.impl.RESPResponse;
import com.kronotop.server.impl.TransactionResponse;
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
 * Serves RESP commands on a client channel. It checks authentication and argument counts, runs
 * the command handler, and converts exceptions to RESP errors. It also queues the commands sent
 * between MULTI and EXEC.
 */
public class KronotopChannelDuplexHandler extends ChannelDuplexHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopChannelDuplexHandler.class);

    private final Context context;
    private final ReadWriteLock transactionLock = new ReentrantReadWriteLock(true);
    private final Watcher watcher;
    private final StashService stashService;
    private final ZMapService zmapService;
    private final CommandHandlerRegistry commands;
    private final ServerKind serverKind;
    private final boolean logCommandForDebugging;
    private boolean authEnabled = false;
    private final NetworkMetrics metrics;

    public KronotopChannelDuplexHandler(Context context, CommandHandlerRegistry commands, ServerKind serverKind) {
        this.context = context;
        this.commands = commands;
        this.serverKind = serverKind;
        this.watcher = context.getService(Watcher.NAME);
        this.stashService = context.getService(StashService.NAME);
        this.zmapService = context.getService(ZMapService.NAME);

        Config config = context.getConfig();
        this.logCommandForDebugging = config.hasPath("log_command_for_debugging") && config.getBoolean("log_command_for_debugging");

        if (config.hasPath("auth.requirepass") || config.hasPath("auth.users")) {
            authEnabled = true;
        }
        this.metrics = context.getRuntimeMetrics().getNetworkMetrics();
    }

    private void checkMaximumArgumentCount(HandlerEntry entry, Request request) throws WrongNumberOfArgumentsException {
        if (entry.hasMaximumArgumentCount()) {
            if (request.getArguments().size() > entry.maximumArgumentCount()) {
                throw new WrongNumberOfArgumentsException(
                        String.format("wrong number of arguments for '%s' command", request.getCommand())
                );
            }
        }
    }

    private void checkMinimumArgumentCount(HandlerEntry entry, Request request) throws WrongNumberOfArgumentsException {
        if (entry.hasMinimumArgumentCount()) {
            if (request.getArguments().size() < entry.minimumArgumentCount()) {
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
        session.channelUnregistered();
        super.channelUnregistered(ctx);
    }

    /**
     * Removes the client from the waiters of the key it is blocked on with ZWATCH, if any.
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
                for (ByteBuf buf : request.getArguments()) {
                    byte[] rawArgument = ProtocolMessageUtil.readAsByteArray(buf);
                    buf.resetReaderIndex();
                    String argument = new String(rawArgument);
                    command.append(argument).append(" ");
                }
                LOGGER.debug("Unhandled error while serving command: {}: '{}'", command, exception.getMessage());
                response.writeError(exception.getMessage());
            }
        }
    }

    private void beforeExecute(HandlerEntry entry, Request request) {
        checkMinimumArgumentCount(entry, request);
        checkMaximumArgumentCount(entry, request);
        try {
            entry.handler().beforeExecute(request);
        } catch (Exception e) {
            for (ByteBuf argument : request.getArguments()) {
                // Reset the reader index to re-construct the received command for debugging purposes.
                argument.resetReaderIndex();
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
                if (request.getSession().isAuthenticated()) {
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
        Response response = new RESPResponse(session.getCtx(), session);
        TransactionResponse transactionResponse = new TransactionResponse(session.getCtx(), session.getProtocolVersion());
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
                        response.writeNULL();
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
     * Joins the command and its arguments into one string, separated by spaces.
     *
     * @param request the request that holds the command and its arguments
     * @return the command and its arguments as one string
     */
    private String readCommandAsString(Request request) {
        List<String> command = new ArrayList<>(List.of(request.getCommand()));
        for (ByteBuf buf : request.getArguments()) {
            String argument = ProtocolMessageUtil.readAsString(buf);
            buf.resetReaderIndex();
            command.add(argument);
        }
        return String.join(" ", command);
    }

    /**
     * Handles a command received after MULTI.
     *
     * @param session  the session that owns the transaction
     * @param request  the request that holds the command and its arguments
     * @param response the response used to reply to the client
     * @return false if the command is DISCARD, true otherwise
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
     * Checks and runs a command under the read lock of the transaction lock.
     *
     * @param request  the request that holds the command and its arguments
     * @param response the response used to reply to the client
     * @param entry    the handler entry of the command
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
     * Checks and runs a command without taking the transaction lock.
     *
     * @param request  the request that holds the command and its arguments
     * @param response the response used to reply to the client
     * @param entry    the handler entry of the command
     */
    private void executeKronotopCommand(Request request, Response response, HandlerEntry entry) {
        beforeExecute(entry, request);
        executeCommand(entry.handler(), request, response);
    }

    private void channelRead0(ChannelHandlerContext ctx, Object message) {
        Session session = Session.extractSessionFromChannel(ctx.channel());

        Request request = new RESPRequest(session, message);
        Response response = new RESPResponse(ctx, session);

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

        try {
            if (serverKind == ServerKind.EXTERNAL) {
                context.getInFlight().enter();
            }
            HandlerEntry entry = commands.get(request.getCommand());
            if (entry.handler().isRedisCompatible()) {
                executeRedisCompatibleCommand(request, response, entry);
            } else {
                executeKronotopCommand(request, response, entry);
            }
        } catch (Exception e) {
            exceptionToRespError(request, response, e);
        } finally {
            if (serverKind == ServerKind.EXTERNAL) {
                context.getInFlight().exit();
            }
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object message) {
        try {
            channelRead0(ctx, message);
        } finally {
            metrics.increaseTotalCommandsProcessed(serverKind);
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
