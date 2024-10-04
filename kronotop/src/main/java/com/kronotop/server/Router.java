/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.apple.foundationdb.Transaction;
import com.kronotop.Context;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.network.ClientIDGenerator;
import com.kronotop.redis.RedisService;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.impl.RespRequest;
import com.kronotop.server.impl.RespResponse;
import com.kronotop.server.impl.TransactionResponse;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.watcher.Watcher;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Router class that extends ChannelDuplexHandler for handling commands.
 */
public class Router extends ChannelDuplexHandler {
    static final String execCommand = "EXEC";
    static final String discardCommand = "DISCARD";
    static final String multiCommand = "MULTI";
    static final String watchCommand = "WATCH";
    private static final Logger logger = LoggerFactory.getLogger(Router.class);
    private final Context context;
    private final ReadWriteLock redisTransactionLock = new ReentrantReadWriteLock(true);
    private final Watcher watcher;
    private final RedisService redisService;
    private final String defaultNamespace;
    CommandHandlerRegistry commands;
    Boolean authEnabled = false;

    public Router(Context context, CommandHandlerRegistry commands) {
        this.context = context;
        this.commands = commands;
        this.watcher = context.getService(Watcher.NAME);
        this.redisService = context.getService(RedisService.NAME);

        if (context.getConfig().hasPath("auth.requirepass") || context.getConfig().hasPath("auth.users")) {
            authEnabled = true;
        }

        defaultNamespace = context.getConfig().getString("default_namespace");
        if (defaultNamespace.isEmpty() || defaultNamespace.isBlank()) {
            throw new IllegalArgumentException("default namespace is empty or blank");
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
        ctx.channel().attr(ChannelAttributes.OPEN_NAMESPACES).set(new HashMap<>());
        ctx.channel().attr(ChannelAttributes.CURRENT_NAMESPACE).set(defaultNamespace);

        Attribute<Boolean> autoCommitAttr = ctx.channel().attr(ChannelAttributes.AUTO_COMMIT);
        autoCommitAttr.set(false);

        Attribute<List<Request>> queuedCommands = ctx.channel().attr(ChannelAttributes.QUEUED_COMMANDS);
        queuedCommands.set(new ArrayList<>());

        Attribute<Boolean> redisMulti = ctx.channel().attr(ChannelAttributes.REDIS_MULTI);
        redisMulti.set(false);

        Attribute<Boolean> redisMultiDiscarded = ctx.channel().attr(ChannelAttributes.REDIS_MULTI_DISCARDED);
        redisMultiDiscarded.set(false);

        Attribute<Long> clientID = ctx.channel().attr(ChannelAttributes.CLIENT_ID);
        clientID.set(ClientIDGenerator.getAndIncrement());

        super.channelRegistered(ctx);
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        watcher.cleanupChannelHandlerContext(ctx);

        Attribute<Boolean> beginAttr = ctx.channel().attr(ChannelAttributes.BEGIN);
        if (Boolean.TRUE.equals(beginAttr.get())) {
            Attribute<Transaction> transactionAttr = ctx.channel().attr(ChannelAttributes.TRANSACTION);
            Transaction tx = transactionAttr.get();
            tx.close();
            logger.debug("An incomplete transaction has been closed.");
        }

        super.channelUnregistered(ctx);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        Attribute<Boolean> autoCommitAttr = ctx.channel().attr(ChannelAttributes.AUTO_COMMIT);
        if (autoCommitAttr.get() != null && !Boolean.FALSE.equals(autoCommitAttr.get())) {
            Attribute<Transaction> transaction = channel.attr(ChannelAttributes.TRANSACTION);
            transaction.get().close();
        }
        autoCommitAttr.set(false);
        super.channelReadComplete(ctx);
    }

    private void exceptionToRespError(Request request, Response response, Exception exception) {
        if (exception instanceof KronotopException) {
            KronotopException exp = (KronotopException) exception;
            response.writeError(exp.getPrefix(), exp.getMessage());
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
            logger.debug("Unhandled error while serving command: {}", command, exception);
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
                Attribute<Boolean> authAttr = response.getChannelContext().channel().attr(ChannelAttributes.AUTH);
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

    private void executeRedisTransaction(ChannelHandlerContext ctx) {
        Response response = new RespResponse(ctx);
        TransactionResponse transactionResponse = new TransactionResponse(ctx);
        redisTransactionLock.writeLock().lock();
        try {
            Attribute<Boolean> redisMultiDiscarded = ctx.channel().attr(ChannelAttributes.REDIS_MULTI_DISCARDED);
            if (redisMultiDiscarded.get()) {
                throw new ExecAbortException();
            }

            HashMap<String, Long> watchedKeys = ctx.channel().attr(ChannelAttributes.WATCHED_KEYS).get();
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

            Attribute<List<Request>> queuedCommands = ctx.channel().attr(ChannelAttributes.QUEUED_COMMANDS);
            for (Request request : queuedCommands.get()) {
                Handler handler = commands.get(request.getCommand());
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
            redisTransactionLock.writeLock().unlock();
        }
    }

    private void queueCommandsForRedisTransaction(Request request, Response response) {
        redisTransactionLock.readLock().lock();
        try {
            try {
                Handler handler = commands.get(request.getCommand());
                beforeExecute(handler, request);
            } catch (Exception e) {
                Attribute<Boolean> redisMultiDiscarded = response.getChannelContext().channel().attr(ChannelAttributes.REDIS_MULTI_DISCARDED);
                redisMultiDiscarded.set(true);
                exceptionToRespError(request, response, e);
                return;
            }

            Attribute<List<Request>> queuedCommands = response.getChannelContext().channel().attr(ChannelAttributes.QUEUED_COMMANDS);
            queuedCommands.get().add(request);
            response.writeQUEUED();
            response.flush();
        } finally {
            redisTransactionLock.readLock().unlock();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Request request = new RespRequest(ctx, msg);
        Response response = new RespResponse(ctx);

        String command = request.getCommand();
        Attribute<Boolean> redisMulti = ctx.channel().attr(ChannelAttributes.REDIS_MULTI);
        if (Boolean.TRUE.equals(redisMulti.get())) {
            switch (command) {
                case multiCommand:
                    response.writeError("MULTI calls can not be nested");
                    return;
                case watchCommand:
                    response.writeError("WATCH inside MULTI is not allowed");
                    return;
                case execCommand:
                    try {
                        executeRedisTransaction(ctx);
                    } finally {
                        redisService.cleanupRedisTransaction(ctx);
                    }
                    return;
                default:
                    if (!command.equals(discardCommand)) {
                        queueCommandsForRedisTransaction(request, response);
                        return;
                    }
                    break;
            }
        }

        try {
            redisTransactionLock.readLock().lock();
            Handler handler = commands.get(command);
            beforeExecute(handler, request);
            executeCommand(handler, request, response);
        } catch (Exception e) {
            exceptionToRespError(request, response, e);
        } finally {
            redisTransactionLock.readLock().unlock();
            ReferenceCountUtil.release(msg);
        }
    }
}
