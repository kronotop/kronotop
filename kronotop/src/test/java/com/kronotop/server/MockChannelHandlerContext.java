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

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;

import java.net.SocketAddress;

public class MockChannelHandlerContext implements ChannelHandlerContext {
    private final EmbeddedChannel embeddedChannel;

    public MockChannelHandlerContext(EmbeddedChannel embeddedChannel) {
        this.embeddedChannel = embeddedChannel;
    }

    public EmbeddedChannel embeddedChannel() {
        return embeddedChannel;
    }

    /**
     * Return the {@link Channel} which is bound to the {@link ChannelHandlerContext}.
     */
    @Override
    public Channel channel() {
        return embeddedChannel;
    }

    /**
     * Returns the {@link EventExecutor} which is used to execute an arbitrary task.
     */
    @Override
    public EventExecutor executor() {
        return null;
    }

    /**
     * The unique name of the {@link ChannelHandlerContext}.The name was used when then {@link ChannelHandler}
     * was added to the {@link ChannelPipeline}. This name can also be used to access the registered
     * {@link ChannelHandler} from the {@link ChannelPipeline}.
     */
    @Override
    public String name() {
        return null;
    }

    /**
     * The {@link ChannelHandler} that is bound this {@link ChannelHandlerContext}.
     */
    @Override
    public ChannelHandler handler() {
        return null;
    }

    /**
     * Return {@code true} if the {@link ChannelHandler} which belongs to this context was removed
     * from the {@link ChannelPipeline}. Note that this method is only meant to be called from with in the
     * {@link EventLoop}.
     */
    @Override
    public boolean isRemoved() {
        return false;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelRegistered() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelUnregistered() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelActive() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelInactive() {
        return null;
    }

    /**
     * @param cause
     * @return
     */
    @Override
    public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
        return null;
    }

    /**
     * @param evt
     * @return
     */
    @Override
    public ChannelHandlerContext fireUserEventTriggered(Object evt) {
        return null;
    }

    /**
     * @param msg
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelRead(Object msg) {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelReadComplete() {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext fireChannelWritabilityChanged() {
        return null;
    }

    /**
     * Request to bind to the given {@link SocketAddress} and notify the {@link ChannelFuture} once the operation
     * completes, either because the operation was successful or because of an error.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#bind(ChannelHandlerContext, SocketAddress, ChannelPromise)} method
     * called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param localAddress
     */
    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return null;
    }

    /**
     * Request to connect to the given {@link SocketAddress} and notify the {@link ChannelFuture} once the operation
     * completes, either because the operation was successful or because of an error.
     * <p>
     * If the connection fails because of a connection timeout, the {@link ChannelFuture} will get failed with
     * a {@link ConnectTimeoutException}. If it fails because of connection refused a {@link ConnectException}
     * will be used.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#connect(ChannelHandlerContext, SocketAddress, SocketAddress, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param remoteAddress
     */
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return null;
    }

    /**
     * Request to connect to the given {@link SocketAddress} while bind to the localAddress and notify the
     * {@link ChannelFuture} once the operation completes, either because the operation was successful or because of
     * an error.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#connect(ChannelHandlerContext, SocketAddress, SocketAddress, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param remoteAddress
     * @param localAddress
     */
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return null;
    }

    /**
     * Request to disconnect from the remote peer and notify the {@link ChannelFuture} once the operation completes,
     * either because the operation was successful or because of an error.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#disconnect(ChannelHandlerContext, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     */
    @Override
    public ChannelFuture disconnect() {
        return null;
    }

    /**
     * Request to close the {@link Channel} and notify the {@link ChannelFuture} once the operation completes,
     * either because the operation was successful or because of
     * an error.
     * <p>
     * After it is closed it is not possible to reuse it again.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#close(ChannelHandlerContext, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     */
    @Override
    public ChannelFuture close() {
        return null;
    }

    /**
     * Request to deregister from the previous assigned {@link EventExecutor} and notify the
     * {@link ChannelFuture} once the operation completes, either because the operation was successful or because of
     * an error.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#deregister(ChannelHandlerContext, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     */
    @Override
    public ChannelFuture deregister() {
        return null;
    }

    /**
     * Request to bind to the given {@link SocketAddress} and notify the {@link ChannelFuture} once the operation
     * completes, either because the operation was successful or because of an error.
     * <p>
     * The given {@link ChannelPromise} will be notified.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#bind(ChannelHandlerContext, SocketAddress, ChannelPromise)} method
     * called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param localAddress
     * @param promise
     */
    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return null;
    }

    /**
     * Request to connect to the given {@link SocketAddress} and notify the {@link ChannelFuture} once the operation
     * completes, either because the operation was successful or because of an error.
     * <p>
     * The given {@link ChannelFuture} will be notified.
     *
     * <p>
     * If the connection fails because of a connection timeout, the {@link ChannelFuture} will get failed with
     * a {@link ConnectTimeoutException}. If it fails because of connection refused a {@link ConnectException}
     * will be used.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#connect(ChannelHandlerContext, SocketAddress, SocketAddress, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param remoteAddress
     * @param promise
     */
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return null;
    }

    /**
     * Request to connect to the given {@link SocketAddress} while bind to the localAddress and notify the
     * {@link ChannelFuture} once the operation completes, either because the operation was successful or because of
     * an error.
     * <p>
     * The given {@link ChannelPromise} will be notified and also returned.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#connect(ChannelHandlerContext, SocketAddress, SocketAddress, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param remoteAddress
     * @param localAddress
     * @param promise
     */
    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return null;
    }

    /**
     * Request to disconnect from the remote peer and notify the {@link ChannelFuture} once the operation completes,
     * either because the operation was successful or because of an error.
     * <p>
     * The given {@link ChannelPromise} will be notified.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#disconnect(ChannelHandlerContext, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param promise
     */
    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
        return null;
    }

    /**
     * Request to close the {@link Channel} and notify the {@link ChannelFuture} once the operation completes,
     * either because the operation was successful or because of
     * an error.
     * <p>
     * After it is closed it is not possible to reuse it again.
     * The given {@link ChannelPromise} will be notified.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#close(ChannelHandlerContext, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param promise
     */
    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return null;
    }

    /**
     * Request to deregister from the previous assigned {@link EventExecutor} and notify the
     * {@link ChannelFuture} once the operation completes, either because the operation was successful or because of
     * an error.
     * <p>
     * The given {@link ChannelPromise} will be notified.
     * <p>
     * This will result in having the
     * {@link ChannelOutboundHandler#deregister(ChannelHandlerContext, ChannelPromise)}
     * method called of the next {@link ChannelOutboundHandler} contained in the {@link ChannelPipeline} of the
     * {@link Channel}.
     *
     * @param promise
     */
    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext read() {
        return null;
    }

    /**
     * Request to write a message via this {@link ChannelHandlerContext} through the {@link ChannelPipeline}.
     * This method will not request to actual flush, so be sure to call {@link #flush()}
     * once you want to request to flush all pending data to the actual transport.
     *
     * @param msg
     */
    @Override
    public ChannelFuture write(Object msg) {
        return null;
    }

    /**
     * Request to write a message via this {@link ChannelHandlerContext} through the {@link ChannelPipeline}.
     * This method will not request to actual flush, so be sure to call {@link #flush()}
     * once you want to request to flush all pending data to the actual transport.
     *
     * @param msg
     * @param promise
     */
    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        return null;
    }

    /**
     * @return
     */
    @Override
    public ChannelHandlerContext flush() {
        embeddedChannel.flush();
        return this;
    }

    /**
     * Shortcut for call {@link #write(Object, ChannelPromise)} and {@link #flush()}.
     *
     * @param msg
     * @param promise
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return embeddedChannel.writeAndFlush(msg, promise);
    }

    /**
     * Shortcut for call {@link #write(Object)} and {@link #flush()}.
     *
     * @param msg
     */
    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        return embeddedChannel.writeAndFlush(msg);
    }

    /**
     * Return a new {@link ChannelPromise}.
     */
    @Override
    public ChannelPromise newPromise() {
        return null;
    }

    /**
     * Return an new {@link ChannelProgressivePromise}
     */
    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return null;
    }

    /**
     * Create a new {@link ChannelFuture} which is marked as succeeded already. So {@link ChannelFuture#isSuccess()}
     * will return {@code true}. All {@link FutureListener} added to it will be notified directly. Also
     * every call of blocking methods will just return without blocking.
     */
    @Override
    public ChannelFuture newSucceededFuture() {
        return null;
    }

    /**
     * Create a new {@link ChannelFuture} which is marked as failed already. So {@link ChannelFuture#isSuccess()}
     * will return {@code false}. All {@link FutureListener} added to it will be notified directly. Also
     * every call of blocking methods will just return without blocking.
     *
     * @param cause
     */
    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return null;
    }

    /**
     * Return a special ChannelPromise which can be reused for different operations.
     * <p>
     * It's only supported to use
     * it for {@link ChannelOutboundInvoker#write(Object, ChannelPromise)}.
     * </p>
     * <p>
     * Be aware that the returned {@link ChannelPromise} will not support most operations and should only be used
     * if you want to save an object allocation for every write operation. You will not be able to detect if the
     * operation  was complete, only if it failed as the implementation will call
     * {@link ChannelPipeline#fireExceptionCaught(Throwable)} in this case.
     * </p>
     * <strong>Be aware this is an expert feature and should be used with care!</strong>
     */
    @Override
    public ChannelPromise voidPromise() {
        return null;
    }

    /**
     * Return the assigned {@link ChannelPipeline}
     */
    @Override
    public ChannelPipeline pipeline() {
        return null;
    }

    /**
     * Return the assigned {@link ByteBufAllocator} which will be used to allocate {@link ByteBuf}s.
     */
    @Override
    public ByteBufAllocator alloc() {
        return null;
    }

    /**
     * @param key
     * @deprecated Use {@link Channel#attr(AttributeKey)}
     */
    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
        return null;
    }

    /**
     * @param key
     * @deprecated Use {@link Channel#hasAttr(AttributeKey)}
     */
    @Override
    public <T> boolean hasAttr(AttributeKey<T> key) {
        return false;
    }
}
