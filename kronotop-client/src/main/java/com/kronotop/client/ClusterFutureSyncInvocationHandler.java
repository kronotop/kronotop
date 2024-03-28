/*
 * Copyright 2016-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kronotop.client;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.internal.AbstractInvocationHandler;
import io.lettuce.core.internal.DefaultMethods;
import io.lettuce.core.internal.Futures;
import io.lettuce.core.internal.TimeoutProvider;
import io.lettuce.core.protocol.RedisCommand;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Invocation-handler to synchronize API calls which use Futures as backend. This class leverages the need to implement a full
 * sync class which just delegates every request.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Mark Paluch
 * @since 3.0
 */
@SuppressWarnings("unchecked")
class ClusterFutureSyncInvocationHandler<K, V> extends AbstractInvocationHandler {

    private final StatefulConnection<K, V> connection;

    private final TimeoutProvider timeoutProvider;

    private final Class<?> asyncCommandsInterface;

    private final Class<?> nodeSelectionInterface;

    private final Class<?> nodeSelectionCommandsInterface;

    private final Object asyncApi;

    private final Map<Method, Method> apiMethodCache = new ConcurrentHashMap<>(RedisClusterCommands.class.getMethods().length, 1);

    private final Map<Method, Method> connectionMethodCache = new ConcurrentHashMap<>(5, 1);

    private final Map<Method, MethodHandle> methodHandleCache = new ConcurrentHashMap<>(5, 1);

    ClusterFutureSyncInvocationHandler(StatefulConnection<K, V> connection, Class<?> asyncCommandsInterface, Class<?> nodeSelectionInterface, Class<?> nodeSelectionCommandsInterface, Object asyncApi) {
        this.connection = connection;
        this.timeoutProvider = new TimeoutProvider(() -> connection.getOptions().getTimeoutOptions(), () -> connection.getTimeout().toNanos());
        this.asyncCommandsInterface = asyncCommandsInterface;
        this.nodeSelectionInterface = nodeSelectionInterface;
        this.nodeSelectionCommandsInterface = nodeSelectionCommandsInterface;
        this.asyncApi = asyncApi;
    }

    private static MethodHandle lookupDefaultMethod(Method method) {

        try {
            return DefaultMethods.lookupMethodHandle(method);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * @see AbstractInvocationHandler#handleInvocation(Object, Method, Object[])
     */
    @Override
    protected Object handleInvocation(Object proxy, Method method, Object[] args) throws Throwable {

        try {

            if (method.isDefault()) {
                return methodHandleCache.computeIfAbsent(method, ClusterFutureSyncInvocationHandler::lookupDefaultMethod).bindTo(proxy).invokeWithArguments(args);
            }

            if (method.getName().equals("getConnection") && args.length > 0) {
                return getConnection(method, args);
            }

            Method targetMethod = apiMethodCache.computeIfAbsent(method, key -> {

                try {
                    return asyncApi.getClass().getMethod(key.getName(), key.getParameterTypes());
                } catch (NoSuchMethodException e) {
                    throw new IllegalStateException(e);
                }
            });

            Object result = targetMethod.invoke(asyncApi, args);

            if (result instanceof RedisFuture) {
                RedisFuture<?> command = (RedisFuture<?>) result;
                if (!method.getName().equals("exec") && !method.getName().equals("multi")) {
                    if (connection instanceof StatefulRedisConnection && ((StatefulRedisConnection) connection).isMulti()) {
                        return null;
                    }
                }
                return Futures.awaitOrCancel(command, getTimeoutNs(command), TimeUnit.NANOSECONDS);
            }

            return result;

        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private long getTimeoutNs(RedisFuture<?> command) {

        if (command instanceof RedisCommand) {
            return timeoutProvider.getTimeoutNs((RedisCommand) command);
        }

        return connection.getTimeout().toNanos();
    }

    private Object getConnection(Method method, Object[] args) throws Exception {

        Method targetMethod = connectionMethodCache.computeIfAbsent(method, this::lookupMethod);

        Object result = targetMethod.invoke(connection, args);
        if (result instanceof StatefulRedisClusterConnection) {
            StatefulRedisClusterConnection<K, V> connection = (StatefulRedisClusterConnection<K, V>) result;
            return connection.sync();
        }

        if (result instanceof StatefulRedisConnection) {
            StatefulRedisConnection<K, V> connection = (StatefulRedisConnection<K, V>) result;
            return connection.sync();
        }

        throw new IllegalArgumentException("Cannot call method " + method);
    }

    private Method lookupMethod(Method key) {
        try {
            return connection.getClass().getMethod(key.getName(), key.getParameterTypes());
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
