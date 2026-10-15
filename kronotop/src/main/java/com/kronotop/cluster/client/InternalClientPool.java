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

package com.kronotop.cluster.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.kronotop.cluster.Member;
import com.kronotop.network.Address;
import com.kronotop.server.TLSConfig;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SslOptions;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.ProtocolVersion;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Connection pool for internal cluster communication. Maintains separate pools
 * for String and ByteArray codecs, keyed by cluster Member with TTL-based eviction.
 * All clients share one ClientResources, so eviction never stops an event loop.
 */
public class InternalClientPool {
    private static final long DEFAULT_IDLE_TIMEOUT_MINUTES = 10;

    private final LoadingCache<Member, PooledConnection<String, String>> stringPool;
    private final LoadingCache<Member, PooledConnection<byte[], byte[]>> byteArrayPool;
    private final TLSConfig tlsConfig;
    private final ClientResources clientResources;

    public InternalClientPool() {
        this(TLSConfig.disabled(), DEFAULT_IDLE_TIMEOUT_MINUTES);
    }

    public InternalClientPool(TLSConfig tlsConfig) {
        this(tlsConfig, DEFAULT_IDLE_TIMEOUT_MINUTES);
    }

    public InternalClientPool(TLSConfig tlsConfig, long idleTimeoutMinutes) {
        this.tlsConfig = tlsConfig;
        this.clientResources = DefaultClientResources.create();

        this.stringPool = CacheBuilder.newBuilder()
                .expireAfterAccess(idleTimeoutMinutes, TimeUnit.MINUTES)
                .removalListener((RemovalListener<Member, PooledConnection<String, String>>) notification -> {
                    PooledConnection<String, String> pooled = notification.getValue();
                    if (pooled != null) {
                        pooled.close();
                    }
                })
                .build(new StringConnectionLoader());

        this.byteArrayPool = CacheBuilder.newBuilder()
                .expireAfterAccess(idleTimeoutMinutes, TimeUnit.MINUTES)
                .removalListener((RemovalListener<Member, PooledConnection<byte[], byte[]>>) notification -> {
                    PooledConnection<byte[], byte[]> pooled = notification.getValue();
                    if (pooled != null) {
                        pooled.close();
                    }
                })
                .build(new ByteArrayConnectionLoader());
    }

    /**
     * Gets or creates a String-codec connection for the given member.
     *
     * @param member the target cluster member
     * @return a stateful connection for cluster communication
     */
    public StatefulInternalConnection<String, String> get(Member member) {
        try {
            return stringPool.get(member).connection();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get connection for member: " + member.getId(), e);
        }
    }

    /**
     * Gets or creates a ByteArray-codec connection for the given member.
     *
     * @param member the target cluster member
     * @return a stateful connection for cluster communication
     */
    public StatefulInternalConnection<byte[], byte[]> getByteArray(Member member) {
        try {
            return byteArrayPool.get(member).connection();
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to get connection for member: " + member.getId(), e);
        }
    }

    /**
     * Returns the ClientResources shared by every client that this pool creates.
     */
    public ClientResources clientResources() {
        return clientResources;
    }

    /**
     * Evicts connections for the given member from both pools.
     *
     * @param member the member whose connections should be evicted
     */
    public void evict(Member member) {
        stringPool.invalidate(member);
        byteArrayPool.invalidate(member);
    }

    /**
     * Shuts down the pool, closing all connections.
     */
    public void shutdown() {
        stringPool.invalidateAll();
        byteArrayPool.invalidateAll();
        clientResources.shutdown();
    }

    private RedisClient createClient(Member member) {
        Address address = member.primaryInternalAdvertise();
        RedisURI uri = RedisURI.builder()
                .withHost(address.getHost())
                .withPort(address.getPort())
                .withSsl(tlsConfig.enabled())
                .withVerifyPeer(tlsConfig.enabled())
                .build();

        RedisClient client = RedisClient.create(clientResources, uri);

        if (tlsConfig.enabled()) {
            SslOptions.Builder sslBuilder = SslOptions.builder();
            if (tlsConfig.caFile() != null) {
                sslBuilder.trustManager(tlsConfig.caFile());
            }
            client.setOptions(ClientOptions.builder()
                    .sslOptions(sslBuilder.build())
                    .protocolVersion(ProtocolVersion.RESP3)
                    .build());
        }

        return client;
    }

    /**
     * Holds both the connection and the underlying RedisClient for proper cleanup.
     */
    private record PooledConnection<K, V>(
            RedisClient client,
            StatefulInternalConnection<K, V> connection
    ) {
        void close() {
            try {
                connection.close();
            } finally {
                client.shutdownAsync();
            }
        }
    }

    private class StringConnectionLoader extends CacheLoader<Member, PooledConnection<String, String>> {
        @Override
        public @Nonnull PooledConnection<String, String> load(@Nonnull Member member) {
            RedisClient client = createClient(member);
            StatefulInternalConnection<String, String> connection =
                    InternalClient.connect(client, StringCodec.UTF8);
            return new PooledConnection<>(client, connection);
        }
    }

    private class ByteArrayConnectionLoader extends CacheLoader<Member, PooledConnection<byte[], byte[]>> {
        @Override
        public @Nonnull PooledConnection<byte[], byte[]> load(@Nonnull Member member) {
            RedisClient client = createClient(member);
            StatefulInternalConnection<byte[], byte[]> connection =
                    InternalClient.connect(client, ByteArrayCodec.INSTANCE);
            return new PooledConnection<>(client, connection);
        }
    }
}
