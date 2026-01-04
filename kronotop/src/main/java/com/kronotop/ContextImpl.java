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

package com.kronotop;

import com.apple.foundationdb.Database;
import com.kronotop.bucket.BucketMetadataCache;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.client.InternalClientPool;
import com.kronotop.commands.CommandMetadata;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.journal.Journal;
import com.kronotop.server.CommandHandlerRegistry;
import com.kronotop.server.ServerKind;
import com.kronotop.server.SessionStore;
import com.kronotop.worker.WorkerRegistry;
import com.typesafe.config.Config;
import io.netty.util.AttributeMap;
import io.netty.util.DefaultAttributeMap;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Default implementation of the {@link Context} interface. Manages the lifecycle of services,
 * command handlers, and shared resources for a Kronotop instance. This class is instantiated
 * once per instance and provides thread-safe access to all registered components.
 *
 * <p>Services are stored in insertion order using a {@link LinkedHashMap} to ensure
 * deterministic shutdown ordering.</p>
 */
public class ContextImpl implements Context {
    private final Config config;
    private final Member member;
    private final Database database;
    private final ExecutorService virtualThreadPerTaskExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final DirectorySubspaceCache directorySubspaceCache;
    private final EnumMap<ServerKind, CommandHandlerRegistry> handlers = new EnumMap<>(ServerKind.class);
    private final LinkedHashMap<String, KronotopService> services = new LinkedHashMap<>();
    private final String clusterName;
    private final Journal journal;
    private final WorkerRegistry workerRegistry;
    private final ConcurrentHashMap<String, CommandMetadata> commandMetadata = new ConcurrentHashMap<>();
    private final Map<String, CommandMetadata> unmodifiableCommandMetadata = Collections.unmodifiableMap(commandMetadata);
    private final ConcurrentHashMap<String, ServiceContext<?>> contexts = new ConcurrentHashMap<>();
    private final Path dataDir;
    private final String defaultNamespace;
    private final AttributeMap memberAttributes = new DefaultAttributeMap();
    private final InternalClientPool internalClientPool;

    // Shortcut to CachedTimeService, it's used in the hot path for Bucket and Redis commands
    private volatile CachedTimeService cachedTimeService;

    // Direct field access for minimal overhead on the hot path.
    // Initialized only once to avoid runtime lookup and casting costs.
    private BucketMetadataCache bucketMetadataCache;

    private final SessionStore sessionStore = new SessionStore();

    /**
     * Creates a new context for a Kronotop instance.
     *
     * @param config   the application configuration (must contain "default_namespace" and "cluster.name")
     * @param member   the cluster member this context belongs to
     * @param database the FoundationDB database connection
     * @throws MissingConfigException   if required configuration keys are missing
     * @throws IllegalArgumentException if default namespace is empty or blank
     */
    public ContextImpl(Config config, Member member, Database database) {
        if (config.hasPath("default_namespace")) {
            defaultNamespace = config.getString("default_namespace");
            if (defaultNamespace.isEmpty() || defaultNamespace.isBlank()) {
                throw new IllegalArgumentException("default namespace is empty or blank");
            }
        } else {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }

        if (config.hasPath("cluster.name")) {
            clusterName = config.getString("cluster.name");
        } else {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }

        this.config = config;
        this.member = member;
        this.database = database;
        this.journal = new Journal(config, database);
        this.workerRegistry = new WorkerRegistry();
        this.dataDir = Path.of(config.getString("data_dir"), clusterName, member.getId());
        this.directorySubspaceCache = new DirectorySubspaceCache(clusterName, database);
        this.internalClientPool = new InternalClientPool();

        for (ServerKind kind : ServerKind.values()) {
            this.handlers.put(kind, new CommandHandlerRegistry());
        }
    }

    @Override
    public AttributeMap getMemberAttributes() {
        return memberAttributes;
    }

    @Override
    public ExecutorService getVirtualThreadPerTaskExecutor() {
        return virtualThreadPerTaskExecutor;
    }

    @Override
    public String getDefaultNamespace() {
        return defaultNamespace;
    }

    @Override
    public DirectorySubspaceCache getDirectorySubspaceCache() {
        return directorySubspaceCache;
    }

    @Override
    public CommandHandlerRegistry getHandlers(ServerKind serverKind) {
        return handlers.get(serverKind);
    }

    @Override
    public Path getDataDir() {
        return dataDir;
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public Database getFoundationDB() {
        return database;
    }

    @Override
    public void registerService(@Nonnull String id, @Nonnull KronotopService service) {
        // Registration sort is important, this is why we use LinkedHashMap to store services.
        services.putIfAbsent(id, service);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getService(@Nonnull String id) {
        return (T) services.get(id);
    }

    public List<KronotopService> getServices() {
        return new ArrayList<>(services.values());
    }

    @Override
    public Journal getJournal() {
        return journal;
    }

    @Override
    public void registerCommandMetadata(String command, CommandMetadata metadata) {
        commandMetadata.put(command, metadata);
    }

    @Override
    public Map<String, CommandMetadata> getCommandMetadata() {
        return unmodifiableCommandMetadata;
    }

    @Override
    public <T> void registerServiceContext(String name, ServiceContext<T> context) {
        synchronized (contexts) {
            if (contexts.containsKey(name)) {
                throw new KronotopException(String.format("ServiceContext '%s' already registered", name));
            }
            contexts.put(name, context);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> ServiceContext<T> getServiceContext(String name) {
        return (ServiceContext<T>) contexts.get(name);
    }

    @Override
    public BucketMetadataCache getBucketMetadataCache() {
        return bucketMetadataCache;
    }

    @Override
    public void setBucketMetadataCache(BucketMetadataCache cache) {
        synchronized (this) {
            if (this.bucketMetadataCache != null) {
                throw new IllegalStateException("BucketMetadataCache has already been set");
            }
            this.bucketMetadataCache = cache;
        }
    }

    @Override
    public long now() {
        if (cachedTimeService == null) {
            cachedTimeService = getService(CachedTimeService.NAME);
        }
        return cachedTimeService.getCurrentTimeInMilliseconds();
    }

    @Override
    public SessionStore getSessionStore() {
        return sessionStore;
    }

    @Override
    public WorkerRegistry getWorkerRegistry() {
        return workerRegistry;
    }

    @Override
    public InternalClientPool getInternalClientPool() {
        return internalClientPool;
    }
}