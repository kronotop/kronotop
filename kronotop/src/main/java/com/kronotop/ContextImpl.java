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
import com.kronotop.cluster.Member;
import com.kronotop.cluster.client.InternalConnectionPool;
import com.kronotop.commands.CommandMetadata;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.journal.Journal;
import com.kronotop.server.CommandHandlerRegistry;
import com.kronotop.server.ServerKind;
import com.typesafe.config.Config;
import io.lettuce.core.codec.ByteArrayCodec;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The ContextImpl class represents the implementation of the Context interface in the Kronotop system.
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
    private final ConcurrentHashMap<String, CommandMetadata> commandMetadata = new ConcurrentHashMap<>();
    private final Map<String, CommandMetadata> unmodifiableCommandMetadata = Collections.unmodifiableMap(commandMetadata);
    private final ConcurrentHashMap<String, ServiceContext<?>> contexts = new ConcurrentHashMap<>();
    private final Path dataDir;
    private final InternalConnectionPool<byte[], byte[]> internalConnectionPool;
    private final String defaultNamespace;

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
        this.dataDir = Path.of(config.getString("data_dir"), clusterName, member.getId());
        this.directorySubspaceCache = new DirectorySubspaceCache(clusterName, database);
        this.internalConnectionPool = new InternalConnectionPool<>(ByteArrayCodec.INSTANCE);

        for (ServerKind kind : ServerKind.values()) {
            this.handlers.put(kind, new CommandHandlerRegistry());
        }
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
    public InternalConnectionPool<byte[], byte[]> getInternalConnectionPool() {
        return internalConnectionPool;
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
}