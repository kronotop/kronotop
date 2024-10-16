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

package com.kronotop;

import com.apple.foundationdb.Database;
import com.google.common.util.concurrent.Striped;
import com.kronotop.cluster.Member;
import com.kronotop.commands.CommandMetadata;
import com.kronotop.journal.Journal;
import com.kronotop.server.CommandHandlerRegistry;
import com.kronotop.server.ServerKind;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * The Context interface represents the context of a Kronotop instance.
 */
public interface Context {

    CommandHandlerRegistry getHandlers(ServerKind kind);

    Path getDataDir();

    /**
     * Retrieves the name of the cluster.
     *
     * @return the name of the cluster.
     */
    String getClusterName();

    /**
     * Retrieves the configuration associated with the Context.
     *
     * @return the configuration associated with the Context.
     */
    Config getConfig();

    /**
     * Retrieves the Member object representing the current member in the cluster.
     *
     * @return the Member representing the current member
     */
    Member getMember();

    /**
     * Returns the FoundationDB database associated with the Context.
     *
     * @return the FoundationDB database.
     */
    Database getFoundationDB();

    /**
     * Registers a Kronotop service in the context.
     *
     * @param id      the unique identifier for the service
     * @param service the Kronotop service to register
     */
    void registerService(String id, KronotopService service);

    /**
     * Retrieves a Kronotop service from the context using the specified service identifier.
     *
     * @param id  the unique identifier for the service
     * @param <T> the type of the service to retrieve
     * @return the Kronotop service with the specified identifier
     */
    <T> T getService(@Nonnull String id);

    /**
     * Retrieves the list of Kronotop services registered in the context.
     *
     * @return the list of Kronotop services
     */
    List<KronotopService> getServices();

    /**
     * Retrieves a Striped object that provides striped read-write locks.
     * The returned Striped object can be used to obtain a specific stripe
     * for locking purposes.
     *
     * @return a Striped object that provides striped read-write locks
     */
    Striped<ReadWriteLock> getStripedReadWriteLock();

    /**
     * Retrieves the Journal object associated with the Context.
     *
     * @return The Journal object.
     */
    Journal getJournal();

    /**
     * Registers the metadata of a command in the context.
     *
     * @param command  the name of the command
     * @param metadata the metadata of the command to register
     */
    void registerCommandMetadata(String command, CommandMetadata metadata);

    /**
     * Retrieves the metadata of commands.
     *
     * @return A map containing the metadata of commands. The keys are the names of the commands,
     * and the values are {@link CommandMetadata} objects containing the metadata of the
     * commands.
     */
    Map<String, CommandMetadata> getCommandMetadata();

    /**
     * Retrieves the KronotopDirectoryLayer object, which provides methods to open or create directory
     * subspaces within the cluster.
     *
     * @return The KronotopDirectoryLayer object.
     */
    KronotopDirectoryLayer getDirectoryLayer();

    /**
     * Registers a service context in the Kronotop instance.
     *
     * @param name    the name of the service context
     * @param context the service context to register
     * @param <T>     the type of the service context
     */
    <T> void registerServiceContext(String name, ServiceContext<T> context);

    /**
     * Retrieves the service context with the specified name.
     *
     * @param name the name of the service context.
     * @param <T>  the type of the service context.
     * @return the service context with the specified name.
     */
    <T> ServiceContext<T> getServiceContext(String name);
}