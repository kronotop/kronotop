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

package com.kronotop.instance;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.common.KronotopException;
import com.kronotop.*;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.coordinator.CoordinatorService;
import com.kronotop.cluster.sharding.ShardingService;
import com.kronotop.network.Address;
import com.kronotop.network.AddressUtil;
import com.kronotop.volume.VolumeService;
import com.kronotop.watcher.Watcher;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.redis.RedisService;
import com.kronotop.server.Handlers;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/*
It can scarcely be denied that the supreme goal of all theory is to make the irreducible basic elements as simple and as
few as possible without having to surrender the adequate representation of a single datum of experience.

Often quoted as ‘Everything should be made as simple as possible, but not simpler’

-- Albert Einstein, ‘On the Method of Theoretical Physics’, lecture delivered at Oxford, 10 June 1933
 */


/**
 * The KronotopInstance class represents a running instance of Kronotop.
 */
public class KronotopInstance {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopInstance.class);
    protected final Config config;
    protected final Handlers handlers = new Handlers();
    private final Database database;
    protected Context context;
    protected Member member;
    private volatile KronotopInstanceStatus status = KronotopInstanceStatus.INITIALIZING;

    public KronotopInstance() {
        this(ConfigFactory.load());
    }

    public KronotopInstance(Config config) {
        this.config = config;
        this.database = FoundationDBFactory.newDatabase(config);
    }

    /**
     * Retrieves the InetAddress associated with the specified host.
     *
     * @param host the host to look up
     * @return the InetAddress associated with the host
     * @throws UnknownHostException if the host address is unknown
     */
    private InetAddress getInetAddress(String host) throws UnknownHostException {
        if (host.equals("0.0.0.0")) {
            return AddressUtil.getIPv4Address();
        }
        return InetAddress.getByName(host);
    }

    /**
     * Registers the Kronotop services in the context.
     *
     * @throws InterruptedException if the thread is interrupted while registering the services
     */
    private void registerKronotopServices() throws InterruptedException {
        Watcher watcher = new Watcher();
        context.registerService(Watcher.NAME, watcher);

        FoundationDBService fdb = new FoundationDBService(context, handlers);
        context.registerService(FoundationDBService.NAME, fdb);

        VolumeService volumeService = new VolumeService(context, handlers);
        context.registerService(VolumeService.NAME, volumeService);

        ShardingService shardingService = new ShardingService(context);
        context.registerService(ShardingService.NAME, shardingService);
        shardingService.start();

        CoordinatorService coordinatorService = new CoordinatorService(context);
        context.registerService(CoordinatorService.NAME, coordinatorService);

        MembershipService membershipService = new MembershipService(context);
        context.registerService(MembershipService.NAME, membershipService);
        membershipService.start();
        membershipService.waitUntilBootstrapped();

        RedisService redisService = new RedisService(context, handlers);
        context.registerService(RedisService.NAME, redisService);
        redisService.start();
    }

    /**
     * Initializes the member of the KronotopInstance.
     *
     * @throws UnknownHostException if the host address is unknown
     */
    private void initializeMember() throws UnknownHostException {
        int inetPort = config.getInt("network.port");
        String inetHost = getInetAddress(
                config.getString("network.host")
        ).getHostAddress();
        Address address = new Address(inetHost, inetPort);

        ProcessIdGenerator processIDGenerator = new ProcessIdGeneratorImpl(config, database);
        Versionstamp processID = processIDGenerator.getProcessID();
        this.member = new Member(address, processID);
    }

    /**
     * Starts the Kronotop instance.
     *
     * <p>
     * This method performs the following steps:
     * <p>
     * 1. Initializes the member of the KronotopInstance.
     * 2. Creates a new ContextImpl object with the provided config, member, and database.
     * 3. Initializes the cluster layout.
     * 4. Registers the Kronotop services in the context.
     * 5. Sets the status of the Kronotop instance to RUNNING.
     * </p>
     *
     * @throws UnknownHostException if the host address is unknown
     * @throws InterruptedException if the thread is interrupted
     * @throws KronotopException    if an error occurs during the startup process
     */
    public void start() throws UnknownHostException, InterruptedException {
        if (getStatus().equals(KronotopInstanceStatus.RUNNING)) {
            throw new IllegalStateException("Kronotop instance is already running");
        }
        LOGGER.info("Initializing a new Kronotop instance");
        try {
            initializeMember();
            context = new ContextImpl(config, member, database);
            registerKronotopServices();
            setStatus(KronotopInstanceStatus.RUNNING);
        } catch (Exception e) {
            shutdown();
            throw e;
        }

        LOGGER.info("Ready to accept connections");
    }

    /**
     * Shuts down the Kronotop instance.
     * <p>
     * This method shuts down the Kronotop instance by performing the following steps:
     * 1. Checks the current status of the Kronotop instance. If it is already in the STOPPED status, returns immediately.
     * 2. Logs the shutdown message.
     * 3. Iterates over the list of services in the context and shuts down each service by calling its "shutdown" method. If any service throws an exception during shutdown, logs
     * an error message and continues with the next service.
     * 4. Closes the FoundationDB connection.
     * 5. Sets the status of the Kronotop instance to STOPPED.
     * </p>
     */
    public synchronized void shutdown() {
        if (status.equals(KronotopInstanceStatus.STOPPED)) {
            // Kronotop instance is already stopped
            return;
        }
        LOGGER.info("Shutting down Kronotop");
        if (context == null) {
            // Even context has not been set. Quit now. There is nothing to do. Possible error:
            // com.apple.foundationdb.FDBException: No cluster file found in current directory or default location
            return;
        }

        try {
            for (KronotopService service : context.getServices()) {
                try {
                    service.shutdown();
                } catch (Exception e) {
                    LOGGER.error("{} service cannot be closed due to errors", service.getName(), e);
                    continue;
                }
                LOGGER.debug("{} service has been shutting down", service.getName());
            }
        } finally {
            setStatus(KronotopInstanceStatus.STOPPED);
        }
    }

    /**
     * Retrieves the status of the Kronotop instance.
     *
     * @return the status of the Kronotop instance.
     */
    public KronotopInstanceStatus getStatus() {
        return status;
    }

    private void setStatus(final KronotopInstanceStatus instanceStatus) {
        this.status = instanceStatus;
        LOGGER.info("Setting instance status to {}", status);
    }

    /**
     * Retrieves the Member object representing the current member in the cluster.
     *
     * @return the Member representing the current member
     */
    public Member getMember() {
        return this.member;
    }

    /**
     * Retrieves the context of a Kronotop instance.
     *
     * @return the context of the Kronotop instance
     */
    public Context getContext() {
        return context;
    }

    /**
     * Closes the FoundationDB connection.
     */
    public void closeFoundationDBConnection() {
        FoundationDBFactory.closeDatabase();
    }
}