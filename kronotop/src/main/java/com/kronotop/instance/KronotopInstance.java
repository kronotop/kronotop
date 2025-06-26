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

package com.kronotop.instance;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.*;
import com.kronotop.bucket.BucketContext;
import com.kronotop.bucket.BucketService;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberIdGenerator;
import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.RoutingService;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.internal.FoundationDBFactory;
import com.kronotop.internal.ProcessIdGenerator;
import com.kronotop.internal.ProcessIdGeneratorImpl;
import com.kronotop.journal.CleanupJournalTask;
import com.kronotop.network.Address;
import com.kronotop.network.AddressUtil;
import com.kronotop.redis.RedisContext;
import com.kronotop.redis.RedisService;
import com.kronotop.server.SessionService;
import com.kronotop.task.TaskService;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.ReplicationService;
import com.kronotop.watcher.Watcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.netty.util.Attribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// Kronotop was imagined and designed by Burak Sezer.

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
    public static final String KING_OF_THE_DATABASES = "Kronotop";
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopInstance.class);
    protected final Config config;
    private final Database database;
    protected Context context;
    protected Member member;
    private ScheduledFuture<?> journalCleanupTaskFuture;

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
     * Registers various Kronotop services within the context.
     * <p>
     * This method registers a list of services which are essential
     * for the proper functioning of a Kronotop instance. The order
     * of registration is crucial due to dependencies between services.
     * <p>
     * After registering the services, the method starts the MembershipService
     * and RedisService to initialize their functionalities.
     */
    private void registerKronotopServices() {
        // Registration sort is important here.

        TaskService taskService = new TaskService(context);
        context.registerService(TaskService.NAME, taskService);

        Watcher watcher = new Watcher();
        context.registerService(Watcher.NAME, watcher);

        SessionService sessionService = new SessionService(context);
        context.registerService(SessionService.NAME, sessionService);

        FoundationDBService foundationDBService = new FoundationDBService(context);
        context.registerService(FoundationDBService.NAME, foundationDBService);

        MembershipService membershipService = new MembershipService(context);
        context.registerService(MembershipService.NAME, membershipService);

        RoutingService routingService = new RoutingService(context);
        context.registerService(RoutingService.NAME, routingService);

        VolumeService volumeService = new VolumeService(context);
        context.registerService(VolumeService.NAME, volumeService);

        ReplicationService replicationService = new ReplicationService(context);
        context.registerService(ReplicationService.NAME, replicationService);

        RedisService redisService = new RedisService(context);
        context.registerService(RedisService.NAME, redisService);

        BucketService bucketService = new BucketService(context);
        context.registerService(BucketService.NAME, bucketService);

        membershipService.start();
        routingService.start();
        volumeService.start();
        replicationService.start();
        redisService.start();
        bucketService.start();
    }

    private Address getAddress(String kind) throws UnknownHostException {
        int port = config.getInt(String.format("network.%s.port", kind));
        String host = getInetAddress(
                config.getString(String.format("network.%s.host", kind))
        ).getHostAddress();
        return new Address(host, port);
    }

    /**
     * Initializes the member of the KronotopInstance.
     *
     * @throws UnknownHostException if the host address is unknown
     */
    private void initializeMember(String id) throws UnknownHostException {
        Address externalAddress = getAddress("external");
        Address internalAddress = getAddress("internal");
        ProcessIdGenerator processIDGenerator = new ProcessIdGeneratorImpl(config, database);
        Versionstamp processID = processIDGenerator.getProcessID();
        this.member = new Member(id, externalAddress, internalAddress, processID);
    }

    /**
     * Initializes the context by creating a new instance of ContextImpl using the provided config,
     * member, and database. It then registers the RedisContext as a child context in the main context.
     */
    private void initializeContext() {
        context = new ContextImpl(config, member, database);

        Attribute<KronotopInstanceStatus> status = context.getMemberAttributes().attr(MemberAttributes.INSTANCE_STATUS);
        if (status.get() != null && status.get().equals(KronotopInstanceStatus.RUNNING)) {
            throw new IllegalStateException("Kronotop instance is already running");
        }
        status.set(KronotopInstanceStatus.INITIALIZING);

        // Register child contexts here.

        // RedisContext
        RedisContext redisContext = new RedisContext(context);
        context.registerServiceContext(RedisService.NAME, redisContext);

        // BucketContext
        BucketContext bucketContext = new BucketContext(context);
        context.registerServiceContext(BucketService.NAME, bucketContext);
    }

    private Path prepareOnDiskDataDirectoryLayout() {
        String dataDir = config.getString("data_dir");
        String clusterName = config.getString("cluster.name");
        try {
            Path parentDataDir = Files.createDirectories(Path.of(dataDir, clusterName));
            File[] files = parentDataDir.toFile().listFiles();
            if (files == null) {
                throw new KronotopException("Failed to list files and directories in " + parentDataDir);
            }
            if (files.length > 1) {
                throw new KronotopException("Found more than one file or directory in " + parentDataDir);
            }
            Path directory;
            if (files.length == 0) {
                String id = MemberIdGenerator.generateId();
                directory = Files.createDirectories(Path.of(parentDataDir.toString(), id));
            } else {
                directory = Path.of(parentDataDir.toString(), files[0].toString());
            }

            // $data_dir/$cluster_id/$member_id
            return directory;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Initializes the directory structure for the Kronotop instance.
     * <p>
     * This method creates or opens essential directories in the FoundationDB
     * directory layer for the Kronotop instance's operation. It organizes the
     * directory layout as follows:
     * <p>
     * 1. Creates or opens the "cluster" directory using the current cluster name.
     * 2. Creates or opens the "metadata/prefixes" directory inside the cluster directory.
     * 3. Creates or opens a directory for the default namespace specified in the
     * configuration under "namespaces" within the cluster directory.
     * <p>
     * After setting up the directories, the transaction is committed to persist
     * the changes.
     * <p>
     * In cases where the transaction is not committed due to conflicts (error code 1020),
     * the conflict is ignored and no exception is thrown. For other exceptions, a
     * KronotopException is thrown to indicate an error.
     * <p>
     * This method ensures that the required directory structure is in place
     * to support various functionalities of the Kronotop instance.
     */
    private void initializeDirectoryLayout() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            KronotopDirectoryNode cluster = KronotopDirectory.kronotop().cluster(context.getClusterName());
            DirectoryLayer.getDefault().createOrOpen(tr, cluster.toList()).join();

            KronotopDirectoryNode prefixes = KronotopDirectory.kronotop().cluster(context.getClusterName()).metadata().prefixes();
            DirectoryLayer.getDefault().createOrOpen(tr, prefixes.toList()).join();

            String defaultNamespace = context.getConfig().getString("default_namespace");
            KronotopDirectoryNode node = KronotopDirectory.kronotop().cluster(context.getClusterName()).namespaces().namespace(defaultNamespace);
            DirectoryLayer.getDefault().createOrOpen(tr, node.toList()).join();
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException ex) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (ex.getCode() == 1020) {
                    // Ignore it.
                    return;
                }
            }
            throw new KronotopException(e.getCause());
        }
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
        LOGGER.info("Initializing a new Kronotop instance");

        Path dataDir = prepareOnDiskDataDirectoryLayout();
        String memberId = dataDir.getFileName().toString();
        try {
            initializeMember(memberId);
            initializeContext();
            initializeDirectoryLayout();
            registerKronotopServices();
            registerCleanupJournalTask();
            setStatus(KronotopInstanceStatus.RUNNING);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize the instance", e);
            shutdown();
            throw e;
        }

        LOGGER.info("Ready to accept connections");
    }

    /**
     * Registers a scheduled cleanup task for the journal.
     * <p>
     * The method retrieves the necessary configuration parameters for the cleanup task
     * (retention period and time unit) from the provided configuration. It uses these parameters
     * to create a {@link CleanupJournalTask} and schedules it to run at a fixed rate of once per day using
     * the {@link TaskService}.
     * <p>
     * If an invalid time unit is specified, an {@link IllegalArgumentException} is thrown,
     * which is caught and re-thrown as a {@link KronotopException} with a descriptive error message.
     *
     * @throws KronotopException if the time unit specified in the configuration is invalid
     */
    private void registerCleanupJournalTask() {
        TaskService taskService = context.getService(TaskService.NAME);

        long retentionPeriod = config.getLong("background_tasks.journal_cleanup_task.retention_period");
        String timeunit = config.getString("background_tasks.journal_cleanup_task.timeunit");

        try {
            CleanupJournalTask cleanupTask = new CleanupJournalTask(context.getJournal(), retentionPeriod, TaskService.timeUnitOf(timeunit));
            journalCleanupTaskFuture = taskService.scheduleAtFixedRate(cleanupTask, 1, 1, TimeUnit.DAYS);
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid timeunit: " + timeunit, e);
        }
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
        if (context == null) {
            // Even context has not been set. Quit now. There is nothing to do. Possible error:
            // com.apple.foundationdb.FDBException: No cluster file found in current directory or default location
            return;
        }

        LOGGER.info("Shutting down Kronotop");
        KronotopInstanceStatus status = getStatus();
        if (status.equals(KronotopInstanceStatus.STOPPED)) {
            // Kronotop instance is already stopped
            return;
        }
        setStatus(KronotopInstanceStatus.STOPPED);

        // Previously submitted tasks are executed, but no new tasks will be accepted.
        context.getVirtualThreadPerTaskExecutor().shutdown();

        for (KronotopService service : context.getServices().reversed()) {
            try {
                LOGGER.debug("{} service has been shutting down", service.getName());
                service.shutdown();
            } catch (Exception e) {
                LOGGER.error("{} service cannot be closed due to errors", service.getName(), e);
                continue;
            }
        }

        if (journalCleanupTaskFuture != null) {
            journalCleanupTaskFuture.cancel(true);
        }
    }

    /**
     * Retrieves the status of the Kronotop instance.
     *
     * @return the status of the Kronotop instance.
     */
    public KronotopInstanceStatus getStatus() {
        Attribute<KronotopInstanceStatus> status = context.getMemberAttributes().attr(MemberAttributes.INSTANCE_STATUS);
        if (status.get() == null) {
            throw new IllegalStateException("Kronotop instance status is not set");
        }
        return status.get();
    }

    private void setStatus(final KronotopInstanceStatus status) {
        context.getMemberAttributes().attr(MemberAttributes.INSTANCE_STATUS).set(status);
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