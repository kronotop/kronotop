/*
 * Copyright (c) 2023 Kronotop
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
import com.kronotop.common.KronotopException;
import com.kronotop.core.*;
import com.kronotop.core.cluster.ClusterService;
import com.kronotop.core.cluster.Member;
import com.kronotop.core.cluster.PartitionMigrationService;
import com.kronotop.core.commands.CommandDefinitions;
import com.kronotop.core.network.Address;
import com.kronotop.core.network.AddressUtil;
import com.kronotop.core.watcher.Watcher;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.ZMapService;
import com.kronotop.redis.RedisService;
import com.kronotop.server.EpollRESP2Server;
import com.kronotop.server.NioRESP2Server;
import com.kronotop.server.RESP2Server;
import com.kronotop.server.resp.Handlers;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KronotopInstanceImpl implements KronotopInstance {
    private static final String NETTY_TRANSPORT_NIO = "nio";
    private static final String NETTY_TRANSPORT_EPOLL = "epoll";
    private static final String DEFAULT_NETTY_TRANSPORT = NETTY_TRANSPORT_NIO;
    private static final Logger logger = LoggerFactory.getLogger(KronotopInstanceImpl.class);
    private static String name;
    private final ReentrantReadWriteLock mtx = new ReentrantReadWriteLock();
    private Context context;
    private Status status = Status.INITIALIZING;

    private static void greeting(Member member) {
        logger.info("pid: {} has been started", ProcessHandle.current().pid());
        logger.info("Kronotop 1.0.0 on {}/{} Java {}",
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                System.getProperty("java.version"));
        logger.info("Member id: {}", member.getId());
        logger.info("Listening TCP connections on {}", member.getAddress());
    }

    private static InetAddress getInetAddress(String host) throws UnknownHostException {
        if (host.equals("0.0.0.0")) {
            return AddressUtil.getIPv4Address();
        }
        return InetAddress.getByName(host);
    }

    public static KronotopInstance newKronotopInstance(Config config) {
        KronotopInstanceImpl kronotopInstance = new KronotopInstanceImpl();
        Thread shutdownHook = new Thread(() -> {
            try {
                kronotopInstance.shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Handlers handlers = new Handlers();
        try {
            Database database = FoundationDBFactory.newDatabase(config);

            ProcessIDGenerator processIDGenerator = new ProcessIDGeneratorImpl(config, database);
            long processID = processIDGenerator.getProcessID();

            int inetPort = config.getInt("network.port");
            String inetHost = getInetAddress(
                    config.getString("network.host")
            ).getHostAddress();
            Address address = new Address(inetHost, inetPort);
            Member member = new Member(address, processID);

            kronotopInstance.context = new ContextImpl(config, member, database);

            name = member.getId();

            Watcher watcher = new Watcher();
            kronotopInstance.context.registerService(Watcher.NAME, watcher);

            String nettyTransport = DEFAULT_NETTY_TRANSPORT;
            if (config.hasPath("network.netty.transport")) {
                nettyTransport = config.getString("network.netty.transport");
            }

            RESP2Server server;
            if (nettyTransport.equals(NETTY_TRANSPORT_NIO)) {
                server = new NioRESP2Server(kronotopInstance.context, handlers);
            } else if (nettyTransport.equals(NETTY_TRANSPORT_EPOLL)) {
                server = new EpollRESP2Server(kronotopInstance.context, handlers);
            } else {
                throw new KronotopException(String.format("invalid network.netty.transport: %s", nettyTransport));
            }

            kronotopInstance.context.registerService(server.getName(), server);
            server.start(member);

            FoundationDBService fdb = new FoundationDBService(kronotopInstance.context, handlers);
            kronotopInstance.context.registerService(FoundationDBService.NAME, fdb);

            PartitionMigrationService partitionMigrationService = new PartitionMigrationService(kronotopInstance.context);
            kronotopInstance.context.registerService(PartitionMigrationService.NAME, partitionMigrationService);
            partitionMigrationService.start();

            ClusterService clusterService = new ClusterService(kronotopInstance.context);
            kronotopInstance.context.registerService(ClusterService.NAME, clusterService);
            clusterService.start();
            clusterService.waitUntilBootstrapped();

            RedisService rdb = new RedisService(kronotopInstance.context, handlers);
            kronotopInstance.context.registerService(RedisService.NAME, rdb);

            ZMapService zmapService = new ZMapService(kronotopInstance.context, handlers);
            kronotopInstance.context.registerService(ZMapService.NAME, zmapService);

            CommandDefinitions commandDefinitions = new CommandDefinitions(handlers);
            kronotopInstance.context.registerService(CommandDefinitions.NAME, commandDefinitions);

            greeting(member);
            kronotopInstance.status = Status.RUNNING;
            logger.info("Ready to accept connections");
        } catch (Exception e) {
            e.printStackTrace();
            kronotopInstance.shutdown();
            throw new KronotopInstanceException("Failed to start Kronotop instance", e);
        }
        return kronotopInstance;
    }

    @Override
    public String getName() {
        if (name == null) {
            throw new RuntimeException("not initialized yet");
        }
        return name;
    }

    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {
        mtx.writeLock().lock();
        try {
            if (status == Status.STOPPED) {
                return;
            }
            logger.info("Shutting down Kronotop");
            for (KronotopService service : context.getServices()) {
                try {
                    service.shutdown();
                } catch (Exception e) {
                    logger.error("{} service cannot be closed due to errors", service.getName(), e);
                    continue;
                }
                logger.debug("{} service has been shutting down", service.getName());
            }
            context.getFoundationDB().close();
            status = Status.STOPPED;
        } finally {
            mtx.writeLock().unlock();
        }

        logger.info("Quit!");
    }

    public enum Status {
        INITIALIZING,
        RUNNING,
        STOPPED
    }
}