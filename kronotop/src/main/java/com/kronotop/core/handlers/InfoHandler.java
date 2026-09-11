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

package com.kronotop.core.handlers;

import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.core.InfoCollector;
import com.kronotop.core.handlers.protocol.InfoMessage;
import com.kronotop.instance.KronotopInstanceStarter;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Command(InfoMessage.COMMAND)
public class InfoHandler implements Handler {
    public static final String SERVER_SECTION = "Server";
    public static final String CLUSTER_SECTION = "Cluster";
    public static final String KRONOTOP_SECTION = "Kronotop";
    public static final String CLIENTS_SECTION = "Clients";
    public static final String MEMORY_SECTION = "Memory";
    private static final Logger LOGGER = LoggerFactory.getLogger(InfoHandler.class);
    private static final Set<String> ALL_SECTIONS = Set.of("all", "default", "everything");

    private final Context context;
    private final String version;
    private final String gitSha1;
    private final String buildTime;

    public InfoHandler(Context context) {
        this.context = context;
        Properties props = loadBuildProperties();
        this.version = KronotopInstanceStarter.resolveProperty(props.getProperty("kronotop.version"));
        this.gitSha1 = KronotopInstanceStarter.resolveProperty(props.getProperty("kronotop.git.commit"));
        this.buildTime = KronotopInstanceStarter.resolveProperty(props.getProperty("kronotop.build.time"));
    }

    private static Properties loadBuildProperties() {
        Properties props = new Properties();
        try (InputStream in = InfoHandler.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to load application.properties", e);
        }
        return props;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.INFO).set(new InfoMessage(request));
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void execute(Request request, Response response) {
        InfoMessage message = request.attr(MessageTypes.INFO).get();

        InfoCollector collector = new InfoCollector();
        collectServer(collector);
        collector.put(CLUSTER_SECTION, "cluster_enabled", 0);
        collectKronotop(collector);
        collectClients(collector);
        collectMemory(collector);
        for (KronotopService service : context.getServices()) {
            try {
                service.collectInfo(collector);
            } catch (Exception e) {
                LOGGER.error("Failed to collect INFO fields from service: {}", service.getName(), e);
            }
        }

        String body = collector.render(sectionFilter(message));
        response.writeFullBulkString(new FullBulkStringRedisMessage(
                Unpooled.buffer().writeBytes(body.getBytes(StandardCharsets.UTF_8))
        ));
    }

    private Set<String> sectionFilter(InfoMessage message) {
        if (message.getSections().isEmpty()) {
            return null;
        }
        Set<String> filter = new HashSet<>();
        for (String section : message.getSections()) {
            String name = section.toLowerCase();
            if (ALL_SECTIONS.contains(name)) {
                return null;
            }
            filter.add(name);
        }
        return filter;
    }

    private void collectServer(InfoCollector collector) {
        Member member = context.getMember();
        collector.put(SERVER_SECTION, "server_name", "kronotop");
        collector.put(SERVER_SECTION, "kronotop_version", version);
        collector.put(SERVER_SECTION, "kronotop_git_sha1", gitSha1);
        collector.put(SERVER_SECTION, "kronotop_build_time", buildTime);
        collector.put(SERVER_SECTION, "server_mode", "standalone");
        collector.put(SERVER_SECTION, "os", String.format("%s %s %s",
                System.getProperty("os.name"),
                System.getProperty("os.version"),
                System.getProperty("os.arch")
        ));
        collector.put(SERVER_SECTION, "arch_bits", System.getProperty("sun.arch.data.model"));
        collector.put(SERVER_SECTION, "java_version", System.getProperty("java.version"));
        collector.put(SERVER_SECTION, "process_id", ProcessHandle.current().pid());
        collector.put(SERVER_SECTION, "run_id", VersionstampUtil.base32HexEncode(member.getProcessId()));
        collector.put(SERVER_SECTION, "tcp_port", member.getExternalAddress().getPort());
        collector.put(SERVER_SECTION, "server_time_usec", context.now() * 1000);
        collector.put(SERVER_SECTION, "fdb_api_version", context.getConfig().getInt("foundationdb.apiversion"));
        collector.put(SERVER_SECTION, "listener0",
                listener("external", member.getExternalAddress(), member.getExternalAdvertise()));
        collector.put(SERVER_SECTION, "listener1",
                listener("internal", member.getInternalAddress(), member.getInternalAdvertise()));
    }

    private static String listener(String name, Address bind, List<Address> advertise) {
        StringBuilder sb = new StringBuilder();
        sb.append("name=").append(name);
        sb.append(",bind=").append(bind.getHost());
        sb.append(",port=").append(bind.getPort());
        for (Address address : advertise) {
            sb.append(",advertise=").append(address);
        }
        return sb.toString();
    }

    private void collectClients(InfoCollector collector) {
        int[] counters = new int[5];
        context.getSessionStore().forEach(session -> {
            if (Boolean.TRUE.equals(session.attr(SessionAttributes.BEGIN).get())) {
                counters[0]++;
            }
            if (Boolean.TRUE.equals(session.attr(SessionAttributes.MULTI).get())) {
                counters[1]++;
            }
            if (TransactionUtil.isSnapshotRead(session)) {
                counters[2]++;
            }
            if (session.getProtocolVersion() == RESPVersion.RESP3) {
                counters[4]++;
            } else {
                counters[3]++;
            }
        });
        collector.put(CLIENTS_SECTION, "connected_clients", context.getSessionStore().size());
        collector.put(CLIENTS_SECTION, "clients_in_transaction", counters[0]);
        if (context.getShardRegistry().getShardKinds().contains(ShardKind.STASH)) {
            collector.put(CLIENTS_SECTION, "clients_in_multi", counters[1]);
        }
        collector.put(CLIENTS_SECTION, "snapshot_read_clients", counters[2]);
        collector.put(CLIENTS_SECTION, "resp2_clients", counters[3]);
        collector.put(CLIENTS_SECTION, "resp3_clients", counters[4]);
    }

    private void collectMemory(InfoCollector collector) {
        Runtime runtime = Runtime.getRuntime();
        long committed = runtime.totalMemory();
        long used = committed - runtime.freeMemory();
        long max = runtime.maxMemory();
        long gcCount = 0;
        long gcTime = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            if (gc.getCollectionCount() > 0) {
                gcCount += gc.getCollectionCount();
            }
            if (gc.getCollectionTime() > 0) {
                gcTime += gc.getCollectionTime();
            }
        }
        collector.put(MEMORY_SECTION, "used_memory", used);
        collector.put(MEMORY_SECTION, "used_memory_human", KronotopInstanceStarter.formatBytes(used));
        collector.put(MEMORY_SECTION, "committed_memory", committed);
        collector.put(MEMORY_SECTION, "max_memory", max);
        collector.put(MEMORY_SECTION, "max_memory_human", KronotopInstanceStarter.formatBytes(max));
        collector.put(MEMORY_SECTION, "gc_count", gcCount);
        collector.put(MEMORY_SECTION, "gc_time_msec", gcTime);
    }

    private void collectKronotop(InfoCollector collector) {
        Member member = context.getMember();
        collector.put(KRONOTOP_SECTION, "cluster_name", context.getClusterName());
        collector.put(KRONOTOP_SECTION, "member_id", member.getId());
        collector.put(KRONOTOP_SECTION, "member_status", member.getStatus());
        collector.put(KRONOTOP_SECTION, "bucket_shards", context.getShardRegistry().getShardIds(ShardKind.BUCKET).size());
        if (context.getShardRegistry().getShardKinds().contains(ShardKind.STASH)) {
            collector.put(KRONOTOP_SECTION, "stash_shards", context.getShardRegistry().getShardIds(ShardKind.STASH).size());
        }
    }
}
