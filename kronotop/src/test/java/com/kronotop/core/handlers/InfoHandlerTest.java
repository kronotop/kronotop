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

import com.kronotop.BaseHandlerTest;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.SnapshotReadArgs;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.network.Address;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class InfoHandlerTest extends BaseHandlerTest {

    private static String expectedListener(String name, Address bind, List<Address> advertise) {
        StringBuilder sb = new StringBuilder();
        sb.append("name=").append(name).append(",bind=").append(bind.getHost()).append(",port=").append(bind.getPort());
        for (Address address : advertise) {
            sb.append(",advertise=").append(address);
        }
        return sb.toString();
    }

    private static String fieldValue(String info, String key) {
        for (String line : info.split("\r\n")) {
            if (line.startsWith(key + ":")) {
                return line.substring(key.length() + 1);
            }
        }
        return null;
    }

    private String runInfo(EmbeddedChannel channel, String... sections) {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(1 + sections.length).append("\r\n$4\r\nINFO\r\n");
        for (String section : sections) {
            sb.append('$').append(section.length()).append("\r\n").append(section).append("\r\n");
        }
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(sb.toString().getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        return ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.US_ASCII);
    }

    @Test
    void shouldReportStandaloneMode() {
        // Behavior: INFO reports server_mode:standalone and cluster_enabled:0 so
        // clients do not switch to slot-based cluster routing
        String info = runInfo(getChannel());

        assertTrue(info.contains("server_mode:standalone\r\n"));
        assertTrue(info.contains("cluster_enabled:0\r\n"));
        assertFalse(info.contains("redis_mode:"));
    }

    @Test
    void shouldReturnServerAndClusterSections() {
        // Behavior: INFO without arguments returns the Server, Cluster and Kronotop
        // sections separated by an empty line
        String info = runInfo(getChannel());

        assertTrue(info.startsWith("# Server\r\n"));
        assertTrue(info.contains("\r\n\r\n# Cluster\r\n"));
        assertTrue(info.contains("\r\n\r\n# Kronotop\r\n"));
        assertTrue(info.contains("\r\n\r\n# Clients\r\n"));
        assertTrue(info.contains("\r\n\r\n# Memory\r\n"));
        assertTrue(info.contains("\r\n\r\n# Traffic\r\n"));
        assertTrue(info.contains("\r\n\r\n# Tasks\r\n"));
        assertTrue(info.contains("\r\n\r\n# Volume\r\n"));
        assertTrue(info.contains("\r\n\r\n# Bucket\r\n"));
        assertTrue(info.contains("\r\n\r\n# Vector\r\n"));
        assertTrue(info.indexOf("# Memory") < info.indexOf("# Traffic"));
        assertTrue(info.indexOf("# Traffic") < info.indexOf("# Tasks"));
        assertTrue(info.indexOf("# Tasks") < info.indexOf("# Volume"));
        assertTrue(info.indexOf("# Volume") < info.indexOf("# Bucket"));
        assertTrue(info.indexOf("# Bucket") < info.indexOf("# Vector"));
        assertTrue(info.contains("kronotop_version:"));
        assertTrue(info.contains("os:"));
    }

    @Test
    void shouldReadBuildInfoFromApplicationProperties() {
        // Behavior: version, git commit and build time come from application.properties;
        // a missing or unresolved value is reported as unknown, never null
        String info = runInfo(getChannel(), "server");

        for (String key : new String[]{"kronotop_version", "kronotop_git_sha1", "kronotop_build_time"}) {
            String value = fieldValue(info, key);
            assertNotNull(value, key);
            assertFalse(value.isBlank(), key);
            assertNotEquals("null", value, key);
            assertFalse(value.startsWith("${"), key);
        }
    }

    @Test
    void shouldReportServerFields() {
        // Behavior: the Server section carries JVM, process and config facts
        String info = runInfo(getChannel(), "server");

        Member member = context.getMember();
        assertTrue(info.contains("server_name:kronotop\r\n"));
        assertTrue(info.contains("server_mode:standalone\r\n"));
        assertTrue(info.contains("java_version:" + System.getProperty("java.version") + "\r\n"));
        assertTrue(info.contains("arch_bits:64\r\n"));
        assertTrue(info.contains("process_id:" + ProcessHandle.current().pid() + "\r\n"));
        assertTrue(info.contains("run_id:" + VersionstampUtil.base32HexEncode(member.getProcessId()) + "\r\n"));
        assertTrue(info.contains("tcp_port:" + member.getExternalAddress().getPort() + "\r\n"));
        assertTrue(info.contains("fdb_api_version:" + context.getConfig().getInt("foundationdb.apiversion") + "\r\n"));
        assertTrue(info.contains("server_time_usec:"));
    }

    @Test
    void shouldReportListeners() {
        // Behavior: listener0 is the external listener and listener1 the internal one.
        // Each line carries the bind address, the bound port and every advertised
        // address of the running member, not the raw config
        String info = runInfo(getChannel(), "server");

        Member member = context.getMember();
        assertEquals(expectedListener("external", member.getExternalAddress(), member.getExternalAdvertise()),
                fieldValue(info, "listener0"));
        assertEquals(expectedListener("internal", member.getInternalAddress(), member.getInternalAdvertise()),
                fieldValue(info, "listener1"));
        assertFalse(member.getExternalAdvertise().isEmpty());
        assertTrue(Objects.requireNonNull(fieldValue(info, "listener0")).contains(",advertise="));
    }

    @Test
    void shouldReportRealBoundPortWhenConfigPortIsZero() {
        // Behavior: the test config binds port 0; INFO reports the port that was
        // actually bound, not 0
        String info = runInfo(getChannel(), "server");

        assertEquals(0, context.getConfig().getInt("network.external.port"));
        assertNotEquals("0", fieldValue(info, "tcp_port"));
    }

    private int intField(String info, String key) {
        String value = fieldValue(info, key);
        assertNotNull(value, key);
        return Integer.parseInt(value);
    }

    private long longField(String info, String key) {
        String value = fieldValue(info, key);
        assertNotNull(value, key);
        return Long.parseLong(value);
    }

    @Test
    void shouldCountConnectedClients() {
        // Behavior: connected_clients grows by one for every new channel
        int before = intField(runInfo(getChannel(), "clients"), "connected_clients");

        EmbeddedChannel second = newChannel();
        int after = intField(runInfo(second, "clients"), "connected_clients");

        assertEquals(before + 1, after);
    }

    @Test
    void shouldCountClientsInTransaction() {
        // Behavior: a session inside BEGIN is counted in clients_in_transaction
        EmbeddedChannel second = newChannel();
        int before = intField(runInfo(getChannel(), "clients"), "clients_in_transaction");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.begin().encode(buf);
        runCommand(second, buf);

        int after = intField(runInfo(getChannel(), "clients"), "clients_in_transaction");
        assertEquals(before + 1, after);
    }

    @Test
    void shouldCountClientsInMulti() {
        // Behavior: a session inside MULTI is counted in clients_in_multi
        EmbeddedChannel second = newChannel();
        int before = intField(runInfo(getChannel(), "clients"), "clients_in_multi");

        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.multi().encode(buf);
        runCommand(second, buf);

        int after = intField(runInfo(getChannel(), "clients"), "clients_in_multi");
        assertEquals(before + 1, after);
    }

    @Test
    void shouldCountSnapshotReadClients() {
        // Behavior: a session with SNAPSHOTREAD ON is counted in snapshot_read_clients
        EmbeddedChannel second = newChannel();
        int before = intField(runInfo(getChannel(), "clients"), "snapshot_read_clients");

        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.snapshotRead(SnapshotReadArgs.Builder.on()).encode(buf);
        runCommand(second, buf);

        int after = intField(runInfo(getChannel(), "clients"), "snapshot_read_clients");
        assertEquals(before + 1, after);
    }

    @Test
    void shouldCountProtocolVersions() {
        // Behavior: resp2_clients and resp3_clients split every connected client by
        // its negotiated protocol and add up to connected_clients
        EmbeddedChannel second = newChannel();
        int resp3Before = intField(runInfo(getChannel(), "clients"), "resp3_clients");

        switchProtocol(second, RESPVersion.RESP3);

        String info = runInfo(getChannel(), "clients");
        int resp2 = intField(info, "resp2_clients");
        int resp3 = intField(info, "resp3_clients");
        assertEquals(resp3Before + 1, resp3);
        assertEquals(intField(info, "connected_clients"), resp2 + resp3);
    }

    @Test
    void shouldReportMemoryFields() {
        // Behavior: the Memory section reports JVM heap and non-heap usage,
        // direct and mapped buffer pools, Netty allocator usage, total allocated
        // bytes and GC totals; non-heap max, buffer pool memory and total allocated
        // bytes may be -1 when the JVM leaves them undefined
        String info = runInfo(getChannel(), "memory");

        long heapUsed = longField(info, "heap_used_memory");
        long heapCommitted = longField(info, "heap_committed_memory");
        long heapMax = longField(info, "heap_max_memory");
        assertTrue(heapUsed > 0);
        assertTrue(heapCommitted >= heapUsed);
        assertTrue(heapMax >= heapUsed);

        long nonHeapUsed = longField(info, "non_heap_used_memory");
        long nonHeapCommitted = longField(info, "non_heap_committed_memory");
        assertTrue(nonHeapUsed > 0);
        assertTrue(nonHeapCommitted >= nonHeapUsed);
        assertTrue(longField(info, "non_heap_max_memory") >= -1);

        for (String pool : List.of("direct_buffer", "mapped_buffer")) {
            assertTrue(longField(info, pool + "_count") >= 0, pool);
            assertTrue(longField(info, pool + "_used_memory") >= -1, pool);
            assertTrue(longField(info, pool + "_total_capacity") >= 0, pool);
        }

        assertTrue(longField(info, "netty_used_direct_memory") >= 0);
        assertTrue(longField(info, "netty_used_heap_memory") >= 0);
        assertTrue(longField(info, "total_allocated_memory") >= -1);

        assertTrue(longField(info, "gc_count") >= 0);
        assertTrue(longField(info, "gc_time_msec") >= 0);
        assertTrue(longField(info, "gc_freed_memory") >= 0);
        for (String field : List.of(
                "heap_used_memory_human", "heap_committed_memory_human", "heap_max_memory_human",
                "non_heap_used_memory_human", "non_heap_committed_memory_human",
                "non_heap_max_memory_human",
                "total_allocated_memory_human",
                "direct_buffer_used_memory_human", "direct_buffer_total_capacity_human",
                "mapped_buffer_used_memory_human", "mapped_buffer_total_capacity_human",
                "netty_used_direct_memory_human", "netty_used_heap_memory_human", "gc_freed_memory_human")) {
            assertFalse(Objects.requireNonNull(fieldValue(info, field)).isBlank(), field);
        }
    }

    @Test
    void shouldReportTrafficFields() {
        // Behavior: the Traffic section reports command and byte totals for the external and
        // internal listeners; succeeded plus failed equals processed; every byte field has a _human pair
        String info = runInfo(getChannel(), "traffic");

        for (String prefix : List.of("external", "internal")) {
            long processed = longField(info, prefix + "_total_commands_processed");
            long succeeded = longField(info, prefix + "_commands_succeeded");
            long failed = longField(info, prefix + "_commands_failed");
            assertTrue(processed >= 0, prefix);
            assertTrue(failed >= 0, prefix);
            assertEquals(processed, succeeded + failed, prefix);
            assertTrue(longField(info, prefix + "_read_bytes") >= 0, prefix);
            assertTrue(longField(info, prefix + "_written_bytes") >= 0, prefix);
            for (String field : List.of(prefix + "_read_bytes_human", prefix + "_written_bytes_human")) {
                assertFalse(Objects.requireNonNull(fieldValue(info, field)).isBlank(), field);
            }
        }
    }

    @Test
    void shouldCountProcessedCommands() {
        // Behavior: external_total_commands_processed grows by at least one for every command
        // the external listener receives, the INFO call itself included
        long first = longField(runInfo(getChannel(), "traffic"), "external_total_commands_processed");
        long second = longField(runInfo(getChannel(), "traffic"), "external_total_commands_processed");

        assertTrue(second >= first + 1);
    }

    @Test
    void shouldCountFailedCommands() {
        // Behavior: a command answered with an error grows external_commands_failed by one and
        // leaves external_commands_succeeded unchanged; INFO itself counts as succeeded
        String before = runInfo(getChannel(), "traffic");
        long failedBefore = longField(before, "external_commands_failed");
        long succeededBefore = longField(before, "external_commands_succeeded");

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*1\r\n$14\r\nNOSUCHCOMMANDX\r\n".getBytes(StandardCharsets.US_ASCII));
        assertInstanceOf(ErrorRedisMessage.class, runCommand(getChannel(), buf));

        String after = runInfo(getChannel(), "traffic");
        assertEquals(failedBefore + 1, longField(after, "external_commands_failed"));
        // The first INFO call succeeded; the failing command did not
        assertEquals(succeededBefore + 1, longField(after, "external_commands_succeeded"));
    }

    @Test
    void shouldReportTasksFields() {
        // Behavior: the Tasks section counts registered tasks and the ones running right now
        String info = runInfo(getChannel(), "tasks");

        int total = intField(info, "task_count");
        int running = intField(info, "running_tasks");
        assertTrue(total >= 0);
        assertTrue(running >= 0);
        assertTrue(running <= total);
    }

    @Test
    void shouldReportVolumeFields() {
        // Behavior: the Volume section lists every open volume with its status, vacuum state
        // and operation counters; a fresh instance has no vacuum running
        String info = runInfo(getChannel(), "volume");

        int count = intField(info, "volume_count");
        int bucketShards = context.getShardRegistry().getShardIds(ShardKind.BUCKET).size();
        assertTrue(count >= bucketShards);

        String line = fieldValue(info, "volume0");
        assertNotNull(line);
        assertTrue(line.startsWith("name="));
        assertTrue(line.contains(",status=READWRITE,"));
        assertTrue(line.contains(",vacuum_active=0,"));
        for (String key : new String[]{"appends", "deletes", "updates", "gets",
                "bytes_appended", "bytes_read", "segments_created"}) {
            assertTrue(line.contains("," + key + "="), key);
        }
        assertNull(fieldValue(info, "volume" + count));
    }

    @Test
    void shouldReportBucketFields() {
        // Behavior: the Bucket section reports the plan cache size and index maintenance totals
        String info = runInfo(getChannel(), "bucket");

        assertTrue(intField(info, "plan_cache_size") >= 0);
        assertTrue(intField(info, "index_maintenance_workers") >= 0);
        assertTrue(longField(info, "index_maintenance_processed_entries") >= 0);
        assertTrue(longField(info, "index_maintenance_retried_conflicts") >= 0);
        assertTrue(longField(info, "index_maintenance_last_run") >= 0);
    }

    @Test
    void shouldReportVectorFields() {
        // Behavior: the Vector section reports the open on-heap graph indexes and their heap usage
        String info = runInfo(getChannel(), "vector");

        assertTrue(intField(info, "vector_indexes") >= 0);
        assertTrue(longField(info, "vector_bytes_used") >= 0);
    }

    @Test
    void shouldKeepClusterSectionMinimal() {
        // Behavior: the Cluster section holds only cluster_enabled, the same shape
        // clients expect from a standalone server
        String info = runInfo(getChannel(), "cluster");

        assertEquals("# Cluster\r\ncluster_enabled:0\r\n", info);
    }

    @Test
    void shouldReportKronotopFields() {
        // Behavior: the Kronotop section reports this member, membership counts and
        // shard counts for a single-member cluster
        String info = runInfo(getChannel(), "kronotop");

        int bucketShards = context.getShardRegistry().getShardIds(ShardKind.BUCKET).size();
        int stashShards = context.getShardRegistry().getShardIds(ShardKind.STASH).size();
        assertTrue(info.contains("cluster_name:" + context.getClusterName() + "\r\n"));
        assertTrue(info.contains("member_id:" + context.getMember().getId() + "\r\n"));
        assertTrue(info.contains("member_status:RUNNING\r\n"));
        assertTrue(info.contains("known_members:1\r\n"));
        assertTrue(info.contains("alive_members:1\r\n"));
        assertTrue(info.contains("bucket_shards:" + bucketShards + "\r\n"));
        assertTrue(info.contains("stash_shards:" + stashShards + "\r\n"));
        assertTrue(info.contains("primary_shards:" + (bucketShards + stashShards) + "\r\n"));
        assertTrue(info.contains("standby_shards:0\r\n"));
    }

    @Test
    void shouldFilterBySection() {
        // Behavior: INFO with a section name returns only that section, matched
        // without regard to case
        String info = runInfo(getChannel(), "SERVER");

        assertTrue(info.startsWith("# Server\r\n"));
        assertFalse(info.contains("# Cluster"));
    }

    @Test
    void shouldReturnMultipleRequestedSections() {
        // Behavior: several section names return each matching section
        String info = runInfo(getChannel(), "cluster", "server");

        assertTrue(info.contains("# Server\r\n"));
        assertTrue(info.contains("# Cluster\r\n"));
        assertFalse(info.contains("# Kronotop"));
    }

    @Test
    void shouldReturnEmptyForUnknownSection() {
        // Behavior: an unknown section name returns an empty bulk string
        String info = runInfo(getChannel(), "nope");

        assertEquals("", info);
    }

    @Test
    void shouldReturnAllForAllKeyword() {
        // Behavior: all, default and everything return every section even when
        // combined with an unknown name
        for (String keyword : new String[]{"all", "default", "everything"}) {
            String info = runInfo(getChannel(), "nope", keyword);
            assertTrue(info.contains("# Server\r\n"), keyword);
            assertTrue(info.contains("# Cluster\r\n"), keyword);
        }
    }
}
