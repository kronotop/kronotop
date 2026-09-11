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
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.lettuce.core.codec.StringCodec;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class InfoHandlerTest extends BaseHandlerTest {

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
        assertTrue(info.contains("\r\n\r\n# Tasks\r\n"));
        assertTrue(info.contains("\r\n\r\n# Volume\r\n"));
        assertTrue(info.contains("\r\n\r\n# Bucket\r\n"));
        assertTrue(info.contains("\r\n\r\n# Vector\r\n"));
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
        assertTrue(info.contains("arch_bits:" + System.getProperty("sun.arch.data.model") + "\r\n"));
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
        assertTrue(fieldValue(info, "listener0").contains(",advertise="));
    }

    private static String expectedListener(String name, Address bind, List<Address> advertise) {
        StringBuilder sb = new StringBuilder();
        sb.append("name=").append(name).append(",bind=").append(bind.getHost()).append(",port=").append(bind.getPort());
        for (Address address : advertise) {
            sb.append(",advertise=").append(address);
        }
        return sb.toString();
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
        // Behavior: the Memory section reports JVM heap usage and GC totals
        String info = runInfo(getChannel(), "memory");

        long used = longField(info, "used_memory");
        long committed = longField(info, "committed_memory");
        long max = longField(info, "max_memory");
        assertTrue(used > 0);
        assertTrue(committed >= used);
        assertTrue(max >= used);
        assertTrue(longField(info, "gc_count") >= 0);
        assertTrue(longField(info, "gc_time_msec") >= 0);
        assertFalse(fieldValue(info, "used_memory_human").isBlank());
        assertFalse(fieldValue(info, "max_memory_human").isBlank());
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

    private static String fieldValue(String info, String key) {
        for (String line : info.split("\r\n")) {
            if (line.startsWith(key + ":")) {
                return line.substring(key.length() + 1);
            }
        }
        return null;
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
