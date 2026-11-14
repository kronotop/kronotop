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

package com.kronotop.instance;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class KronotopInstanceStarterTest {

    @TempDir
    Path tempDir;

    private static KronotopInstanceStarter parse(String... args) {
        KronotopInstanceStarter starter = new KronotopInstanceStarter();
        new CommandLine(starter).parseArgs(args);
        return starter;
    }

    private String writeConfigFile(String content) throws IOException {
        Path file = tempDir.resolve("kronotop.conf");
        Files.writeString(file, content);
        return file.toString();
    }

    @Test
    void shouldOverrideConfigWithOptions() {
        // Behavior: with no config file, --data-dir, --cluster-name, --fdb-cluster-file, --external-port and
        // --netty-transport set data_dir, cluster.name, foundationdb.clusterfile, network.external.port and the
        // netty transport of both the external and internal listeners.
        Config config = parse(
                "--data-dir", "/tmp/kronotop-node",
                "--cluster-name", "production",
                "--fdb-cluster-file", "/etc/foundationdb/fdb.cluster",
                "--external-port", "6000",
                "--netty-transport", "epoll"
        ).buildConfig();

        assertEquals("/tmp/kronotop-node", config.getString("data_dir"));
        assertEquals("production", config.getString("cluster.name"));
        assertEquals("/etc/foundationdb/fdb.cluster", config.getString("foundationdb.clusterfile"));
        assertEquals(6000, config.getInt("network.external.port"));
        assertEquals("epoll", config.getString("network.external.netty.transport"));
        assertEquals("epoll", config.getString("network.internal.netty.transport"));
    }

    @Test
    void shouldSplitAdvertiseList() {
        // Behavior: --external-advertise with a comma separated value "10.0.0.1:5484,10.0.0.2:5484" becomes a two item
        // list at network.external.advertise, in the given order.
        Config config = parse("--external-advertise", "10.0.0.1:5484,10.0.0.2:5484").buildConfig();

        assertEquals(List.of("10.0.0.1:5484", "10.0.0.2:5484"), config.getStringList("network.external.advertise"));
    }

    @Test
    void shouldDropEmptyAdvertiseEntries() {
        // Behavior: --external-advertise with the value "10.0.0.1:5484,,10.0.0.2:5484" has an empty entry between
        // two commas. The empty entry is dropped, and network.external.advertise holds the two addresses in the
        // given order.
        Config config = parse("--external-advertise", "10.0.0.1:5484,,10.0.0.2:5484").buildConfig();

        assertEquals(List.of("10.0.0.1:5484", "10.0.0.2:5484"), config.getStringList("network.external.advertise"));
    }

    @Test
    void shouldPreferOptionsOverConfigFile() throws IOException {
        // Behavior: a config file sets cluster.name and data_dir, and --cluster-name is also given. The option value
        // wins for cluster.name, and data_dir keeps the value from the config file.
        String file = writeConfigFile("cluster.name = \"from-file\"\ndata_dir = \"/data/from-file\"\n");

        Config config = parse("-c", file, "--cluster-name", "from-option").buildConfig();

        assertEquals("from-option", config.getString("cluster.name"));
        assertEquals("/data/from-file", config.getString("data_dir"));
    }

    @Test
    void shouldFailWhenConfigFileIsMissing() {
        // Behavior: -c points to a file that does not exist, so building the config throws a ConfigException.
        KronotopInstanceStarter starter = parse("-c", tempDir.resolve("missing.conf").toString());

        assertThrows(ConfigException.class, starter::buildConfig);
    }

    @Test
    void shouldPassCheckConfigWithDefaults() {
        // Behavior: --check-config with only the built-in defaults returns exit code 0 without starting the server.
        assertEquals(0, parse("--check-config").call());
    }

    @Test
    void shouldFailCheckConfigOnInvalidPort() throws IOException {
        // Behavior: --check-config with a config file that sets network.external.port to the string "abc" returns
        // exit code 1.
        String file = writeConfigFile("network.external.port = \"abc\"\n");

        assertEquals(1, parse("-c", file, "--check-config").call());
    }

    @Test
    void shouldFailCheckConfigOnInvalidNettyTransport() {
        // Behavior: --check-config with --netty-transport set to "bogus", which is neither nio nor epoll, returns
        // exit code 1.
        assertEquals(1, parse("--netty-transport", "bogus", "--check-config").call());
    }

    @Test
    void shouldReturnErrorCodeWhenInstanceCannotBeCreated() throws IOException {
        // Behavior: a config file sets foundationdb.apiversion to 0, so the instance cannot be created. Starting the
        // server with this file returns exit code 1 and does not throw.
        String file = writeConfigFile("foundationdb.apiversion = 0\n");

        assertEquals(1, parse("-c", file).call());
    }

    @Test
    void shouldRenderOnlyKronotopSettings() {
        // Behavior: with --data-dir set, the rendered config parses back with data_dir equal to the option value and
        // cluster.name present. It has no java or os paths from the JVM system properties and no kronotop path from
        // the build properties.
        KronotopInstanceStarter starter = parse("--data-dir", "/tmp/kronotop-render");

        Config rendered = ConfigFactory.parseString(starter.renderConfig(starter.buildConfig()));

        assertEquals("/tmp/kronotop-render", rendered.getString("data_dir"));
        assertTrue(rendered.hasPath("cluster.name"));
        assertFalse(rendered.hasPath("java"));
        assertFalse(rendered.hasPath("os"));
        assertFalse(rendered.hasPath("kronotop"));
    }

    @Test
    void shouldFormatBytesAcrossUnits() {
        // Behavior: formatBytes renders each unit boundary as a short human-readable string.
        assertEquals("512 B", KronotopInstanceStarter.formatBytes(512));
        assertEquals("1023 B", KronotopInstanceStarter.formatBytes(1023));
        assertEquals("1 KB", KronotopInstanceStarter.formatBytes(1024));
        assertEquals("1 MB", KronotopInstanceStarter.formatBytes(1024L * 1024));
        assertEquals("512 MB", KronotopInstanceStarter.formatBytes(512L * 1024 * 1024));
        assertEquals("4.0 GB", KronotopInstanceStarter.formatBytes(4L * 1024 * 1024 * 1024));
        assertEquals("1.5 GB", KronotopInstanceStarter.formatBytes(3L * 512 * 1024 * 1024));
    }

    @Test
    void shouldResolveMissingOrUnresolvedProperty() {
        // Behavior: resolveProperty falls back to "unknown" for missing or unresolved build placeholders.
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty(null));
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty(""));
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty("   "));
        assertEquals("unknown", KronotopInstanceStarter.resolveProperty("${git.commit.id.abbrev}"));
        assertEquals("a1b2c3d", KronotopInstanceStarter.resolveProperty("a1b2c3d"));
    }
}
