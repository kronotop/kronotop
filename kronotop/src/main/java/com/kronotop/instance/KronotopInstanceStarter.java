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


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.apple.foundationdb.FDBException;
import com.kronotop.ConfigException;
import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberAlreadyRunningException;
import com.kronotop.network.Address;
import com.kronotop.network.NetworkConfigUtil;
import com.typesafe.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.base.Throwables.getRootCause;

/*
What Des-Cartes [sic] did was a good step. You have added much several ways, & especially in taking the colours of thin
plates into philosophical consideration. If I have seen further it is by standing on the shoulders of Giants.

-- Isaac Newton, Letter to Robert Hooke, 5 February 1675.
 */

/**
 * The KronotopInstanceStarter class is responsible for starting and shutting down a KronotopInstance.
 */
@Command(
        name = "kronotop-server",
        description = "Starts a Kronotop server.",
        versionProvider = KronotopInstanceStarter.VersionProvider.class,
        sortOptions = false
)
public class KronotopInstanceStarter implements Callable<Integer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopInstanceStarter.class);
    private static final List<String> NETWORK_INTERFACES = List.of("external", "internal");
    // FoundationDB error code of no_cluster_file_found.
    private static final int NO_CLUSTER_FILE_FOUND = 1515;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show help.")
    private boolean helpRequested;

    @Option(names = {"-v", "--version"}, versionHelp = true, description = "Print version information and quit.")
    private boolean versionRequested;

    @Option(names = {"-c", "--config"}, paramLabel = "<file>", description = "Path to the config file.")
    private File configFile;

    @Option(names = "--data-dir", paramLabel = "<path>", description = "Data directory.")
    private String dataDir;

    @Option(names = "--cluster-name", paramLabel = "<name>", description = "Cluster name.")
    private String clusterName;

    @Option(names = "--fdb-cluster-file", paramLabel = "<path>", description = "FoundationDB cluster file.")
    private String fdbClusterFile;

    @Option(names = "--fdbc", paramLabel = "<path>", description = "Path to the libfdb_c library.")
    private String fdbc;

    @Option(names = "--external-host", paramLabel = "<host>", description = "Bind host of the client listener.")
    private String externalHost;

    @Option(names = "--external-port", paramLabel = "<port>", description = "Bind port of the client listener.")
    private Integer externalPort;

    @Option(names = "--internal-host", paramLabel = "<host>", description = "Bind host of the internal listener.")
    private String internalHost;

    @Option(names = "--internal-port", paramLabel = "<port>", description = "Bind port of the internal listener.")
    private Integer internalPort;

    @Option(names = "--external-advertise", paramLabel = "<address>", split = ",",
            description = "Comma separated list of advertised client addresses.")
    private List<String> externalAdvertise;

    @Option(names = "--internal-advertise", paramLabel = "<address>", split = ",",
            description = "Comma separated list of advertised internal addresses.")
    private List<String> internalAdvertise;

    @Option(names = "--netty-transport", paramLabel = "<nio|epoll>", description = "Transport for both listeners.")
    private String nettyTransport;

    @Option(names = "--log-level", paramLabel = "<level>", description = "Log level.")
    private String logLevel;

    @Option(names = "--check-config", description = "Load and validate the config, then exit.")
    private boolean checkConfig;

    @Option(names = "--print-config", description = "Print the resolved config, then exit.")
    private boolean printConfig;

    private static Properties loadApplicationProperties() {
        Properties props = new Properties();
        try (InputStream in = KronotopInstance.class.getClassLoader().getResourceAsStream("kronotop-build.properties")) {
            props.load(in);
        } catch (IOException exp) {
            LOGGER.error("Failed to load application properties", exp);
            throw new RuntimeException(exp);
        }
        return props;
    }

    private static void greeting(KronotopInstance instance) {
        Member member = instance.getMember();
        Context context = instance.getContext();
        Config config = context.getConfig();
        Properties props = loadApplicationProperties();

        String fdbClusterFile = config.hasPath("foundationdb.clusterfile")
                ? config.getString("foundationdb.clusterfile")
                : "default cluster file";

        LOGGER.info("{}, a distributed transactional document database", KronotopInstance.PRODUCT_NAME);
        LOGGER.info("https://kronotop.com");
        LOGGER.info("Starting {} {} ({}, built {}) pid {}",
                KronotopInstance.PRODUCT_NAME,
                props.getProperty("kronotop.version"),
                resolveProperty(props.getProperty("kronotop.git.commit")),
                resolveProperty(props.getProperty("kronotop.build.time")),
                ProcessHandle.current().pid());
        LOGGER.info("Runtime:  Java {} ({}), {}/{}, {} cores, {} heap",
                System.getProperty("java.version"),
                System.getProperty("java.vendor"),
                System.getProperty("os.name"),
                System.getProperty("os.arch"),
                Runtime.getRuntime().availableProcessors(),
                formatBytes(Runtime.getRuntime().maxMemory()));
        LOGGER.info("Cluster:  {}", context.getClusterName());
        LOGGER.info("Member:   {} [{}]", member.getId(), instance.getStatus());
        LOGGER.info("FDB:      {} (API {})", fdbClusterFile, config.getInt("foundationdb.apiversion"));
        LOGGER.info("Client:   bound to {}, advertised as {}",
                member.getExternalAddress(), joinAddresses(member.getExternalAdvertise()));
        LOGGER.info("Internal: bound to {}, advertised as {}",
                member.getInternalAddress(), joinAddresses(member.getInternalAdvertise()));
        LOGGER.info("Ready to accept connections in {} ms",
                ManagementFactory.getRuntimeMXBean().getUptime());
    }

    /**
     * Renders a list of addresses as a comma separated string.
     */
    static String joinAddresses(List<Address> addresses) {
        return addresses.stream().map(Address::toString).collect(Collectors.joining(", "));
    }

    /**
     * Returns the value, or "unknown" when it is missing or an unresolved build placeholder.
     */
    public static String resolveProperty(String value) {
        if (value == null || value.isBlank() || value.startsWith("${")) {
            return "unknown";
        }
        return value;
    }

    /**
     * Renders a byte count as a short human-readable string (B, KB, MB, GB).
     */
    public static String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        }
        double kb = bytes / 1024.0;
        if (kb < 1024) {
            return String.format(Locale.ROOT, "%.0f KB", kb);
        }
        double mb = kb / 1024.0;
        if (mb < 1024) {
            return String.format(Locale.ROOT, "%.0f MB", mb);
        }
        return String.format(Locale.ROOT, "%.1f GB", mb / 1024.0);
    }

    static void main(String[] args) {
        int exitCode = new CommandLine(new KronotopInstanceStarter()).execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private static Thread createShutdownHook(KronotopInstance instance) {
        return new Thread(() -> {
            try {
                instance.shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                instance.closeFoundationDBConnection();
                LOGGER.info("Quit!");
            }
        });
    }

    static boolean isExpectedFailure(Throwable failure) {
        for (Throwable cause : getCausalChain(failure)) {
            if (cause instanceof ConfigException
                    || cause instanceof com.typesafe.config.ConfigException
                    || cause instanceof MemberAlreadyRunningException
                    || cause instanceof BindException
                    || cause instanceof UnsatisfiedLinkError) {
                return true;
            }
            if (cause instanceof FDBException fdbException && fdbException.getCode() == NO_CLUSTER_FILE_FOUND) {
                return true;
            }
        }
        return false;
    }

    private static void logFailure(String message, Throwable e) {
        if (LOGGER.isDebugEnabled() || !isExpectedFailure(e)) {
            LOGGER.error(message, e);
        } else {
            LOGGER.error("{}: {}", message, getRootCause(e).getMessage());
        }
    }

    private static void applyLogLevel(String name) {
        Level level = Level.toLevel(name, null);
        if (level == null) {
            throw new ConfigException("Unknown log level: '" + name + "'");
        }
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).setLevel(level);
    }

    private static void checkNetwork(Config config) {
        for (String networkInterface : NETWORK_INTERFACES) {
            NetworkConfigUtil.getHost(config, networkInterface);
            NetworkConfigUtil.getPort(config, networkInterface);
            NetworkConfigUtil.getAdvertise(config, networkInterface);
            NetworkConfigUtil.getNettyTransport(config, networkInterface);
        }
    }

    private static List<String> withoutBlanks(List<String> values) {
        return values == null ? null : values.stream().filter(value -> !value.isBlank()).toList();
    }

    private static void putIfSet(Map<String, Object> overrides, String path, Object value) {
        if (value != null) {
            overrides.put(path, value);
        }
    }

    private static int start(Config config) {
        try {
            KronotopInstance kronotopInstance = new KronotopInstanceWithRESP(config);
            Runtime.getRuntime().addShutdownHook(createShutdownHook(kronotopInstance));
            kronotopInstance.start();
            greeting(kronotopInstance);
            return 0;
        } catch (Throwable e) {
            logFailure("Failed to start Kronotop instance", e);
            return 1;
        }
    }

    Config loadApplicationConfig() {
        if (configFile == null) {
            return ConfigFactory.defaultApplication();
        }
        return ConfigFactory.parseFile(configFile, ConfigParseOptions.defaults().setAllowMissing(false));
    }

    Config buildConfig() {
        Map<String, Object> overrides = new HashMap<>();
        putIfSet(overrides, "data_dir", dataDir);
        putIfSet(overrides, "cluster.name", clusterName);
        putIfSet(overrides, "foundationdb.clusterfile", fdbClusterFile);
        putIfSet(overrides, "foundationdb.fdbc", fdbc);
        putIfSet(overrides, "network.external.host", externalHost);
        putIfSet(overrides, "network.external.port", externalPort);
        putIfSet(overrides, "network.external.advertise", withoutBlanks(externalAdvertise));
        putIfSet(overrides, "network.internal.host", internalHost);
        putIfSet(overrides, "network.internal.port", internalPort);
        putIfSet(overrides, "network.internal.advertise", withoutBlanks(internalAdvertise));
        for (String networkInterface : NETWORK_INTERFACES) {
            putIfSet(overrides, "network." + networkInterface + ".netty.transport", nettyTransport);
        }

        // Precedence: command line options, -D system properties, config file, reference.conf
        return ConfigFactory.parseMap(overrides, "command line options")
                .withFallback(ConfigFactory.defaultOverrides())
                .withFallback(loadApplicationConfig())
                .withFallback(ConfigFactory.defaultReference())
                .resolve();
    }

    String renderConfig(Config config) {
        // System properties such as java.* and os.* are not Kronotop settings, drop them from the output.
        ConfigObject application = loadApplicationConfig().root();
        ConfigObject reference = ConfigFactory.parseResources("reference.conf").root();
        ConfigObject root = config.root();
        for (String key : config.root().keySet()) {
            if (!application.containsKey(key) && !reference.containsKey(key)) {
                root = root.withoutKey(key);
            }
        }
        return root.render(ConfigRenderOptions.concise().setFormatted(true).setJson(false));
    }

    @Override
    public Integer call() {
        Config config;
        try {
            if (logLevel != null) {
                applyLogLevel(logLevel);
            }
            config = buildConfig();
            if (checkConfig) {
                checkNetwork(config);
            }
        } catch (Exception e) {
            logFailure("Invalid configuration", e);
            return 1;
        }

        if (checkConfig) {
            LOGGER.info("Configuration is valid");
            return 0;
        }
        if (printConfig) {
            System.out.println(renderConfig(config));
            return 0;
        }
        return start(config);
    }

    /**
     * Provides the version string printed by {@code --version}.
     */
    public static class VersionProvider implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() {
            return new String[]{"kronotop-server " + loadApplicationProperties().getProperty("kronotop.version")};
        }
    }
}
