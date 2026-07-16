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


import com.kronotop.Context;
import com.kronotop.cluster.Member;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;

import static com.google.common.base.Throwables.getRootCause;

/*
What Des-Cartes [sic] did was a good step. You have added much several ways, & especially in taking the colours of thin
plates into philosophical consideration. If I have seen further it is by standing on the shoulders of Giants.

-- Isaac Newton, Letter to Robert Hooke, 5 February 1675.
 */

/**
 * The KronotopInstanceStarter class is responsible for starting and shutting down a KronotopInstance.
 */
public class KronotopInstanceStarter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopInstanceStarter.class);

    private static void greeting(KronotopInstance instance) {
        Member member = instance.getMember();
        Context context = instance.getContext();
        Config config = context.getConfig();

        Properties props = new Properties();
        try (InputStream in = KronotopInstance.class.getClassLoader().getResourceAsStream("application.properties")) {
            props.load(in);
        } catch (IOException exp) {
            LOGGER.error("Failed to load application properties", exp);
            throw new RuntimeException(exp);
        }

        String fdbClusterFile = config.hasPath("foundationdb.clusterfile")
                ? config.getString("foundationdb.clusterfile")
                : "default cluster file";

        LOGGER.info("{}, distributed transactional document database", KronotopInstance.PRODUCT_NAME);
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
        LOGGER.info("Client:   {}", member.getExternalAddress());
        LOGGER.info("Internal: {}", member.getInternalAddress());
        LOGGER.info("Ready to accept connections");
    }

    /**
     * Returns the value, or "unknown" when it is missing or an unresolved build placeholder.
     */
    static String resolveProperty(String value) {
        if (value == null || value.isBlank() || value.startsWith("${")) {
            return "unknown";
        }
        return value;
    }

    /**
     * Renders a byte count as a short human-readable string (B, KB, MB, GB).
     */
    static String formatBytes(long bytes) {
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

    public static void main(String[] args) {
        KronotopInstance kronotopInstance = new KronotopInstanceWithRESP();
        Thread shutdownHook = createShutdownHook(kronotopInstance);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            kronotopInstance.start();
            greeting(kronotopInstance);
        } catch (Exception e) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error("Failed to start Kronotop instance", e);
            } else {
                LOGGER.error("Failed to start Kronotop instance {}", getRootCause(e).getMessage());
            }
            System.exit(1);
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
}
