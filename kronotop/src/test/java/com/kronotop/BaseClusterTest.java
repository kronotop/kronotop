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

package com.kronotop;

import com.kronotop.cluster.Member;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.BindException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * BaseClusterTest is a base class for testing cluster functionality.
 * <p>
 * It provides common functionality for setting up and tearing down a Kronotop cluster
 * with multiple instances. The class uses a ConcurrentHashMap to store the KronotopTestInstance
 * objects associated with their respective Members.
 */
public class BaseClusterTest extends BaseTest {
    protected Map<Member, KronotopTestInstance> kronotopInstances = new LinkedHashMap<>();

    private static boolean isAddressInUse(Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof BindException) {
                return true;
            }
        }
        return false;
    }

    private static void safeShutdown(KronotopTestInstance instance) {
        try {
            instance.shutdown();
        } catch (Exception ignored) {
            // Best-effort cleanup of the partially started instance before retrying.
        }
    }

    @BeforeEach
    public void setup() {
        addNewInstance();
    }

    protected List<KronotopTestInstance> getInstances() {
        List<KronotopTestInstance> instances = new ArrayList<>(kronotopInstances.values());
        return Collections.unmodifiableList(instances);
    }

    protected KronotopTestInstance addNewInstance() {
        return addNewInstance(false);
    }

    /**
     * Adds a new KronotopTestInstance to the cluster.
     *
     * @return the added KronotopTestInstance
     * @throws RuntimeException if an UnknownHostException or InterruptedException occurs
     */
    protected KronotopTestInstance addNewInstance(boolean runWithTCPServer) {
        int maxAttempts = 5;
        for (int attempt = 1; ; attempt++) {
            Config config = loadConfig("test.conf");
            KronotopTestInstance kronotopInstance = new KronotopTestInstance(config, runWithTCPServer);
            try {
                kronotopInstance.start();
            } catch (InterruptedException exp) {
                Thread.currentThread().interrupt();
                throw new KronotopException(exp);
            } catch (Exception exp) {
                // The kernel-assigned ephemeral port can occasionally be taken by another process
                // between assignment and bind (a TOCTOU race that surfaces under parallel test forks)
                // as "Address already in use". Release the partially started instance and retry; a
                // fresh start gets a newly assigned port.
                if (isAddressInUse(exp) && attempt < maxAttempts) {
                    safeShutdown(kronotopInstance);
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    continue;
                }
                throw new KronotopException(exp);
            }
            kronotopInstances.put(kronotopInstance.getMember(), kronotopInstance);
            return kronotopInstance;
        }
    }

    /**
     * Cleans up resources after each test execution in the test class.
     * <p>
     * This method ensures that all Kronotop test instances are properly shut down
     * and their associated clusters are cleaned up to maintain test isolation and avoid resource leakage.
     * The cleanup process is performed in two steps:
     * 1. Each Kronotop test instance is shut down without cleaning up its associated state.
     * 2. Each Kronotop test instance performs cluster cleanup tasks to remove any residual data or configurations.
     * <p>
     * This method is annotated with {@code @AfterEach}, indicating it will run after each test.
     */
    @AfterEach
    public void tearDown() {
        for (KronotopTestInstance kronotopInstance : kronotopInstances.values()) {
            kronotopInstance.shutdownWithoutCleanup();
        }
        for (KronotopTestInstance kronotopInstance : kronotopInstances.values()) {
            kronotopInstance.cleanupTestCluster();
        }
    }
}
