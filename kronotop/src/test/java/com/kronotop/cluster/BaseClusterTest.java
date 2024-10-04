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

package com.kronotop.cluster;

import com.kronotop.BaseTest;
import com.kronotop.KronotopTestInstance;
import com.typesafe.config.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BaseClusterTest is a base class for testing cluster functionality.
 * <p>
 * It provides common functionality for setting up and tearing down a Kronotop cluster
 * with multiple instances. The class uses a ConcurrentHashMap to store the KronotopTestInstance
 * objects associated with their respective Members.
 */
public class BaseClusterTest extends BaseTest {
    protected ConcurrentHashMap<Member, KronotopTestInstance> kronotopInstances = new ConcurrentHashMap<>();

    @BeforeEach
    public void setup() {
        addNewInstance();
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
        Config config = loadConfig("test.conf");
        KronotopTestInstance kronotopInstance = new KronotopTestInstance(config, runWithTCPServer);

        try {
            kronotopInstance.start();
        } catch (UnknownHostException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        kronotopInstances.put(kronotopInstance.getMember(), kronotopInstance);
        return kronotopInstance;
    }

    /**
     * Retrieves the cluster coordinator instance from the Kronotop test instances.
     *
     * <p>
     * This method iterates through the Kronotop test instances and checks if the instance's member
     * is equal to the known coordinator member obtained from the membership service. If a matching coordinator
     * instance is found, it is returned.
     * </p>
     * <p>
     * If no coordinator instance is found, an {@code IllegalStateException} is thrown.
     * </p>
     *
     * @return the cluster coordinator instance
     * @throws IllegalStateException if no coordinator instance is found
     */
    protected KronotopTestInstance getClusterCoordinator() {
        KronotopTestInstance instance = kronotopInstances.values().iterator().next();
        MembershipService membershipService = instance.getContext().getService(MembershipService.NAME);
        Member coordinator = membershipService.getKnownCoordinator();
        for (KronotopTestInstance coordinatorInstance : kronotopInstances.values()) {
            if (coordinatorInstance.getContext().getMember().equals(coordinator)) {
                return coordinatorInstance;
            }
        }
        throw new IllegalStateException("No coordinator found");
    }

    /**
     * Tears down the Kronotop cluster by shutting down all instances.
     * <p>
     * This method iterates through all the instances in the {@code kronotopInstances} map and calls
     * the {@code shutdown()} method on each instance. It is called automatically after each test method
     * is executed in a JUnit 5 test class annotated with {@code @Test}.
     */
    @AfterEach
    public void tearDown() {
        for (KronotopTestInstance kronotopInstance : kronotopInstances.values()) {
            kronotopInstance.shutdown();
        }
    }
}
