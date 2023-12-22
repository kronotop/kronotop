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

package com.kronotop;

import com.kronotop.core.cluster.Member;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * KronotopTestCluster is a class that represents a cluster of KronotopTestInstances.
 * It provides methods to start and shutdown the cluster.
 */
public class KronotopTestCluster {
    private static final Logger LOGGER = LoggerFactory.getLogger(KronotopTestInstance.class);
    private final Config config;
    private final ConcurrentHashMap<Member, KronotopTestInstance> members = new ConcurrentHashMap<>();

    public KronotopTestCluster(Config config) {
        this.config = config;
    }

    public static void main(String[] args) throws InterruptedException, UnknownHostException {
        Config config = ConfigTestUtil.load("test.conf");
        KronotopTestCluster cluster = new KronotopTestCluster(config);
        cluster.start(2);

        KronotopTestInstance instance = cluster.addInstance();
        System.out.println(instance.getMember());

        cluster.shutdown();
    }

    /**
     * Starts the specified number of instances in the cluster.
     * Each instance is started in a separate thread.
     * The method waits until all instances are started before returning.
     *
     * @param number the number of instances to start
     * @throws InterruptedException if the thread is interrupted while waiting for all instances to start
     */
    public void start(int number) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(number);
        for (int i = 0; i < number; i++) {
            Thread thread = new Thread(() -> {
                try {
                    addInstance();
                    countDownLatch.countDown();
                } catch (UnknownHostException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.start();
        }
        countDownLatch.await();
        LOGGER.info("Kronotop cluster has been started with {} members", number);
    }

    /**
     * Adds a new KronotopTestInstance to the KronotopTestCluster.
     * The instance is started and its member is added to the cluster.
     *
     * @throws UnknownHostException if the host address is unknown
     * @throws InterruptedException if the thread is interrupted
     */
    public KronotopTestInstance addInstance() throws UnknownHostException, InterruptedException {
        KronotopTestInstance instance = new KronotopTestInstance(config);
        instance.start();

        Member member = instance.getMember();
        members.put(member, instance);
        LOGGER.info("Cluster member has been started: {}", member);

        return instance;
    }

    /**
     * Removes a KronotopTestInstance from the KronotopTestCluster.
     * The instance is shut down and its member is removed from the cluster.
     *
     * @param instance the KronotopTestInstance to be removed
     */
    public void removeInstance(KronotopTestInstance instance) {
        try {
            instance.shutdown();
        } finally {
            members.remove(instance.getMember());
        }
    }

    /**
     * Retrieves a list of KronotopTestInstance objects representing the instances in the cluster.
     *
     * @return a List of KronotopTestInstance objects
     */
    public List<KronotopTestInstance> getInstances() {
        return new ArrayList<>(members.values());
    }

    /**
     * Shuts down the cluster by shutting down each instance.
     * This method removes all members from the cluster.
     */
    public void shutdown() {
        for (KronotopTestInstance instance : members.values()) {
            instance.shutdown();
        }
        members.clear();
    }
}
