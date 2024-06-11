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

import com.kronotop.ConfigTestUtil;
import com.kronotop.KronotopTestInstance;
import com.kronotop.redis.storage.Shard;
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
public class BaseClusterTest {
    private final Config config = ConfigTestUtil.load("test.conf");
    protected ConcurrentHashMap<Member, KronotopTestInstance> kronotopInstances = new ConcurrentHashMap<>();

    @BeforeEach
    public void setup() {
        addNewInstance();
    }

    /**
     * Checks if all shards in the Kronotop test instances are operable.
     *
     * @return true if all shards are operable, false otherwise
     */
    protected boolean areAllShardsOperable() {
        for (KronotopTestInstance kronotopTestInstance : kronotopInstances.values()) {
            int numberOfShards = kronotopTestInstance.getContext().getConfig().getInt("cluster.number_of_shards");
            for (int shardId = 0; shardId < numberOfShards; shardId++) {
                Shard shard = kronotopTestInstance.getContext().getLogicalDatabase().getShards().get(shardId);
                if (shard != null && shard.isReadOnly() && !shard.isOperable()) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Adds a new KronotopTestInstance to the cluster.
     *
     * @return the added KronotopTestInstance
     * @throws RuntimeException if an UnknownHostException or InterruptedException occurs
     */
    protected KronotopTestInstance addNewInstance() {
        KronotopTestInstance kronotopInstance = new KronotopTestInstance(config);

        try {
            kronotopInstance.start();
        } catch (UnknownHostException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        kronotopInstances.put(kronotopInstance.getMember(), kronotopInstance);
        return kronotopInstance;
    }

    @AfterEach
    public void tearDown() {
        for (KronotopTestInstance kronotopInstance : kronotopInstances.values()) {
            kronotopInstance.shutdown();
        }
    }
}
