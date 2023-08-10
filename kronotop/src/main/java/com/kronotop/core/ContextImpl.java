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

package com.kronotop.core;

import com.apple.foundationdb.Database;
import com.kronotop.MissingConfigException;
import com.kronotop.common.KronotopException;
import com.kronotop.core.cluster.Member;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ContextImpl implements Context {
    private final Config config;
    private final Member member;
    private final Database database;
    private final ConcurrentMap<String, KronotopService> services = new ConcurrentHashMap<>();
    private final String clusterName;

    public ContextImpl(Config config, Member member, Database database) {
        this.config = config;
        this.member = member;
        this.database = database;

        if (config.hasPath("cluster.name")) {
            clusterName = config.getString("cluster.name");
        } else {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }
    }

    @Override
    public String getClusterName() {
        return clusterName;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public Member getMember() {
        return member;
    }

    @Override
    public Database getFoundationDB() {
        return database;
    }

    @Override
    public void registerService(@Nonnull String id, @Nonnull KronotopService service) {
        synchronized (services) {
            if (services.containsKey(id)) {
                throw new KronotopException(String.format("Service '%s' already registered", id));
            }
            services.put(id, service);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getService(@Nonnull String id) {
        return (T) services.get(id);
    }

    public List<KronotopService> getServices() {
        return new ArrayList<>(services.values());
    }
}