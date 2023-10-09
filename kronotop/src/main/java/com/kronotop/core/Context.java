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
import com.google.common.util.concurrent.Striped;
import com.kronotop.core.cluster.Member;
import com.kronotop.redis.storage.LogicalDatabase;
import com.typesafe.config.Config;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

public interface Context {
    String getClusterName();

    Config getConfig();

    Member getMember();

    Database getFoundationDB();

    void registerService(String id, KronotopService service);

    <T> T getService(@Nonnull String id);

    List<KronotopService> getServices();

    LogicalDatabase getLogicalDatabase();

    Striped<ReadWriteLock> getStripedReadWriteLock();
}