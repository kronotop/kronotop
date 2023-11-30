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
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.MissingConfigException;
import com.kronotop.common.utils.ByteUtils;
import com.kronotop.common.utils.DirectoryLayout;
import com.typesafe.config.Config;

import java.util.List;
import java.util.concurrent.CompletionException;

public class ProcessIDGeneratorImpl implements ProcessIDGenerator {
    private final Config config;
    private final Database database;

    public ProcessIDGeneratorImpl(Config config, Database database) {
        this.config = config;
        this.database = database;
    }

    @Override
    public long getProcessID() {
        try (Transaction tr = database.createTransaction()) {
            if (!config.hasPath("cluster.name")) {
                throw new MissingConfigException("cluster.name is missing in configuration");
            }
            List<String> subpath = DirectoryLayout.Builder.clusterName(config.getString("cluster.name")).asList();
            DirectorySubspace root = DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            Subspace processIDSubspace = root.subspace(Tuple.from("processID"));

            tr.mutate(MutationType.ADD, processIDSubspace.pack(), ByteUtils.fromLong(1L));
            byte[] rawProcessID = tr.get(processIDSubspace.pack()).join();
            long processID = ByteUtils.toLong(rawProcessID);
            tr.commit().join();

            return processID;
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException) {
                if (((FDBException) e.getCause()).isRetryableNotCommitted()) {
                    return getProcessID();
                }
            }
            throw e;
        }
    }
}
