/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.internal;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.MissingConfigException;
import com.kronotop.directory.KronotopDirectory;
import com.typesafe.config.Config;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ProcessIdGeneratorImpl implements ProcessIdGenerator {
    private final Config config;
    private final Database database;

    public ProcessIdGeneratorImpl(Config config, Database database) {
        this.config = config;
        this.database = database;
    }

    /**
     * Retrieves the process ID.
     *
     * @return The process ID as a Versionstamp object.
     */
    public Versionstamp getProcessID() {
        CompletableFuture<byte[]> transactionVersionFuture = database.run(transaction -> {
            DirectorySubspace root = getDirectoryRoot(transaction);
            Tuple tuple = Tuple.from("processID-prefix", Versionstamp.incomplete());
            transaction.mutate(MutationType.SET_VERSIONSTAMPED_KEY, root.packWithVersionstamp(tuple), new byte[0]);
            return transaction.getVersionstamp();
        });

        byte[] transactionVersion = transactionVersionFuture.join();
        return Versionstamp.complete(transactionVersion);
    }

    private DirectorySubspace getDirectoryRoot(Transaction transaction) {
        if (!config.hasPath("cluster.name")) {
            throw new MissingConfigException("cluster.name is missing in configuration");
        }

        List<String> subpath = KronotopDirectory.kronotop().cluster(config.getString("cluster.name")).toList();
        return DirectoryLayer.getDefault().createOrOpen(transaction, subpath).join();
    }
}
