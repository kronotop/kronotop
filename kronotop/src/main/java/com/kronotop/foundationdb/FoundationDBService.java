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

package com.kronotop.foundationdb;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.CommandHandlerService;
import com.kronotop.core.Context;
import com.kronotop.core.KronotopService;
import com.kronotop.server.Handlers;

import java.util.List;

/**
 * The FoundationDBService class is an implementation of the KronotopService interface that represents a service for
 * interacting with the FoundationDB database.
 */
public class FoundationDBService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "FoundationDB";

    public FoundationDBService(Context context, Handlers handlers) {
        super(context, handlers);

        // Register handlers here
        registerHandler(new BeginHandler(this));
        registerHandler(new RollbackHandler(this));
        registerHandler(new CommitHandler(this));
        registerHandler(new NamespaceHandler(this));
        registerHandler(new SnapshotReadHandler(this));
        registerHandler(new GetReadVersionHandler(this));
        registerHandler(new GetApproximateSizeHandler(this));

        Database database = context.getFoundationDB();
        DirectoryLayer directoryLayer = new DirectoryLayer();
        List<String> root = DirectoryLayout.Builder.clusterName(context.getClusterName()).namespaces().asList();
        DirectorySubspace rootSubspace = database.run(tr -> directoryLayer.createOrOpen(tr, root).join());
        if (!root.equals(rootSubspace.getPath())) {
            throw new KronotopException(
                    String.format("Unexpected root subspace: %s", String.join(".", rootSubspace.getPath()))
            );
        }

    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return this.context;
    }

    @Override
    public void shutdown() {
    }
}
