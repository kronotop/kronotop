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

package com.kronotop.foundationdb;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.foundationdb.namespace.NamespaceHandler;
import com.kronotop.foundationdb.zmap.*;
import com.kronotop.server.Handlers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * The FoundationDBService class is an implementation of the KronotopService interface that represents a service for
 * interacting with the FoundationDB database.
 */
public class FoundationDBService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "FoundationDB";
    private final String defaultNamespaceName;

    public FoundationDBService(Context context, Handlers handlers) {
        super(context, handlers);
        defaultNamespaceName = context.getConfig().getString("default_namespace");

        // Register handlers here
        registerHandler(new BeginHandler(this));
        registerHandler(new RollbackHandler(this));
        registerHandler(new CommitHandler(this));
        registerHandler(new NamespaceHandler(this));
        registerHandler(new SnapshotReadHandler(this));
        registerHandler(new GetReadVersionHandler(this));
        registerHandler(new GetApproximateSizeHandler(this));
        registerHandler(new ZSetHandler(this));
        registerHandler(new ZGetHandler(this));
        registerHandler(new ZDelHandler(this));
        registerHandler(new ZDelRangeHandler(this));
        registerHandler(new ZDelPrefixHandler(this));
        registerHandler(new ZGetRangeHandler(this));
        registerHandler(new ZGetKeyHandler(this));
        registerHandler(new ZMutateHandler(this));
        registerHandler(new ZGetRangeSizeHandler(this));

        initializeDefaultNamespace();
    }

    private void initializeDefaultNamespace() {
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            List<String> namespacePath = new ArrayList<>();
            Collections.addAll(namespacePath, defaultNamespaceName.split("\\."));
            List<String> subpath = DirectoryLayout.Builder.clusterName(context.getClusterName()).namespaces().addAll(namespacePath).asList();
            DirectoryLayer.getDefault().createOrOpen(tr, subpath).join();
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof FDBException) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (((FDBException) e.getCause()).getCode() == 1020) {
                    // retry
                    initializeDefaultNamespace();
                }
            }
            throw new KronotopException(e);
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
