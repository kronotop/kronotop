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

package com.kronotop.foundationdb;

import com.kronotop.CommandHandlerService;
import com.kronotop.Context;
import com.kronotop.KronotopService;
import com.kronotop.foundationdb.namespace.NamespaceHandler;
import com.kronotop.foundationdb.zmap.*;
import com.kronotop.server.ServerKind;

/**
 * The FoundationDBService class is an implementation of the KronotopService interface that represents a service for
 * interacting with the FoundationDB database.
 */
public class FoundationDBService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "FoundationDB";

    public FoundationDBService(Context context) {
        super(context, NAME);

        // Register handlers here
        handlerMethod(ServerKind.EXTERNAL, new BeginHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new RollbackHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new CommitHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new NamespaceHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new SnapshotReadHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new GetReadVersionHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new GetApproximateSizeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZSetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZDelHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZDelRangeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZDelPrefixHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetRangeHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetKeyHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZMutateHandler(this));
        handlerMethod(ServerKind.EXTERNAL, new ZGetRangeSizeHandler(this));
    }
}
