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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.Context;
import com.kronotop.DataStructureKind;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.Handler;
import com.kronotop.server.Session;

public abstract class BaseFoundationDBHandler implements Handler {
    protected FoundationDBService service;
    protected Context context;

    public BaseFoundationDBHandler(FoundationDBService service) {
        this.service = service;
        this.context = service.getContext();
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    /**
     * Opens a ZMap subspace in the FoundationDB database.
     *
     * @param tr      the transaction object for performing operations within the current transaction context
     * @param session the session object representing the user session and its associated metadata
     * @return the DirectorySubspace object representing the ZMap subspace in the database
     */
    protected DirectorySubspace openZMapSubspace(Transaction tr, Session session) {
        return NamespaceUtil.openDataStructureSubspace(context, tr, session, DataStructureKind.ZMAP);
    }
}
