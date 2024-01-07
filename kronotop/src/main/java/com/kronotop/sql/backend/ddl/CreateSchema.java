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

package com.kronotop.sql.backend.ddl;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.Executor;
import com.kronotop.sql.backend.FoundationDBBackend;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * The CreateSchema class is responsible for creating a schema directory in FoundationDB.
 * It extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 */
public class CreateSchema extends FoundationDBBackend implements Executor<SqlNode> {

    public CreateSchema(SqlService service) {
        super(service);
    }

    private RedisMessage createDirectory(Transaction tr, List<String> names, boolean ifNotExists) {
        List<String> subpath = DirectoryLayout.
                Builder.
                clusterName(service.getContext().getClusterName()).
                internal().
                sql().
                metadata().
                schemas().
                addAll(names).
                asList();
        try {
            DirectoryLayer.getDefault().create(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                if (!ifNotExists) {
                    return new ErrorRedisMessage(String.format("Schema '%s' already exists", String.join(".", names)));
                }
            }
            throw new KronotopException(e);
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(SqlNode node) {
        SqlCreateSchema createSchema = (SqlCreateSchema) node;
        List<String> names = new ArrayList<>(createSchema.name.names);
        return service.getContext().getFoundationDB().run(tr -> createDirectory(tr, names, createSchema.ifNotExists));
    }
}
