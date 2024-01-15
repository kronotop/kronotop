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
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.FoundationDBBackend;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.SchemaCreatedEvent;
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

    private RedisMessage createSchemaHierarchy(Transaction tr, List<String> names, boolean ifNotExists) {
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

        // Trigger the other nodes
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.SCHEMA_CREATED, new SchemaCreatedEvent(names));
        service.getContext().getJournal().getPublisher().publish(tr, JournalName.sqlMetadataEvents(), broadcastEvent);

        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) {
        SqlCreateSchema createSchema = (SqlCreateSchema) node;
        List<String> names = new ArrayList<>(createSchema.name.names);
        return service.getContext().getFoundationDB().run(tr -> createSchemaHierarchy(tr, names, createSchema.ifNotExists));
    }
}
