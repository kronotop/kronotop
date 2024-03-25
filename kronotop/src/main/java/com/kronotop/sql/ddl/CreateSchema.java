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

package com.kronotop.sql.ddl;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.core.TransactionUtils;
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.StatementExecutor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.FoundationDBBackend;
import com.kronotop.sql.metadata.events.BroadcastEvent;
import com.kronotop.sql.metadata.events.EventTypes;
import com.kronotop.sql.metadata.events.SchemaCreatedEvent;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateSchema;

import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * The CreateSchema class is responsible for creating a schema directory in FoundationDB.
 * It extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 */
public class CreateSchema extends FoundationDBBackend implements StatementExecutor<SqlNode> {

    public CreateSchema(SqlService service) {
        super(service);
    }

    private RedisMessage createSchemaHierarchy(ExecutionContext context, Transaction tr, SqlCreateSchema createSchema) {
        if (createSchema.name.names.isEmpty()) {
            return new ErrorRedisMessage(RESPError.SQL, "Schema is not defined");
        }
        if (createSchema.name.names.size() > 1) {
            return new ErrorRedisMessage(RESPError.SQL, "Sub-schemas are not allowed");
        }

        String schema = createSchema.name.names.get(0);
        List<String> subpath = service.getMetadataService().getSchemaLayout(schema).asList();
        try {
            DirectoryLayer.getDefault().create(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                if (!createSchema.ifNotExists) {
                    return new ErrorRedisMessage(RESPError.SQL, String.format("Schema '%s' already exists", schema));
                }
            }
            throw new KronotopException(e);
        }

        TransactionUtils.addPostCommitHook(() -> {
            // Trigger the other nodes
            BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.SCHEMA_CREATED, new SchemaCreatedEvent(schema));
            service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
        }, context.getRequest().getChannelContext());

        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) {
        SqlCreateSchema createSchema = (SqlCreateSchema) node;
        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), context.getRequest().getChannelContext());
        RedisMessage result = createSchemaHierarchy(context, tr, createSchema);
        TransactionUtils.commitIfAutoCommitEnabled(tr, context.getRequest().getChannelContext());
        return result;
    }
}
