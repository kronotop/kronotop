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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
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
import com.kronotop.sql.metadata.SchemaNotExistsException;
import com.kronotop.sql.metadata.events.BroadcastEvent;
import com.kronotop.sql.metadata.events.EventTypes;
import com.kronotop.sql.metadata.events.SchemaDroppedEvent;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * DropSchema is a class that represents the execution of a DROP SCHEMA statement in an SQL service.
 * DropSchema class extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 */
public class DropSchema extends FoundationDBBackend implements StatementExecutor<SqlNode> {

    public DropSchema(SqlService service) {
        super(service);
    }

    private RedisMessage dropSchemaHierarchy(ExecutionContext context, Transaction tr, SqlDropSchema dropSchema) {
        if (dropSchema.name.names.isEmpty()) {
            return new ErrorRedisMessage(RESPError.SQL, "schema is not defined");
        }
        if (dropSchema.name.names.size() > 1) {
            return new ErrorRedisMessage(RESPError.SQL, "sub-schemas are not allowed");
        }

        String schema = dropSchema.name.names.get(0);
        List<String> subpath = service.getMetadataService().getSchemaLayout(schema).asList();
        try {
            DirectoryLayer.getDefault().remove(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                if (dropSchema.ifExists) {
                    return new SimpleStringRedisMessage(Response.OK);
                } else {
                    Throwable throwable = new SchemaNotExistsException(schema);
                    return new ErrorRedisMessage(RESPError.SQL, throwable.getMessage());
                }
            }
            throw new KronotopException(e);
        }

        TransactionUtils.addPostCommitHook(() -> {
            BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.SCHEMA_DROPPED, new SchemaDroppedEvent(schema));
            service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
        }, context.getRequest().getChannelContext());

        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlDropSchema dropSchema = (SqlDropSchema) node;

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), context.getRequest().getChannelContext());
        RedisMessage result = dropSchemaHierarchy(context, tr, dropSchema);

        TransactionUtils.commitIfAutoCommitEnabled(tr, context.getRequest().getChannelContext());
        return result;
    }
}
