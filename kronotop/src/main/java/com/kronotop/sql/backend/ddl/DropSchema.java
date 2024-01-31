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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
import com.kronotop.common.KronotopException;
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.FoundationDBBackend;
import com.kronotop.sql.backend.metadata.SchemaNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.SchemaDroppedEvent;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * DropSchema is a class that represents the execution of a DROP SCHEMA statement in a SQL service.
 * DropSchema class extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 */
public class DropSchema extends FoundationDBBackend implements Executor<SqlNode> {

    public DropSchema(SqlService service) {
        super(service);
    }

    private RedisMessage dropSchemaHierarchy(Transaction tr, String schema, boolean ifExists) {
        List<String> subpath = service.getMetadataService().getSchemaLayout(schema).asList();
        try {
            DirectoryLayer.getDefault().remove(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                if (ifExists) {
                    return new SimpleStringRedisMessage(Response.OK);
                } else {
                    Throwable throwable = new SchemaNotExistsException(schema);
                    return new ErrorRedisMessage(throwable.getMessage());
                }
            }
            throw new KronotopException(e);
        }

        // Trigger the other nodes
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.SCHEMA_DROPPED, new SchemaDroppedEvent(schema));
        service.getContext().getJournal().getPublisher().publish(tr, JournalName.sqlMetadataEvents(), broadcastEvent);

        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlDropSchema dropSchema = (SqlDropSchema) node;
        if (dropSchema.name.names.isEmpty()) {
            return new ErrorRedisMessage(service.formatErrorMessage("schema is not defined"));
        }
        if (dropSchema.name.names.size() > 1) {
            return new ErrorRedisMessage(service.formatErrorMessage("Sub-schemas are not allowed"));
        }
        String schema = dropSchema.name.names.get(0);
        return service.getContext().getFoundationDB().run(tr -> dropSchemaHierarchy(tr, schema, dropSchema.ifExists));
    }
}
