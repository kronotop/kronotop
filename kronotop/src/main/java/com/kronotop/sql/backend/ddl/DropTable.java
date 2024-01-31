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
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableDroppedEvent;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlDropTable;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.List;
import java.util.concurrent.CompletionException;

public class DropTable extends FoundationDBBackend implements Executor<SqlNode> {

    public DropTable(SqlService service) {
        super(service);
    }

    private RedisMessage dropTable(Transaction tr, String schema, String table, boolean ifExists) {
        List<String> subpath = service.getMetadataService().getSchemaLayout(schema).tables().add(table).asList();
        try {
            DirectoryLayer.getDefault().remove(tr, subpath).join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                if (ifExists) {
                    return new SimpleStringRedisMessage(Response.OK);
                } else {
                    Throwable throwable = new TableNotExistsException(table);
                    return new ErrorRedisMessage(throwable.getMessage());
                }
            }
            throw new KronotopException(e);
        }

        // Trigger the other nodes
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_DROPPED, new TableDroppedEvent(schema, table));
        service.getContext().getJournal().getPublisher().publish(tr, JournalName.sqlMetadataEvents(), broadcastEvent);

        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlDropTable dropTable = (SqlDropTable) node;
        String schema = service.getSchemaFromNames(context, dropTable.name.names);
        String table = dropTable.name.names.get(dropTable.name.names.size() - 1);
        return service.getContext().getFoundationDB().run(tr -> dropTable(tr, schema, table, dropTable.ifExists));
    }
}
