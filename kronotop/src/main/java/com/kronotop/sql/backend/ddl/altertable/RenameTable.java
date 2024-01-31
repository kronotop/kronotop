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

package com.kronotop.sql.backend.ddl.altertable;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.*;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.*;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableRenamedEvent;
import com.kronotop.sql.parser.SqlAlterTable;

import java.util.concurrent.CompletionException;

/**
 * The RenameTable class is responsible for renaming a table in the database.
 */
public class RenameTable implements AlterType {
    private final SqlService service;

    public RenameTable(SqlService service) {
        this.service = service;
    }

    /**
     * Renames a table in the database.
     *
     * @param tr            The current transaction object.
     * @param context       The execution context object containing the schema list.
     * @param sqlAlterTable The object containing information for the ALTER TABLE command.
     * @return A RedisMessage indicating the success or failure of the operation.
     * @throws SqlExecutionException if there is an error during the execution of the SQL statement.
     */
    public RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) throws SqlExecutionException {
        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        String newTable = sqlAlterTable.newTableName.getSimple();
        if (table.equals(newTable)) {
            throw new SqlExecutionException(new TableAlreadyExistsException(newTable));
        }

        String schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        DirectoryLayout oldTableLayout = service.getMetadataService().getSchemaLayout(schema).tables().add(table);
        DirectoryLayout nextTableLayout = service.getMetadataService().getSchemaLayout(schema).tables().add(newTable);

        try {
            DirectorySubspace subspace = DirectoryLayer.getDefault().move(tr, oldTableLayout.asList(), nextTableLayout.asList()).join();

            // Create a new version with the new table name.
            //
            // First get the latest table version
            AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
            if (!iterator.hasNext()) {
                throw new KronotopException(String.format("Table '%s' exists but no version found", table));
            }
            KeyValue next = iterator.next();
            TableModel tableModel = JSONUtils.readValue(next.getValue(), TableModel.class);
            tableModel.setTable(newTable);
            byte[] data = JSONUtils.writeValueAsBytes(tableModel);
            Tuple tuple = Tuple.from(Versionstamp.incomplete(), 1);
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), data);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new SqlExecutionException(new TableNotExistsException(table));
            } else if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new SqlExecutionException(new TableAlreadyExistsException(newTable));
            } else if (e.getCause() instanceof DirectoryMoveException) {
                throw new KronotopException("Invalid move location is specified: " + e.getCause().getMessage());
            }
            throw new KronotopException(e.getCause().getMessage());
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    public void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        String schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String oldTableName = service.getTableNameFromNames(sqlAlterTable.name.names);
        String newTableName = sqlAlterTable.newTableName.getSimple();
        byte[] versionstamp = result.getVersionstamp().join();
        byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
        TableRenamedEvent tableRenamedEvent = new TableRenamedEvent(schema, oldTableName, newTableName, tableVersion);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_RENAMED, tableRenamedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }
}
