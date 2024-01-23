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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableRenamedEvent;
import com.kronotop.sql.parser.SqlAlterTable;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionException;

/**
 * The RenameTable class is responsible for renaming a table in the database.
 */
public class RenameTable implements AlterType {
    private final SqlService service;
    private final ObjectMapper objectMapper;

    public RenameTable(SqlService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    /**
     * Renames a table in the database.
     *
     * @param tr            the transaction object used to perform the operation
     * @param context       the execution context object containing the schema information
     * @param sqlAlterTable the SqlAlterTable object representing the ALTER TABLE command
     * @return a RedisMessage indicating the success or failure of the operation
     * @throws TableNotExistsException       if the table does not exist in the database
     * @throws TableAlreadyExistsException   if the new table name already exists in the database
     * @throws KronotopException            if there is an error during the operation
     */
    public RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable)
            throws TableNotExistsException, TableAlreadyExistsException {
        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        String newTable = sqlAlterTable.newTableName.getSimple();
        if (table.equals(newTable)) {
            throw new TableAlreadyExistsException(newTable);
        }

        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
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
            TableModel tableModel = objectMapper.readValue(next.getValue(), TableModel.class);
            tableModel.setTable(newTable);
            byte[] data = objectMapper.writeValueAsBytes(tableModel);
            Tuple tuple = Tuple.from(Versionstamp.incomplete(), 1);
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), data);
        } catch (CompletionException e) {
            if (e.getCause() instanceof NoSuchDirectoryException) {
                throw new TableNotExistsException(table);
            } else if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new TableAlreadyExistsException(newTable);
            } else if (e.getCause() instanceof DirectoryMoveException) {
                // TODO: Better error message is needed here.
                throw new KronotopException("Invalid move location is specified: " + e.getCause().getMessage());
            }
            throw new KronotopException(e.getCause().getMessage());
        } catch (IOException e) {
            throw new KronotopException(e.getMessage());
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    public void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String oldTableName = service.getTableNameFromNames(sqlAlterTable.name.names);
        String newTableName = sqlAlterTable.newTableName.getSimple();
        byte[] versionstamp = result.getVersionstamp().join();
        byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
        TableRenamedEvent tableRenamedEvent = new TableRenamedEvent(schema, oldTableName, newTableName, tableVersion);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_RENAMED, tableRenamedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }
}
