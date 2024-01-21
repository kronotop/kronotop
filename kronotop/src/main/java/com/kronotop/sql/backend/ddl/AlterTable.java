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
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.FoundationDBBackend;
import com.kronotop.sql.backend.ddl.model.CreateTableModel;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableRenamedEvent;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionException;

public class AlterTable extends FoundationDBBackend implements Executor<SqlNode> {
    public AlterTable(SqlService service) {
        super(service);
    }

    private RedisMessage renameTable(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) throws TableNotExistsException, TableAlreadyExistsException {
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
            CreateTableModel createTableModel = objectMapper.readValue(next.getValue(), CreateTableModel.class);
            createTableModel.setTable(newTable);
            byte[] data = objectMapper.writeValueAsBytes(createTableModel);
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

    private TransactionResult alterTable(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        RedisMessage redisMessage;
        try {
            switch (sqlAlterTable.alterType) {
                case RENAME_TABLE:
                    redisMessage = renameTable(tr, context, sqlAlterTable);
                    break;
                default:
                    throw new KronotopException("Unknown ALTER type: " + sqlAlterTable.alterType);
            }
            return new TransactionResult(tr.getVersionstamp(), redisMessage);
        } catch (TableNotExistsException | TableAlreadyExistsException | KronotopException e) {
            String message = service.formatErrorMessage(e.getMessage());
            return new TransactionResult(new ErrorRedisMessage(message));
        }
    }

    private void publishTableRenamedEvent(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String oldTableName = service.getTableNameFromNames(sqlAlterTable.name.names);
        String newTableName = sqlAlterTable.newTableName.getSimple();
        byte[] versionstamp = result.getVersionstamp().join();
        TableRenamedEvent tableRenamedEvent = new TableRenamedEvent(schema, oldTableName, newTableName, versionstamp);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_RENAMED, tableRenamedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) node;
        TransactionResult result = service.getContext().getFoundationDB().run(tr -> alterTable(tr, context, sqlAlterTable));

        if (result.getVersionstamp() != null) {
            switch (sqlAlterTable.alterType) {
                case RENAME_TABLE:
                    publishTableRenamedEvent(result, context, sqlAlterTable);
                    break;
                default:
                    throw new KronotopException("Unknown ALTER type: " + sqlAlterTable.alterType);
            }
        }
        return result.getResult();
    }
}
