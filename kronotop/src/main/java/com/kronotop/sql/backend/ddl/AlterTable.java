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
import com.apple.foundationdb.directory.DirectoryMoveException;
import com.apple.foundationdb.directory.NoSuchDirectoryException;
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
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableRenamedEvent;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorException;

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
            DirectoryLayer.getDefault().move(tr, oldTableLayout.asList(), nextTableLayout.asList()).join();
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

    private void publishTableRenamedEvent(ExecutionContext context, SqlAlterTable sqlAlterTable) {
        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String oldTableName = service.getTableNameFromNames(sqlAlterTable.name.names);
        String newTableName = sqlAlterTable.newTableName.getSimple();
        TableRenamedEvent tableRenamedEvent = new TableRenamedEvent(schema, oldTableName, newTableName);
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
                    publishTableRenamedEvent(context, sqlAlterTable);
                    break;
                default:
                    throw new KronotopException("Unknown ALTER type: " + sqlAlterTable.alterType);
            }
        }
        return result.getResult();
    }
}
