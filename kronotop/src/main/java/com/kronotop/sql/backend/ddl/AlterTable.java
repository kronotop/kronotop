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
import com.kronotop.common.KronotopException;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.FoundationDBBackend;
import com.kronotop.sql.backend.ddl.altertable.AddColumn;
import com.kronotop.sql.backend.ddl.altertable.AlterType;
import com.kronotop.sql.backend.ddl.altertable.RenameTable;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorException;

/**
 * AlterTable is a class that represents an ALTER TABLE statement in the Kronotop database system.
 * <p>
 * This class extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 * It allows for the execution of ALTER TABLE statements in the database.
 */
public class AlterTable extends FoundationDBBackend implements Executor<SqlNode> {
    private final AlterType renameTable;
    private final AlterType addColumn;

    public AlterTable(SqlService service) {
        super(service);
        this.renameTable = new RenameTable(service, objectMapper);
        this.addColumn = new AddColumn(service, objectMapper);
    }

    private TransactionResult alterTable(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        RedisMessage redisMessage;
        try {
            switch (sqlAlterTable.alterType) {
                case RENAME_TABLE:
                    redisMessage = renameTable.alter(tr, context, sqlAlterTable);
                    break;
                case ADD_COLUMN:
                    redisMessage = addColumn.alter(tr, context, sqlAlterTable);
                    break;
                default:
                    throw new KronotopException("Unknown ALTER type: " + sqlAlterTable.alterType);
            }
            return new TransactionResult(tr.getVersionstamp(), redisMessage);
        } catch (TableNotExistsException | TableAlreadyExistsException | ColumnAlreadyExistsException |
                 KronotopException e) {
            String message = service.formatErrorMessage(e.getMessage());
            return new TransactionResult(new ErrorRedisMessage(message));
        }
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) node;
        TransactionResult result = service.getContext().getFoundationDB().run(tr -> alterTable(tr, context, sqlAlterTable));

        if (result.getVersionstamp() != null) {
            switch (sqlAlterTable.alterType) {
                case RENAME_TABLE:
                    renameTable.notifyCluster(result, context, sqlAlterTable);
                    break;
                case ADD_COLUMN:
                    addColumn.notifyCluster(result, context, sqlAlterTable);
                    break;
                default:
                    throw new KronotopException("Unknown ALTER type: " + sqlAlterTable.alterType);
            }
        }
        return result.getResult();
    }
}
