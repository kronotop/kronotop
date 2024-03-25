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
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.core.TransactionUtils;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.*;
import com.kronotop.sql.FoundationDBBackend;
import com.kronotop.sql.ddl.altertable.*;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.concurrent.CompletableFuture;

/**
 * AlterTable is a class that represents an ALTER TABLE statement in the Kronotop database system.
 * <p>
 * This class extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 * It allows for the execution of ALTER TABLE statements in the database.
 */
public class AlterTable extends FoundationDBBackend implements StatementExecutor<SqlNode> {
    private final AlterType renameTable;
    private final AlterType addColumn;
    private final AlterType dropColumn;
    private final AlterType renameColumn;

    public AlterTable(SqlService service) {
        super(service);
        this.renameTable = new RenameTable(service);
        this.addColumn = new AddColumn(service);
        this.dropColumn = new DropColumn(service);
        this.renameColumn = new RenameColumn(service);
    }

    private RedisMessage alterTable(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        try {
            RedisMessage redisMessage = switch (sqlAlterTable.alterType) {
                case RENAME_TABLE -> renameTable.alter(tr, context, sqlAlterTable);
                case ADD_COLUMN -> addColumn.alter(tr, context, sqlAlterTable);
                case DROP_COLUMN -> dropColumn.alter(tr, context, sqlAlterTable);
                case RENAME_COLUMN -> renameColumn.alter(tr, context, sqlAlterTable);
                default -> new ErrorRedisMessage(RESPError.SQL, "Unknown ALTER type: " + sqlAlterTable.alterType);
            };

            CompletableFuture<byte[]> versionstampFuture = tr.getVersionstamp();
            TransactionUtils.addPostCommitHook(() -> {
                switch (sqlAlterTable.alterType) {
                    case RENAME_TABLE -> renameTable.notifyCluster(versionstampFuture, context, sqlAlterTable);
                    case ADD_COLUMN -> addColumn.notifyCluster(versionstampFuture, context, sqlAlterTable);
                    case DROP_COLUMN -> dropColumn.notifyCluster(versionstampFuture, context, sqlAlterTable);
                    case RENAME_COLUMN -> renameColumn.notifyCluster(versionstampFuture, context, sqlAlterTable);
                }
            }, context.getRequest().getChannelContext());
            return redisMessage;
        } catch (SqlExecutionException | KronotopException e) {
            if (e instanceof SqlExecutionException) {
                return new ErrorRedisMessage(RESPError.SQL, e.getCause().getMessage());
            }
            return new ErrorRedisMessage(RESPError.SQL, e.getMessage());
        }
    }

    private void checkVirtualColumn(SqlIdentifier sqlIdentifier, KronotopTable.StoredColumn virtualColumn) throws SqlExecutionException {
        if (virtualColumn.name().equals(sqlIdentifier.getSimple())) {
            throw new SqlExecutionException(String.format("Cannot ALTER generated column '%s'", virtualColumn.name()));
        }
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException, SqlExecutionException {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) node;

        if (sqlAlterTable.alterType == SqlAlterTable.AlterType.ADD_COLUMN || sqlAlterTable.alterType == SqlAlterTable.AlterType.DROP_COLUMN) {
            assert sqlAlterTable.columnList != null;
            for (SqlNode column : sqlAlterTable.columnList.getList()) {
                if (column instanceof SqlIdentifier sqlIdentifier) {
                    checkVirtualColumn(sqlIdentifier, KronotopTable.IDStoredColumn);
                } else if (column instanceof SqlColumnDeclaration sqlColumnDeclaration) {
                    checkVirtualColumn(sqlColumnDeclaration.name, KronotopTable.IDStoredColumn);
                }
            }
        } else if (sqlAlterTable.alterType == SqlAlterTable.AlterType.RENAME_COLUMN) {
            checkVirtualColumn(sqlAlterTable.columnName, KronotopTable.IDStoredColumn);
        }

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), context.getRequest().getChannelContext());
        RedisMessage result = alterTable(tr, context, sqlAlterTable);

        TransactionUtils.commitIfAutoCommitEnabled(tr, context.getRequest().getChannelContext());
        return result;
    }
}
