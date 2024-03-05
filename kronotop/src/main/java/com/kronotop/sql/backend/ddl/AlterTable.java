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
import com.kronotop.common.resp.RESPError;
import com.kronotop.core.TransactionUtils;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.FoundationDBBackend;
import com.kronotop.sql.backend.ddl.altertable.*;
import com.kronotop.sql.parser.SqlAlterTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.concurrent.CompletableFuture;

/**
 * AlterTable is a class that represents an ALTER TABLE statement in the Kronotop database system.
 * <p>
 * This class extends the FoundationDBBackend class and implements the Executor<SqlNode> interface.
 * It allows for the execution of ALTER TABLE statements in the database.
 */
public class AlterTable extends FoundationDBBackend implements Executor<SqlNode> {
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

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlAlterTable sqlAlterTable = (SqlAlterTable) node;

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), context.getRequest().getChannelContext());
        RedisMessage result = alterTable(tr, context, sqlAlterTable);

        TransactionUtils.commitIfOneOff(tr, context.getRequest().getChannelContext());
        return result;
    }
}
