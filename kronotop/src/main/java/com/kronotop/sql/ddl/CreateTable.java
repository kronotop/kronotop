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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.kronotop.common.KronotopException;
import com.kronotop.common.resp.RESPError;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.TransactionUtils;
import com.kronotop.core.journal.JournalName;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.*;
import com.kronotop.sql.FoundationDBBackend;
import com.kronotop.sql.ddl.model.ColumnModel;
import com.kronotop.sql.ddl.model.TableModel;
import com.kronotop.sql.metadata.SchemaNotExistsException;
import com.kronotop.sql.metadata.TableAlreadyExistsException;
import com.kronotop.sql.metadata.TableNameConflictException;
import com.kronotop.sql.metadata.events.BroadcastEvent;
import com.kronotop.sql.metadata.events.EventTypes;
import com.kronotop.sql.metadata.events.TableCreatedEvent;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * This class represents the CreateTable class which extends FoundationDBBackend and implements Executor<SqlNode>.
 * It is responsible for creating a table in the FoundationDB.
 */
public class CreateTable extends FoundationDBBackend implements StatementExecutor<SqlNode> {

    public CreateTable(SqlService service) {
        super(service);
    }

    /**
     * Prepares a TableModel object to represent a table creation operation.
     *
     * @param context        The ExecutionContext object containing the names of the context.
     * @param sqlCreateTable The SqlCreateTable object representing the table creation operation.
     * @return TableModel object containing information about the table schema, name, query, column definitions, and other properties.
     */
    private TableModel prepareTableModel(ExecutionContext context, SqlCreateTable sqlCreateTable) throws SqlExecutionException {
        TableModel tableModel = new TableModel();

        String schema = service.getSchemaFromNames(context, sqlCreateTable.name.names);
        String table = service.getTableNameFromNames(sqlCreateTable.name.names);
        tableModel.setSchema(schema);
        tableModel.setTable(table);
        tableModel.setOperator(sqlCreateTable.getOperator().kind);
        tableModel.setQuery(sqlCreateTable.toString());
        tableModel.setReplace(sqlCreateTable.getReplace());
        tableModel.setIfNotExists(sqlCreateTable.ifNotExists);

        List<ColumnModel> columnList = new ArrayList<>();

        // Create the stored ID column first.
        ColumnModel idColumn = new ColumnModel();
        idColumn.setName(KronotopTable.IDStoredColumn.name());
        idColumn.setStrategy(KronotopTable.IDStoredColumn.columnStrategy());
        idColumn.setDataType(KronotopTable.IDStoredColumn.sqlTypeName());
        columnList.add(idColumn);

        for (int i = 0; i < Objects.requireNonNull(sqlCreateTable.columnList).size(); i++) {
            SqlNode sqlNode = sqlCreateTable.columnList.get(i);
            SqlColumnDeclaration column = (SqlColumnDeclaration) sqlNode;

            if (column.name.getSimple().equals(KronotopTable.IDStoredColumn.name())) {
                throw new SqlExecutionException("Cannot CREATE generated column 'id'");
            }

            ColumnModel columnModel = new ColumnModel();
            columnModel.setName(column.name.getSimple());
            columnModel.setStrategy(column.strategy);
            SqlTypeName dataType = SqlTypeName.valueOf(column.dataType.getTypeName().names.get(0));
            columnModel.setDataType(dataType);
            if (column.expression != null) {
                columnModel.setExpression(column.expression.toString());
            }
            columnList.add(columnModel);
        }
        tableModel.setColumnList(columnList);
        return tableModel;
    }

    /**
     * Checks for any conflict between the table name and schema name.
     *
     * @param tr         The transaction to use for the operation.
     * @param tableModel The model representing the table to create.
     *                   It contains information about the table schema, the table name, the SQL query,
     *                   column definitions, and other properties.
     * @throws TableNameConflictException If the table name conflicts with the schema name.
     */
    private void checkSchemaNameConflict(Transaction tr, TableModel tableModel) throws TableNameConflictException {
        DirectoryLayout schemaLayout = service.getMetadataService().getSchemaLayout(tableModel.getSchema());
        if (DirectoryLayer.getDefault().exists(tr, schemaLayout.add(tableModel.getTable()).asList()).join()) {
            throw new TableNameConflictException(tableModel.getTable(), tableModel.getSchema());
        }
    }

    /**
     * Creates a table in the FoundationDB.
     *
     * @param tr         The transaction to use for the operation.
     * @param tableModel The model representing the table to create.
     *                   It contains information about the table schema, the table name, the SQL query,
     *                   column definitions, and other properties.
     * @return The result of the transaction, containing the version stamp and the result message.
     * @throws SchemaNotExistsException    If the schema specified by the tableModel does not exist.
     *                                     The schema is checked using the DirectoryLayout.
     * @throws TableAlreadyExistsException If a table with the same name already exists in the database
     *                                     and the "ifNotExists" flag in the tableModel is set to false.
     * @throws TableNameConflictException  If the table name conflicts with the schema name.
     */
    private RedisMessage createTableOnFDB(Transaction tr, TableModel tableModel) throws SchemaNotExistsException, TableAlreadyExistsException, TableNameConflictException {
        DirectoryLayout schemaLayout = service.getMetadataService().getSchemaLayout(tableModel.getSchema());
        if (!DirectoryLayer.getDefault().exists(tr, schemaLayout.asList()).join()) {
            throw new SchemaNotExistsException(String.join(".", tableModel.getSchema()));
        }

        checkSchemaNameConflict(tr, tableModel);

        try {
            List<String> subpath = schemaLayout.tables().add(tableModel.getTable()).asList();
            byte[] data = objectMapper.writeValueAsBytes(tableModel);
            DirectorySubspace subspace = DirectoryLayer.getDefault().create(tr, subpath).join();
            Tuple tuple = Tuple.from(Versionstamp.incomplete());
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), data);
        } catch (CompletionException | JsonProcessingException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                if (!tableModel.getIfNotExists()) {
                    throw new TableAlreadyExistsException(tableModel.getTable());
                }
            }
            throw new KronotopException(e);
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    /**
     * Creates a table in the FoundationDB.
     *
     * @param context    The ExecutionContext object containing the names of the context.
     * @param tr         The transaction to use for the operation.
     * @param tableModel The model representing the table to create.
     *                   It contains information about the table schema, the table name, the SQL query,
     *                   column definitions, and other properties.
     * @return RedisMessage object containing the result of the transaction.
     */
    private RedisMessage createTable(ExecutionContext context, Transaction tr, TableModel tableModel) {
        try {
            RedisMessage result = createTableOnFDB(tr, tableModel);

            CompletableFuture<byte[]> versionstampFuture = tr.getVersionstamp();
            TransactionUtils.addPostCommitHook(() -> {
                byte[] versionstamp = versionstampFuture.join();
                byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
                TableCreatedEvent tableCreatedEvent = new TableCreatedEvent(tableModel.getSchema(), tableModel.getTable(), tableVersion);
                BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_CREATED, tableCreatedEvent);
                service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
            }, context.getRequest().getChannelContext());

            return result;
        } catch (SchemaNotExistsException | TableAlreadyExistsException | TableNameConflictException e) {
            return new ErrorRedisMessage(RESPError.SQL, e.getMessage());
        }
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException, SqlExecutionException {
        SqlCreateTable sqlCreateTable = (SqlCreateTable) node;
        if (sqlCreateTable.columnList == null) {
            return new ErrorRedisMessage(RESPError.SQL, "column list cannot be empty");
        }

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), context.getRequest().getChannelContext());

        TableModel tableModel = prepareTableModel(context, sqlCreateTable);
        RedisMessage result = createTable(context, tr, tableModel);
        TransactionUtils.commitIfAutoCommitEnabled(tr, context.getRequest().getChannelContext());
        return result;
    }
}