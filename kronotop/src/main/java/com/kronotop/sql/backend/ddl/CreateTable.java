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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.ddl.model.CreateTableModel;
import com.kronotop.sql.backend.metadata.SchemaNotExistsException;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNameConflictException;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableCreatedEvent;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * This class represents the CreateTable class which extends FoundationDBBackend and implements Executor<SqlNode>.
 * It is responsible for creating a table in the FoundationDB.
 */
public class CreateTable extends FoundationDBBackend implements Executor<SqlNode> {

    public CreateTable(SqlService service) {
        super(service);
    }

    /**
     * Prepares a CreateTableModel object to represent a table creation operation.
     *
     * @param context        The ExecutionContext object containing the names of the context.
     * @param sqlCreateTable The SqlCreateTable object representing the table creation operation.
     * @return The CreateTableModel object containing information about the table schema, name, query, column definitions, and other properties.
     */
    private CreateTableModel prepareCreateTableModel(ExecutionContext context, SqlCreateTable sqlCreateTable) {
        CreateTableModel createTableModel = new CreateTableModel();

        List<String> names = new ArrayList<>();
        if (sqlCreateTable.name.names.size() == 1) {
            // No schema in names
            names.addAll(context.getSchema());
        }
        names.addAll(sqlCreateTable.name.names);

        createTableModel.setSchema(names.subList(0, names.size() - 1));
        createTableModel.setTable(names.get(names.size() - 1));
        createTableModel.setOperator(sqlCreateTable.getOperator().kind);
        createTableModel.setQuery(sqlCreateTable.toString());
        createTableModel.setReplace(sqlCreateTable.getReplace());
        createTableModel.setIfNotExists(sqlCreateTable.ifNotExists);

        List<ColumnModel> columnList = new ArrayList<>();
        for (int i = 0; i < Objects.requireNonNull(sqlCreateTable.columnList).size(); i++) {
            SqlNode sqlNode = sqlCreateTable.columnList.get(i);
            SqlColumnDeclaration column = (SqlColumnDeclaration) sqlNode;

            ColumnModel columnModel = new ColumnModel();
            columnModel.setNames(column.name.names);
            columnModel.setStrategy(column.strategy);
            SqlTypeName dataType = SqlTypeName.valueOf(column.dataType.getTypeName().names.get(0));
            columnModel.setDataType(dataType);
            if (column.expression != null) {
                columnModel.setExpression(column.expression.toString());
            }
            columnList.add(columnModel);
        }
        createTableModel.setColumnList(columnList);
        return createTableModel;
    }

    /**
     * Checks for any conflict between the table name and schema name.
     *
     * @param tr               The transaction to use for the operation.
     * @param createTableModel The model representing the table to create.
     *                         It contains information about the table schema, the table name, the SQL query,
     *                         column definitions, and other properties.
     * @throws TableNameConflictException If the table name conflicts with the schema name.
     */
    private void checkSchemaNameConflict(Transaction tr, CreateTableModel createTableModel) throws TableNameConflictException {
        DirectoryLayout schemaLayout = service.getMetadataService().getSchemaLayout(createTableModel.getSchema());
        if (DirectoryLayer.getDefault().exists(tr, schemaLayout.add(createTableModel.getTable()).asList()).join()) {
            List<String> conflictSchema = new ArrayList<>(createTableModel.getSchema());
            conflictSchema.add(createTableModel.getTable());
            throw new TableNameConflictException(createTableModel.getTable(), conflictSchema);
        }
    }

    /**
     * Creates a table in the FoundationDB.
     *
     * @param tr               The transaction to use for the operation.
     * @param createTableModel The model representing the table to create.
     *                         It contains information about the table schema, the table name, the SQL query,
     *                         column definitions, and other properties.
     * @return The result of the transaction, containing the version stamp and the result message.
     * @throws SchemaNotExistsException    If the schema specified by the createTableModel does not exist.
     *                                     The schema is checked using the DirectoryLayout.
     * @throws TableAlreadyExistsException If a table with the same name already exists in the database
     *                                     and the "ifNotExists" flag in the createTableModel is set to false.
     * @throws TableNameConflictException  If the table name conflicts with the schema name.
     */
    private RedisMessage createTableOnFDB(Transaction tr, CreateTableModel createTableModel) throws SchemaNotExistsException, TableAlreadyExistsException, TableNameConflictException {
        DirectoryLayout schemaLayout = service.getMetadataService().getSchemaLayout(createTableModel.getSchema());
        if (!DirectoryLayer.getDefault().exists(tr, schemaLayout.asList()).join()) {
            throw new SchemaNotExistsException(String.join(".", createTableModel.getSchema()));
        }

        checkSchemaNameConflict(tr, createTableModel);

        try {
            List<String> subpath = schemaLayout.tables().add(createTableModel.getTable()).asList();
            byte[] data = objectMapper.writeValueAsBytes(createTableModel);
            DirectorySubspace subspace = DirectoryLayer.getDefault().create(tr, subpath).join();
            Tuple tuple = Tuple.from(Versionstamp.incomplete(), 1);
            tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), data);
        } catch (CompletionException | JsonProcessingException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                if (!createTableModel.getIfNotExists()) {
                    throw new TableAlreadyExistsException(createTableModel.getTable());
                }
            }
            throw new KronotopException(e);
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    /**
     * Creates a table in the FoundationDB.
     *
     * @param tr               The transaction to use for the operation.
     * @param createTableModel The model representing the table to create.
     * @return The result of the transaction, containing the version stamp and the result message.
     */
    private TransactionResult createTable(Transaction tr, CreateTableModel createTableModel) {
        try {
            RedisMessage result = createTableOnFDB(tr, createTableModel);
            return new TransactionResult(tr.getVersionstamp(), result);
        } catch (SchemaNotExistsException | TableAlreadyExistsException | TableNameConflictException e) {
            String message = service.formatErrorMessage(e.getMessage());
            return new TransactionResult(new ErrorRedisMessage(message));
        }
    }

    /**
     * Publishes a table created event to the SQL metadata events journal.
     *
     * @param result           The result of the transaction, containing the version stamp.
     * @param createTableModel The model representing the table that was created.
     */
    private void publishTableCreatedEvent(TransactionResult result, CreateTableModel createTableModel) {
        byte[] versionstamp = result.getVersionstamp().join();
        byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
        TableCreatedEvent tableCreatedEvent = new TableCreatedEvent(createTableModel.getSchema(), createTableModel.getTable(), tableVersion);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_CREATED, tableCreatedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlCreateTable sqlCreateTable = (SqlCreateTable) node;
        if (sqlCreateTable.columnList == null) {
            return new ErrorRedisMessage("Column list cannot be empty.");
        }

        CreateTableModel createTableModel = prepareCreateTableModel(context, sqlCreateTable);
        TransactionResult result = service.getContext().getFoundationDB().run(tr -> createTable(tr, createTableModel));

        if (result.getVersionstamp() != null) {
            publishTableCreatedEvent(result, createTableModel);
        }
        return result.getResult();
    }
}