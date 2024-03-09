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

package com.kronotop.sql;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.google.common.collect.ImmutableList;
import com.kronotop.common.KronotopException;
import com.kronotop.common.utils.DirectoryLayout;
import com.kronotop.core.CommandHandlerService;
import com.kronotop.core.Context;
import com.kronotop.core.JSONUtils;
import com.kronotop.core.KronotopService;
import com.kronotop.server.Handlers;
import com.kronotop.sql.backend.ddl.*;
import com.kronotop.sql.backend.ddl.model.TableModel;
import com.kronotop.sql.backend.metadata.SqlMetadataService;
import com.kronotop.sql.plan.PlanCache;
import com.kronotop.sql.plan.PlanExecutor;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.EnumSet;
import java.util.HashMap;

/*
Imagination is more important than knowledge. Knowledge is limited. Imagination encircles the world.

-- Albert Einstein, in Saturday Evening Post, 26 October 1929
 */

/**
 * SqlService represents a service that handles SQL commands in the Kronotop database system.
 */
public class SqlService extends CommandHandlerService implements KronotopService {
    public static final String NAME = "SQL";
    protected final EnumSet<SqlKind> statements;
    protected final HashMap<SqlKind, Executor<SqlNode>> executors = new HashMap<>();
    protected final SqlMetadataService metadataService;
    // TODO: Invalidate the plan cache if a schema has changed.
    protected final PlanCache planCache = new PlanCache();
    protected final PlanExecutor planExecutor;
    private final Context context;

    public SqlService(Context context, Handlers handlers) {
        super(context, handlers);
        this.context = context;
        this.metadataService = context.getService(SqlMetadataService.NAME);
        this.planExecutor = new PlanExecutor(context);

        this.statements = EnumSet.of(
                SqlKind.INSERT,
                SqlKind.SELECT,
                SqlKind.ORDER_BY,
                SqlKind.DELETE,
                SqlKind.UPDATE
        );

        this.executors.put(SqlKind.CREATE_SCHEMA, new CreateSchema(this));
        this.executors.put(SqlKind.CREATE_TABLE, new CreateTable(this));
        this.executors.put(SqlKind.DROP_SCHEMA, new DropSchema(this));
        this.executors.put(SqlKind.DROP_TABLE, new DropTable(this));
        this.executors.put(SqlKind.ALTER_TABLE, new AlterTable(this));
        this.executors.put(SqlKind.SET_OPTION, new SetOption(this));

        registerHandler(new SqlHandler(this));
    }

    /**
     * Retrieves the schema names from the given list of names. It assumes that if the provided {@link ImmutableList <String>}
     * only has one item, this item is the table name.
     *
     * <p>
     * If the number of names is equal to 1, returns the schema list from the provided {@link ExecutionContext} object.
     * Otherwise, returns a sublist of names without the last element.
     * </p>
     *
     * @param context the execution context object containing the schema list
     * @param names   the list of names representing the hierarchy to search for the schema
     * @return the list of schema names
     */
    public String getSchemaFromNames(ExecutionContext context, ImmutableList<String> names) {
        if (names.size() == 1) {
            return context.getSchema();
        } else {
            return names.get(0);
        }
    }

    /**
     * Retrieves the table name from the given list of names. It assumes that the last element in the list is the table name.
     *
     * @param names the list of names representing the hierarchy to search for the table
     * @return the table name
     */
    public String getTableNameFromNames(ImmutableList<String> names) {
        return names.get(names.size() - 1);
    }

    public SqlMetadataService getMetadataService() {
        return metadataService;
    }

    public DirectorySubspace openTableSubspace(Transaction tr, String schema, String table) {
        DirectoryLayout tableLayout = metadataService.getSchemaLayout(schema).tables().add(table);
        return DirectoryLayer.getDefault().open(tr, tableLayout.asList()).join();
    }

    public TableModel getLatestTableModel(Transaction tr, DirectorySubspace subspace) {
        AsyncIterator<KeyValue> iterator = tr.getRange(subspace.range(), 1, true).iterator();
        if (!iterator.hasNext()) {
            throw new KronotopException("No data found for this table range");
        }
        KeyValue next = iterator.next();
        return JSONUtils.readValue(next.getValue(), TableModel.class);
    }

    public void saveTableModel(Transaction tr, TableModel tableModel, DirectorySubspace subspace) {
        byte[] data = JSONUtils.writeValueAsBytes(tableModel);
        Tuple tuple = Tuple.from(Versionstamp.incomplete());
        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subspace.packWithVersionstamp(tuple), data);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Context getContext() {
        return context;
    }

    @Override
    public void shutdown() {

    }
}
