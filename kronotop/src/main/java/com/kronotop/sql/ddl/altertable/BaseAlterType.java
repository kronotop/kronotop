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

package com.kronotop.sql.ddl.altertable;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.core.journal.JournalName;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.SqlExecutionException;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.ddl.ColumnNotExistsException;
import com.kronotop.sql.ddl.model.ColumnModel;
import com.kronotop.sql.ddl.model.TableModel;
import com.kronotop.sql.metadata.events.BroadcastEvent;
import com.kronotop.sql.metadata.events.EventTypes;
import com.kronotop.sql.metadata.events.TableAlteredEvent;
import com.kronotop.sql.parser.SqlAlterTable;

import java.util.concurrent.CompletableFuture;

class BaseAlterType {
    protected final SqlService service;

    BaseAlterType(SqlService service) {
        this.service = service;
    }

    protected int findColumnIndex(String targetColumnName, TableModel tableModel) throws SqlExecutionException {
        for (int index = 0; index < tableModel.getColumnList().size(); index++) {
            ColumnModel column = tableModel.getColumnList().get(index);
            String columnName = column.getName();
            if (targetColumnName.equals(columnName)) {
                return index;
            }
        }
        throw new SqlExecutionException(new ColumnNotExistsException(targetColumnName, tableModel.getTable()));
    }

    /**
     * Publishes a table altered event to the specified journal.
     *
     * @param versionstampFuture The future containing the versionstamp for the event
     * @param context            The execution context
     * @param sqlAlterTable      The SqlAlterTable object representing the ALTER TABLE command
     */
    void publishTableAlteredEvent(CompletableFuture<byte[]> versionstampFuture, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        String schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        byte[] versionstamp = versionstampFuture.join();
        byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
        TableAlteredEvent columnAddedEvent = new TableAlteredEvent(schema, table, tableVersion);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_ALTERED, columnAddedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }
}
