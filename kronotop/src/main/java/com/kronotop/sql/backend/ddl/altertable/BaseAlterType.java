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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.core.journal.JournalName;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.metadata.events.BroadcastEvent;
import com.kronotop.sql.backend.metadata.events.EventTypes;
import com.kronotop.sql.backend.metadata.events.TableAlteredEvent;
import com.kronotop.sql.parser.SqlAlterTable;

import java.util.List;

class BaseAlterType {
    protected final SqlService service;

    BaseAlterType(SqlService service) {
        this.service = service;
    }

    void publishTableAlteredEvent(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable) {
        List<String> schema = service.getSchemaFromNames(context, sqlAlterTable.name.names);
        String table = service.getTableNameFromNames(sqlAlterTable.name.names);
        byte[] versionstamp = result.getVersionstamp().join();
        byte[] tableVersion = Versionstamp.complete(versionstamp).getBytes();
        TableAlteredEvent columnAddedEvent = new TableAlteredEvent(schema, table, tableVersion);
        BroadcastEvent broadcastEvent = new BroadcastEvent(EventTypes.TABLE_ALTERED, columnAddedEvent);
        service.getContext().getJournal().getPublisher().publish(JournalName.sqlMetadataEvents(), broadcastEvent);
    }
}
