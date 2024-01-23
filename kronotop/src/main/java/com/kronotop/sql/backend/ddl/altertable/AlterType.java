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

import com.apple.foundationdb.Transaction;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.TransactionResult;
import com.kronotop.sql.backend.ddl.ColumnAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableAlreadyExistsException;
import com.kronotop.sql.backend.metadata.TableNotExistsException;
import com.kronotop.sql.parser.SqlAlterTable;

/**
 * The AlterType interface represents an operation that can be performed on a database table
 * as part of an ALTER TABLE command.
 */
public interface AlterType {
    RedisMessage alter(Transaction tr, ExecutionContext context, SqlAlterTable sqlAlterTable)
            throws TableNotExistsException, TableAlreadyExistsException, ColumnAlreadyExistsException;

    void notifyCluster(TransactionResult result, ExecutionContext context, SqlAlterTable sqlAlterTable);
}
