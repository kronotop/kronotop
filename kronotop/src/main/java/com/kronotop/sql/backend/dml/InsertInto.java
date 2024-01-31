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

package com.kronotop.sql.backend.dml;

import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.FoundationDBBackend;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorException;

public class InsertInto extends FoundationDBBackend implements Executor<SqlNode> {

    public InsertInto(SqlService service) {
        super(service);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        return null;
    }
}
