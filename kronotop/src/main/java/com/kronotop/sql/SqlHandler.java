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

import com.kronotop.common.resp.RESPError;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.protocol.SqlMessage;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlValidatorException;

import java.util.List;

@Command(SqlMessage.COMMAND)
public class SqlHandler extends BaseSqlHandler implements Handler {

    public SqlHandler(SqlService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SQL).set(new SqlMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        SqlMessage sqlMessage = request.attr(MessageTypes.SQL).get();
        try {
            SqlNode sqlTree = Parser.parse(sqlMessage.getQuery());

            // DDL Commands
            ExecutionContext executionContext = new ExecutionContext(request, response);
            String schema = request.getContext().channel().attr(ChannelAttributes.SCHEMA).get();
            executionContext.setSchema(schema);

            RedisMessage result;
            Executor<SqlNode> executor = service.ddlExecutors.get(sqlTree.getKind());
            if (executor != null) {
                result = executor.execute(executionContext, sqlTree);
            } else {
                response.writeError(RESPError.SQL, String.format("Unknown SQL command: %s", sqlTree.getKind()));
                return;
            }
            response.writeRedisMessage(result);
        } catch (SqlParseException e) {
            // redis-cli has a problem with bulk errors:
            // Error: Protocol error, got "!" as reply type byte
            List<String> messages = List.of(e.getMessage().split("\n"));
            response.writeError(RESPError.SQL, messages.get(0));
        } catch (SqlValidatorException e) {
            response.writeError(RESPError.SQL, String.format("%s %s", e.getMessage(), e.getCause().getMessage()));
        }
    }
}
