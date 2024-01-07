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

import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.protocol.SqlGetSchemaMessage;
import io.netty.util.Attribute;

import java.util.ArrayList;
import java.util.List;

@Command(SqlGetSchemaMessage.COMMAND)
@MaximumParameterCount(SqlGetSchemaMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(SqlGetSchemaMessage.MINIMUM_PARAMETER_COUNT)
public class SqlGetSchemaHandler extends BaseSqlHandler implements Handler {

    public SqlGetSchemaHandler(SqlService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.SQLGETSCHEMA).set(new SqlGetSchemaMessage());
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        Attribute<List<String>> currentSchema = response.getContext().channel().attr(ChannelAttributes.CURRENT_SCHEMA);
        List<RedisMessage> children = new ArrayList<>();
        for (String schema : currentSchema.get()) {
            children.add(new SimpleStringRedisMessage(schema));
        }
        response.writeArray(children);
    }
}
