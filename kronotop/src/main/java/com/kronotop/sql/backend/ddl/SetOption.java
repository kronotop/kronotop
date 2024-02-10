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

import com.kronotop.server.ChannelAttributes;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.ExecutionContext;
import com.kronotop.sql.Executor;
import com.kronotop.sql.SqlService;
import com.kronotop.sql.backend.metadata.SchemaNotExistsException;
import io.netty.util.Attribute;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.validate.SqlValidatorException;

public class SetOption implements Executor<SqlNode> {
    private final SqlService service;

    public SetOption(SqlService service) {
        this.service = service;
    }

    private RedisMessage setSessionScope(ExecutionContext context, SqlSetOption sqlSetOption) {
        SqlIdentifier value = (SqlIdentifier) sqlSetOption.getValue();
        if (value == null || value.names.isEmpty()) {
            return new ErrorRedisMessage(service.formatErrorMessage("value cannot be empty"));
        }

        String key = String.join(".", sqlSetOption.getName().names);
        if (key.equalsIgnoreCase(Key.SCHEMA.toString())) {
            try {
                service.getMetadataService().findOrLoadSchemaMetadata(value.getSimple());
            } catch (SchemaNotExistsException e) {
                return new ErrorRedisMessage(e.getMessage());
            }
            context.getResponse().getChannelContext().channel().attr(ChannelAttributes.SCHEMA).set(value.getSimple());
        } else {
            return new ErrorRedisMessage("Unknown key: " + key);
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    private RedisMessage resetSessionScope(ExecutionContext context, SqlSetOption sqlSetOption) {
        String key = String.join(".", sqlSetOption.getName().names);
        if (key.equalsIgnoreCase(Key.SCHEMA.toString())) {
            Attribute<String> schema = context.getResponse().getChannelContext().channel().attr(ChannelAttributes.SCHEMA);
            if (service.getContext().getConfig().hasPath("sql.default_schema")) {
                schema.set(service.getContext().getConfig().getString("sql.default_schema"));
            }
        } else {
            return new ErrorRedisMessage("Unknown key: " + key);
        }
        return new SimpleStringRedisMessage(Response.OK);
    }

    @Override
    public RedisMessage execute(ExecutionContext context, SqlNode node) throws SqlValidatorException {
        SqlSetOption sqlSetOption = (SqlSetOption) node;
        String scope = sqlSetOption.getScope();

        // Default scope is null, set it to SESSION
        if (scope == null) {
            scope = "SESSION";
        }

        RedisMessage response;
        if (scope.equalsIgnoreCase(Scope.SESSION.toString())) {
            SqlIdentifier value = (SqlIdentifier) sqlSetOption.getValue();
            // If value is a non-null, this should be a 'set' operation
            if (value != null) {
                response = setSessionScope(context, sqlSetOption);
            } else {
                return resetSessionScope(context, sqlSetOption);
            }
        } else {
            response = new ErrorRedisMessage(service.formatErrorMessage("Unsupported scope: " + scope));
        }
        return response;
    }

    enum Scope {
        SESSION
    }

    enum Key {
        SCHEMA
    }
}
