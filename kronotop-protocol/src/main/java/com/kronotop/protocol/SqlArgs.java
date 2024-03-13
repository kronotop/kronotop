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

package com.kronotop.protocol;

import io.lettuce.core.protocol.CommandArgs;

/**
 * Represents a wrapper class for constructing SQL arguments for a command.
 */
public class SqlArgs {
    private String[] queries;
    private String[] returning;

    public SqlArgs queries(String... queries) {
        this.queries = queries;
        return this;
    }

    public SqlArgs returning(String... fields) {
        this.returning = fields;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (queries != null) {
            for (String query : queries) {
                args.add(query);
            }
        }

        if (returning != null) {
            args.add(SqlKeywords.RETURNING);
            for (String field: returning) {
                args.add(field);
            }
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static SqlArgs queries(String... queries) {
            return new SqlArgs().queries(queries);
        }

        public static SqlArgs returning(String... field) {
            return new SqlArgs().returning(field);
        }
    }
}