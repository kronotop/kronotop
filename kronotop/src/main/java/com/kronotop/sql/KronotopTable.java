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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class KronotopTable extends AbstractTable implements ScannableTable {
    private final String tableName;
    private final List<String> fieldNames;
    private final List<SqlTypeName> fieldTypes;
    private final KronotopTableStatistic statistic;

    private RelDataType rowType;

    private KronotopTable(String tableName, List<String> fieldNames, List<SqlTypeName> fieldTypes, KronotopTableStatistic statistic) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.statistic = statistic;
    }

    public static Builder newBuilder(String tableName) {
        return new Builder(tableName);
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>(fieldNames.size());

            for (int i = 0; i < fieldNames.size(); i++) {
                RelDataType fieldType = typeFactory.createSqlType(fieldTypes.get(i));
                RelDataTypeField field = new RelDataTypeFieldImpl(fieldNames.get(i), i, fieldType);
                fields.add(field);
            }

            rowType = new RelRecordType(StructKind.PEEK_FIELDS, fields, false);
        }

        return rowType;
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public static final class Builder {

        private final String tableName;
        private final List<String> fieldNames = new ArrayList<>();
        private final List<SqlTypeName> fieldTypes = new ArrayList<>();
        private long rowCount;

        private Builder(String tableName) {
            if (tableName == null || tableName.isEmpty()) {
                throw new IllegalArgumentException("Table name cannot be null or empty");
            }

            this.tableName = tableName;
        }

        public Builder addField(String name, SqlTypeName typeName) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Field name cannot be null or empty");
            }

            if (fieldNames.contains(name)) {
                throw new IllegalArgumentException("Field already defined: " + name);
            }

            fieldNames.add(name);
            fieldTypes.add(typeName);

            return this;
        }

        public Builder withRowCount(long rowCount) {
            this.rowCount = rowCount;

            return this;
        }

        public KronotopTable build() {
            if (fieldNames.isEmpty()) {
                throw new IllegalStateException("Table must have at least one field");
            }

            if (rowCount == 0L) {
                throw new IllegalStateException("Table must have positive row count");
            }

            return new KronotopTable(tableName, fieldNames, fieldTypes, new KronotopTableStatistic(rowCount));
        }
    }
}
