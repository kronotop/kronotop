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

import com.kronotop.sql.backend.ddl.model.ColumnModel;
import com.kronotop.sql.backend.ddl.model.TableModel;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class KronotopTable extends AbstractTable implements ScannableTable {
    private final byte[] prefix;
    private final TableModel tableModel;
    private final KronotopTableStatistic statistic;
    private final InitializerExpressionFactory initializerExpressionFactory;

    private RelDataType rowType;

    public KronotopTable(@Nonnull TableModel model, @Nonnull byte[] prefix) {
        this.prefix = prefix;
        this.tableModel = model;
        this.initializerExpressionFactory = new TableMetaInitializerExpressionFactory(model);
        // TODO: ??
        this.statistic = new KronotopTableStatistic(0);
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        System.out.println(aClass);
        if (aClass.isInstance(initializerExpressionFactory)) {
            return aClass.cast(initializerExpressionFactory);
        } else if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        return null;
    }

    public byte[] getPrefix() {
        return prefix;
    }

    public String getName() {
        return tableModel.getTable();
    }

    public String getSchema() {
        return tableModel.getSchema();
    }

    public TableModel getTableModel() {
        return tableModel;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            List<RelDataTypeField> fields = new ArrayList<>();
            int index = 0;
            for (ColumnModel columnModel : tableModel.getColumnList()) {
                RelDataType fieldType = typeFactory.createSqlType(columnModel.getDataType());
                if (columnModel.getStrategy() == ColumnStrategy.NULLABLE) {
                    fieldType = typeFactory.createTypeWithNullability(fieldType, true);
                }
                RelDataTypeField field = new RelDataTypeFieldImpl(columnModel.getName(), index, fieldType);
                fields.add(field);
                index++;
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

    private static class TableMetaInitializerExpressionFactory extends NullInitializerExpressionFactory {
        private final TableModel tableModel;

        public TableMetaInitializerExpressionFactory(TableModel tableModel) {
            this.tableModel = tableModel;
        }

        @Override
        public ColumnStrategy generationStrategy(RelOptTable table, int iColumn) {
            ColumnModel columnModel = tableModel.getColumnList().get(iColumn);
            return columnModel.getStrategy();
        }

        @Override
        public RexNode newColumnDefaultValue(RelOptTable table, int iColumn, InitializerContext context) {
            final RelDataTypeField relDataTypeField = table.getRowType().getFieldList().get(iColumn);
            final RexBuilder rexBuilder = context.getRexBuilder();
            final ColumnModel columnModel = tableModel.getColumnList().get(iColumn);

            Object value = null;
            switch (relDataTypeField.getType().getSqlTypeName()) {
                case INTEGER -> value = Integer.parseInt(columnModel.getExpression());
                case VARCHAR, CHAR -> value = columnModel.getExpression();
            }
            if (value != null) {
                return rexBuilder.makeLiteral(value, relDataTypeField.getType(), false);
            }
            return null;
        }
    }
}
