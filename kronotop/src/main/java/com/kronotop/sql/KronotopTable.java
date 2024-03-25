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

import com.kronotop.sql.ddl.model.ColumnModel;
import com.kronotop.sql.ddl.model.TableModel;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.InitializerContext;
import org.apache.calcite.sql2rel.InitializerExpressionFactory;
import org.apache.calcite.sql2rel.NullInitializerExpressionFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * The KronotopTable class represents a table in the Kronotop database.
 * It extends the AbstractTable class and implements the ScannableTable interface.
 */
public class KronotopTable extends AbstractTable {
    // ID column is computed and stored. You cannot insert into it.
    public static StoredColumn IDStoredColumn = new StoredColumn("id", SqlTypeName.VARCHAR, ColumnStrategy.STORED);
    private final byte[] prefix;
    private final TableModel tableModel;
    private final KronotopTableStatistic statistic;
    private final InitializerExpressionFactory initializerExpressionFactory;
    private final RexNode filter;
    private final List<RexNode> projects;
    private RelDataType rowType;

    public KronotopTable(
            @Nonnull TableModel model,
            @Nonnull byte[] prefix
    ) {
        this.prefix = prefix;
        this.tableModel = model;
        this.initializerExpressionFactory = new TableMetaInitializerExpressionFactory(model);
        this.statistic = new KronotopTableStatistic(0);
        this.filter = null;
        this.projects = null;
    }

    private KronotopTable(
            @Nonnull TableModel model,
            @Nonnull byte[] prefix,
            List<RexNode> projects,
            RelDataType rowType,
            RexNode filter
    ) {
        this.prefix = prefix;
        this.tableModel = model;
        this.initializerExpressionFactory = new TableMetaInitializerExpressionFactory(model);
        this.statistic = new KronotopTableStatistic(0);
        this.filter = filter;
        this.projects = projects;
        this.rowType = rowType;
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
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

    @Nullable
    public List<RexNode> getProjects() {
        return projects;
    }

    @Nullable
    public RexNode getFilter() {
        return filter;
    }

    public KronotopTable withFilter(RexNode filter) {
        return new KronotopTable(tableModel, prefix, projects, rowType, filter);
    }

    public KronotopTable withProjects(List<RexNode> projects, @Nullable RelDataType rowType) {
        return new KronotopTable(tableModel, prefix, projects, rowType, filter);
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
            // TODO: This method should be re-implemented.
            final RelDataTypeField relDataTypeField = table.getRowType().getFieldList().get(iColumn);
            final RexBuilder rexBuilder = context.getRexBuilder();
            final ColumnModel columnModel = tableModel.getColumnList().get(iColumn);

            if (columnModel.getName().equals(IDStoredColumn.name())) {
                return rexBuilder.makeLiteral("", relDataTypeField.getType(), false);
            }

            Object value = null;
            switch (relDataTypeField.getType().getSqlTypeName()) {
                case INTEGER -> value = Integer.parseInt(columnModel.getExpression());
                case SMALLINT ->  value = Short.parseShort(columnModel.getExpression());
                case TINYINT -> value = Byte.valueOf(columnModel.getExpression());
                case VARCHAR, CHAR -> value = columnModel.getExpression();
            }

            if (value != null) {
                return rexBuilder.makeLiteral(value, relDataTypeField.getType(), false);
            }

            return null;
        }
    }

    public record StoredColumn(String name, SqlTypeName sqlTypeName, ColumnStrategy columnStrategy) {
    }
}
