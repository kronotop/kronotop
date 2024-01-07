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

package com.kronotop.sql.optimizer;

import com.kronotop.sql.KronotopSchema;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * The Optimizer class is responsible for optimizing SQL queries using the Calcite framework.
 * It performs various steps such as parsing, validation, and conversion of SQL nodes to relational nodes.
 */
public class Optimizer {
    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final VolcanoPlanner planner;
    private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    private final Prepare.CatalogReader catalogReader;


    public Optimizer(KronotopSchema schema) {
        config = getCalciteConnectionConfig();
        catalogReader = getCalciteCatalogReader(schema);
        validator = getSqlValidator();
        planner = getVolcanoPlanner();
        converter = getSqlToRelConverter();
    }

    private SqlToRelConverter getSqlToRelConverter() {
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false); // https://issues.apache.org/jira/browse/CALCITE-1045
        return new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig
        );
    }

    private VolcanoPlanner getVolcanoPlanner() {
        VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.setTopDownOpt(true);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return planner;
    }

    private CalciteCatalogReader getCalciteCatalogReader(KronotopSchema schema) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(schema.getName(), schema);
        return new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(schema.getName()),
                typeFactory,
                config
        );
    }

    private SqlValidator getSqlValidator() {
        SqlOperatorTable operatorTable = new ChainedSqlOperatorTable(List.of(SqlStdOperatorTable.instance()));
        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);
        return SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);
    }

    private CalciteConnectionConfig getCalciteConnectionConfig() {
        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        return new CalciteConnectionConfigImpl(configProperties);
    }

    private SqlParser.Config getParserConfig() {
        SqlParser.Config parserConfig = SqlParser.config().
                withParserFactory(SqlDdlParserImpl.FACTORY);
        return SqlDialect.DatabaseProduct.POSTGRESQL.getDialect().configureParser(parserConfig);
    }

    public SqlNode validate(SqlNode node) {
        return validator.validate(node);
    }

    public RelNode convert(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);
        return root.rel;
    }

    public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        Program program = Programs.of(rules);
        return program.run(
                planner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList()
        );
    }
}
