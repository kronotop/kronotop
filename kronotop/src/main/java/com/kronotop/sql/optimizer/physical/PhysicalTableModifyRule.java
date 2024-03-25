/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kronotop.sql.optimizer.physical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Planner rule that converts a {@link LogicalTableModify} to an {@link PhysicalTableModify}.
 * You may provide a custom config to convert other nodes that extend {@link TableModify}.
 *
 * @see PhysicalRules#PHYSICAL_TABLE_MODIFY_RULE
 */
public class PhysicalTableModifyRule extends ConverterRule {
    /**
     * Default configuration.
     */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalTableModify.class, Convention.NONE,
                    PhysicalConvention.INSTANCE, "PhysicalTableModifyRule")
            .withRuleFactory(PhysicalTableModifyRule::new);

    /**
     * Creates an PhysicalTableModifyRule.
     */
    protected PhysicalTableModifyRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final TableModify modify = (TableModify) rel;
        final RelTraitSet traitSet =
                modify.getTraitSet().replace(PhysicalConvention.INSTANCE);
        return new PhysicalTableModify(
                modify.getCluster(), traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getInput(), traitSet),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened());
    }
}
