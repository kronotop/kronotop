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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Sort;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert an {@link Sort} to an
 * {@link PhysicalSort}.
 *
 * @see PhysicalRules#PHYSICAL_SORT_RULE
 */
class PhysicalSortRule extends ConverterRule {
    /**
     * Default configuration.
     */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(Sort.class, Convention.NONE,
                    PhysicalConvention.INSTANCE, "PhysicalSortRule")
            .withRuleFactory(PhysicalSortRule::new);

    /**
     * Called from the Config.
     */
    protected PhysicalSortRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final Sort sort = (Sort) rel;
        if (sort.offset != null || sort.fetch != null) {
            return null;
        }
        final RelNode input = sort.getInput();
        return PhysicalSort.create(
                convert(
                        input,
                        input.getTraitSet().replace(PhysicalConvention.INSTANCE)),
                sort.getCollation(),
                null,
                null);
    }
}
