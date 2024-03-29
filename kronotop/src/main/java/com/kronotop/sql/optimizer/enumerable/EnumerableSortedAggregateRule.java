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
package com.kronotop.sql.optimizer.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableIntList;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule to convert a {@link LogicalAggregate} to an {@link EnumerableSortedAggregate}.
 * You may provide a custom config to convert other nodes that extend {@link Aggregate}.
 *
 * @see EnumerableRules#ENUMERABLE_SORTED_AGGREGATE_RULE
 */
class EnumerableSortedAggregateRule extends ConverterRule {
    /**
     * Default configuration.
     */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
            .withConversion(LogicalAggregate.class, Convention.NONE,
                    EnumerableConvention.INSTANCE, "EnumerableSortedAggregateRule")
            .withRuleFactory(EnumerableSortedAggregateRule::new);

    /**
     * Called from the Config.
     */
    protected EnumerableSortedAggregateRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        if (!Aggregate.isSimple(agg)) {
            return null;
        }
        final RelTraitSet inputTraits = rel.getCluster()
                .traitSet().replace(EnumerableConvention.INSTANCE)
                .replace(
                        RelCollations.of(
                                ImmutableIntList.copyOf(
                                        agg.getGroupSet().asList())));
        final RelTraitSet selfTraits =
                inputTraits.replace(
                        RelCollations.of(
                                ImmutableIntList.identity(agg.getGroupSet().cardinality())));
        return new EnumerableSortedAggregate(
                rel.getCluster(),
                selfTraits,
                convert(agg.getInput(), inputTraits),
                agg.getGroupSet(),
                agg.getGroupSets(),
                agg.getAggCallList());
    }
}
