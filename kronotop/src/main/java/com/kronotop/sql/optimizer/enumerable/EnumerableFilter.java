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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Implementation of {@link Filter} in
 * {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableFilter
        extends Filter
        implements EnumerableRel {
    /**
     * Creates an EnumerableFilter.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public EnumerableFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            RexNode condition) {
        super(cluster, traitSet, child, condition);
        assert getConvention() instanceof EnumerableConvention;
    }

    /**
     * Creates an EnumerableFilter.
     */
    public static EnumerableFilter create(final RelNode input,
                                          RexNode condition) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(EnumerableConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.filter(mq, input))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.filter(mq, input));
        return new EnumerableFilter(cluster, traitSet, input, condition);
    }

    @Override
    public EnumerableFilter copy(RelTraitSet traitSet, RelNode input,
                                 RexNode condition) {
        return new EnumerableFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        // EnumerableCalc is always better
        throw new UnsupportedOperationException();
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
            RelTraitSet required) {
        RelCollation collation = required.getCollation();
        if (collation == null || collation == RelCollations.EMPTY) {
            return null;
        }
        RelTraitSet traits = traitSet.replace(collation);
        return Pair.of(traits, ImmutableList.of(traits));
    }

    @Override
    public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
            final RelTraitSet childTraits, final int childId) {
        RelCollation collation = childTraits.getCollation();
        if (collation == null || collation == RelCollations.EMPTY) {
            return null;
        }
        RelTraitSet traits = traitSet.replace(collation);
        return Pair.of(traits, ImmutableList.of(traits));
    }
}
