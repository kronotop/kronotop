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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Relational expression that applies a limit and/or offset to its input.
 */
public class PhysicalLimit extends SingleRel implements PhysicalRel {
    public final @Nullable RexNode offset;
    public final @Nullable RexNode fetch;

    /**
     * Creates an PhysicalLimit.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public PhysicalLimit(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        super(cluster, traitSet, input);
        this.offset = offset;
        this.fetch = fetch;
        //assert getConvention() instanceof PhysicalConvention;
        assert getConvention() == input.getConvention();
    }

    /**
     * Creates an PhysicalLimit.
     */
    public static PhysicalLimit create(final RelNode input, @Nullable RexNode offset,
                                       @Nullable RexNode fetch) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(PhysicalConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.limit(mq, input))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.limit(mq, input));
        return new PhysicalLimit(cluster, traitSet, input, offset, fetch);
    }

    static Expression getExpression(RexNode rexNode) {
        if (rexNode instanceof RexDynamicParam param) {
            return Expressions.convert_(
                    Expressions.call(DataContext.ROOT,
                            BuiltInMethod.DATA_CONTEXT_GET.method,
                            Expressions.constant("?" + param.getIndex())),
                    Integer.class);
        } else {
            return Expressions.constant(RexLiteral.intValue(rexNode));
        }
    }

    @Override
    public PhysicalLimit copy(
            RelTraitSet traitSet,
            List<RelNode> newInputs) {
        return new PhysicalLimit(
                getCluster(),
                traitSet,
                sole(newInputs),
                offset,
                fetch);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("offset", offset, offset != null)
                .itemIf("fetch", fetch, fetch != null);
    }
}
