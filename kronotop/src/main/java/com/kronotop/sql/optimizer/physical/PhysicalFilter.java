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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Implementation of {@link Filter} in
 * {@link PhysicalConvention physical calling convention}.
 */
public class PhysicalFilter extends Filter implements PhysicalRel {
    /**
     * Creates a PhysicalFilter.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public PhysicalFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            RexNode condition) {
        super(cluster, traitSet, child, condition);
        //assert getConvention() instanceof PhysicalConvention;
    }

    /**
     * Creates a PhysicalFilter.
     */
    public static PhysicalFilter create(final RelNode input,
                                        RexNode condition) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(PhysicalConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.filter(mq, input))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.filter(mq, input));
        return new PhysicalFilter(cluster, traitSet, input, condition);
    }

    @Override
    public PhysicalFilter copy(RelTraitSet traitSet, RelNode input,
                               RexNode condition) {
        return new PhysicalFilter(getCluster(), traitSet, input, condition);
    }
}
