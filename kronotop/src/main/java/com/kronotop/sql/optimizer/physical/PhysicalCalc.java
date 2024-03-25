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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexProgram;

/**
 * Implementation of {@link Calc} in
 * {@link PhysicalConvention calling convention}.
 */
public class PhysicalCalc extends Calc implements PhysicalRel {
    /**
     * Creates a PhysicalCalc.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public PhysicalCalc(RelOptCluster cluster,
                        RelTraitSet traitSet,
                        RelNode input,
                        RexProgram program) {
        super(cluster, traitSet, ImmutableList.of(), input, program);
        assert !program.containsAggs();
    }

    /**
     * Creates a PhysicalCalc.
     */
    public static PhysicalCalc create(final RelNode input,
                                      final RexProgram program) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSet()
                .replace(PhysicalConvention.INSTANCE)
                .replaceIfs(RelCollationTraitDef.INSTANCE,
                        () -> RelMdCollation.calc(mq, input, program))
                .replaceIf(RelDistributionTraitDef.INSTANCE,
                        () -> RelMdDistribution.calc(mq, input, program));
        return new PhysicalCalc(cluster, traitSet, input, program);
    }

    @Override
    public PhysicalCalc copy(RelTraitSet traitSet, RelNode child,
                             RexProgram program) {
        // we do not need to copy program; it is immutable
        return new PhysicalCalc(getCluster(), traitSet, child, program);
    }
}
