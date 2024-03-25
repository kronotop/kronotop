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
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

/**
 * Implementation of {@link Values} in
 * {@link PhysicalConvention physical calling convention}.
 */
public class PhysicalValues extends Values implements PhysicalRel {
    /**
     * Creates an PhysicalValues.
     */
    private PhysicalValues(RelOptCluster cluster, RelDataType rowType,
                           ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
        super(cluster, rowType, tuples, traitSet);
    }

    /**
     * Creates an PhysicalValues.
     */
    public static PhysicalValues create(RelOptCluster cluster,
                                        final RelDataType rowType,
                                        final ImmutableList<ImmutableList<RexLiteral>> tuples) {
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(PhysicalConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.values(mq, rowType, tuples))
                        .replaceIf(RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.values(rowType, tuples));
        return new PhysicalValues(cluster, rowType, tuples, traitSet);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new PhysicalValues(getCluster(), getRowType(), tuples, traitSet);
    }
}
