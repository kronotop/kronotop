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
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * Implementation of {@link Project} in
 * {@link PhysicalConvention physical calling convention}.
 */
public class PhysicalProject extends Project implements PhysicalRel {
    /**
     * Creates a PhysicalProject.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster  Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param input    Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType  Output row type
     */
    public PhysicalProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    }

    @Deprecated // to be removed before 2.0
    public PhysicalProject(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode input, List<? extends RexNode> projects, RelDataType rowType,
                           int flags) {
        this(cluster, traitSet, input, projects, rowType);
        Util.discard(flags);
    }

    /**
     * Creates a PhysicalProject, specifying a row type rather than field
     * names.
     */
    public static PhysicalProject create(final RelNode input,
                                         final List<? extends RexNode> projects, RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSet().replace(PhysicalConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.project(mq, input, projects));
        return new PhysicalProject(cluster, traitSet, input, projects, rowType);
    }

    @Override
    public PhysicalProject copy(RelTraitSet traitSet, RelNode input,
                                List<RexNode> projects, RelDataType rowType) {
        return new PhysicalProject(getCluster(), traitSet, input,
                projects, rowType);
    }
}
