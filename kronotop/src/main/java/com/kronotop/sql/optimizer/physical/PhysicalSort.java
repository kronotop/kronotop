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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Implementation of {@link Sort} in
 * {@link PhysicalConvention physical calling convention}.
 */
public class PhysicalSort extends Sort implements PhysicalRel {
    /**
     * Creates a PhysicalSort.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public PhysicalSort(RelOptCluster cluster, RelTraitSet traitSet,
                        RelNode input, RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert getConvention() == input.getConvention();
        assert fetch == null : "fetch must be null";
        assert offset == null : "offset must be null";
    }

    /**
     * Creates a PhysicalSort.
     */
    public static PhysicalSort create(RelNode child, RelCollation collation,
                                      @Nullable RexNode offset, @Nullable RexNode fetch) {
        final RelOptCluster cluster = child.getCluster();
        final RelTraitSet traitSet =
                cluster.traitSetOf(PhysicalConvention.INSTANCE)
                        .replace(collation);
        return new PhysicalSort(cluster, traitSet, child, collation, offset,
                fetch);
    }

    @Override
    public PhysicalSort copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelCollation newCollation,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        return new PhysicalSort(getCluster(), traitSet, newInput, newCollation,
                offset, fetch);
    }
}
