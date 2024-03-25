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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;

/**
 * Implementation of {@link TableScan} in
 * {@link PhysicalConvention physical calling convention}.
 */
public class PhysicalTableScan extends TableScan implements PhysicalRel {
    //private final Class elementType;

    /**
     * Creates an PhysicalTableScan.
     *
     * <p>Use {@link #create} unless you know what you are doing.
     */
    public PhysicalTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                             RelOptTable table, Class elementType) {
        super(cluster, traitSet, ImmutableList.of(), table);
        //assert getConvention() instanceof PhysicalConvention;
        //this.elementType = elementType;
    }

    /**
     * Creates an PhysicalTableScan.
     */
    public static PhysicalTableScan create(RelOptCluster cluster,
                                           RelOptTable relOptTable) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
                cluster.traitSetOf(PhysicalConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                            if (table != null) {
                                return table.getStatistic().getCollations();
                            }
                            return ImmutableList.of();
                        });
        return new PhysicalTableScan(cluster, traitSet, relOptTable, Object[].class);
    }
}
