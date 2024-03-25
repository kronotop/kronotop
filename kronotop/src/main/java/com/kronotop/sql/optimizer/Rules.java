/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.sql.optimizer;

import com.kronotop.sql.optimizer.physical.PhysicalRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class Rules {
    public static RuleSet rules = RuleSets.ofList(
            PhysicalRules.PHYSICAL_TABLE_SCAN_RULE,
            PhysicalRules.PHYSICAL_CALC_RULE,
            PhysicalRules.PHYSICAL_PROJECT_RULE,
            PhysicalRules.PHYSICAL_FILTER_RULE,
            PhysicalRules.PHYSICAL_SORT_RULE,
            PhysicalRules.PHYSICAL_LIMIT_RULE,
            PhysicalRules.PHYSICAL_TABLE_MODIFY_RULE,
            PhysicalRules.PHYSICAL_VALUES_RULE,
            CoreRules.PROJECT_TO_SEMI_JOIN,
            CoreRules.JOIN_ON_UNIQUE_TO_SEMI_JOIN,
            CoreRules.JOIN_TO_SEMI_JOIN,
            CoreRules.MATCH,
            CalciteSystemProperty.COMMUTE.value()
                    ? CoreRules.JOIN_ASSOCIATE
                    : CoreRules.PROJECT_MERGE,
            CoreRules.AGGREGATE_STAR_TABLE,
            CoreRules.AGGREGATE_PROJECT_STAR_TABLE,
            CoreRules.FILTER_SCAN,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES,
            CoreRules.AGGREGATE_REDUCE_FUNCTIONS,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.SORT_PROJECT_TRANSPOSE
    );
}
