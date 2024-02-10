package com.kronotop.sql.optimizer;

import com.kronotop.sql.optimizer.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class Rules {
    public static RuleSet rules = RuleSets.ofList(
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CORRELATE_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE,
            EnumerableRules.ENUMERABLE_MERGE_UNION_RULE,
            EnumerableRules.ENUMERABLE_INTERSECT_RULE,
            EnumerableRules.ENUMERABLE_MINUS_RULE,
            EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_WINDOW_RULE,
            EnumerableRules.ENUMERABLE_MATCH_RULE,
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
            CoreRules.JOIN_COMMUTE,
            JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT,
            CoreRules.SORT_PROJECT_TRANSPOSE
    );
}
