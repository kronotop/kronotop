package com.kronotop.sql.optimizer.physical;

public class PhysicalRules {

    public static final PhysicalTableModifyRule PHYSICAL_TABLE_MODIFY_RULE = PhysicalTableModifyRule.DEFAULT_CONFIG.toRule(PhysicalTableModifyRule.class);

    public static final PhysicalTableScanRule PHYSICAL_TABLE_SCAN_RULE = PhysicalTableScanRule.DEFAULT_CONFIG.toRule(PhysicalTableScanRule.class);

    public static final PhysicalValuesRule PHYSICAL_VALUES_RULE = PhysicalValuesRule.DEFAULT_CONFIG.toRule(PhysicalValuesRule.class);

    public static final PhysicalProjectRule PHYSICAL_PROJECT_RULE = PhysicalProjectRule.DEFAULT_CONFIG.toRule(PhysicalProjectRule.class);

    public static final PhysicalFilterRule PHYSICAL_FILTER_RULE = PhysicalFilterRule.DEFAULT_CONFIG.toRule(PhysicalFilterRule.class);

    public static final PhysicalSortRule PHYSICAL_SORT_RULE = PhysicalSortRule.DEFAULT_CONFIG.toRule(PhysicalSortRule.class);

    public static final PhysicalLimitRule PHYSICAL_LIMIT_RULE = PhysicalLimitRule.Config.DEFAULT.toRule();

    public static final PhysicalCalcRule PHYSICAL_CALC_RULE = PhysicalCalcRule.DEFAULT_CONFIG.toRule(PhysicalCalcRule.class);

}
