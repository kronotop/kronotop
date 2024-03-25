package com.kronotop.sql.optimizer.enumerable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.immutables.value.Value;

@Value.Enclosing
public class IndexScanMapPhysicalRule extends RelRule<RelRule.Config> {

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    private IndexScanMapPhysicalRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        System.out.println(call);
    }

    /**
     * Rule configuration.
     */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableIndexScanMapPhysicalRule.Config.builder()
                .description(IndexScanMapPhysicalRule.class.getSimpleName())
                .operandSupplier(b -> b.operand(LogicalTableScan.class).anyInputs()).build();

        @Override
        default IndexScanMapPhysicalRule toRule() {
            return new IndexScanMapPhysicalRule(this);
        }
    }

    static final RelOptRule INSTANCE = new IndexScanMapPhysicalRule(Config.DEFAULT);
}
