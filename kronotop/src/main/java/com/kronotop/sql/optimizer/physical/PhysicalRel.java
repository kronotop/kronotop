package com.kronotop.sql.optimizer.physical;

import com.kronotop.sql.KronotopTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

public interface PhysicalRel extends RelNode {

    @Override
    default RelDataType getRowType() {
        return getTable().unwrap(KronotopTable.class).getRowType(getCluster().getTypeFactory());
    }
}
