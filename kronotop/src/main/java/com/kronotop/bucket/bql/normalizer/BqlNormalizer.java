// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.normalizer;

import com.kronotop.bucket.ReservedFieldName;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.values.BqlValue;
import com.kronotop.bucket.bql.values.StringVal;
import com.kronotop.bucket.bql.values.VersionstampVal;
import com.kronotop.internal.VersionstampUtils;

import java.util.List;
import java.util.Objects;

/**
 * The BqlNormalizer class processes and normalizes a collection of BqlOperator objects.
 */
public class BqlNormalizer {
    private final List<BqlOperator> operators;

    private BqlNormalizer(List<BqlOperator> operators) {
        this.operators = operators;
    }

    public static void normalize(List<BqlOperator> operators) {
        BqlNormalizer normalizer = new BqlNormalizer(operators);
        normalizer.normalize();
    }

    /**
     * Converts the value of the "_id" field within a given operator from a Base32Hex encoded string
     * to a Versionstamp object. This normalization is only applied if the operator type is equality
     * and the field corresponds to the reserved "_id" field name.
     *
     * @param rootIndex The index of the root operator in the list of operators.
     * @param operator  The BqlOperator to process for normalization. The function checks if this
     *                  operator defines an equality condition on the "_id" field.
     */
    @SuppressWarnings("unchecked")
    private void normalizeIdFieldValue(int rootIndex, BqlOperator operator) {
        if (operator.getOperatorType().equals(OperatorType.EQ)) {
            BqlEqOperator eqOperator = (BqlEqOperator) operator;
            if (Objects.nonNull(eqOperator.getField()) && eqOperator.getField().equals(ReservedFieldName.ID.getValue())) {
                BqlOperator idValue = operators.get(rootIndex + 1);
                for (int childIndex = 0; childIndex < idValue.getValues().size(); childIndex++) {
                    BqlValue<?> rawValue = idValue.getValues().get(childIndex);
                    StringVal stringVal = (StringVal) rawValue;
                    VersionstampVal versionstampVal = new VersionstampVal(VersionstampUtils.base32HexDecode(stringVal.value()));
                    idValue.getValues().set(childIndex, versionstampVal);
                }
            }
        }
    }

    private void normalize() {
        for (int index = 0; index < operators.size(); index++) {
            BqlOperator operator = operators.get(index);
            normalizeIdFieldValue(index, operator);
        }
    }
}
