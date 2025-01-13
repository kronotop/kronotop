// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.validator;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.IllegalArgumentSizeException;
import com.kronotop.bucket.bql.InvalidTypeException;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import org.bson.BsonType;

import java.util.List;

public class BqlValidator {
    private final List<BqlOperator> operators;

    private BqlValidator(List<BqlOperator> operators) {
        this.operators = operators;
    }

    public static void validate(List<BqlOperator> operators) {
        BqlValidator validator = new BqlValidator(operators);
        validator.validate();
    }

    @SuppressWarnings("unchecked")
    private void validateSizeOperator(BqlOperator operator) {
        if (operator.getValues().size() > 1) {
            throw new IllegalArgumentSizeException("$size operator requires only one value");
        }
        BqlValue<Integer> bqlValue = (BqlValue<Integer>) operator.getValues().getFirst();
        if (!bqlValue.getBsonType().equals(BsonType.INT32)) {
            throw new InvalidTypeException("Argument type must be INT32 for $size operator");
        }
    }

    private void validate() {
        for (BqlOperator operator : operators) {
            if (operator.getOperatorType().equals(OperatorType.SIZE)) {
                validateSizeOperator(operator);
            }
        }
    }
}
