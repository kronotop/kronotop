/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.bql.validator;

import com.kronotop.bucket.ReservedFieldName;
import com.kronotop.bucket.bql.IllegalArgumentSizeException;
import com.kronotop.bucket.bql.IllegalFieldException;
import com.kronotop.bucket.bql.InvalidTypeException;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.OperatorType;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.values.BqlValue;
import com.kronotop.internal.VersionstampUtil;
import org.bson.BsonType;

import java.util.List;
import java.util.Objects;

public class BqlValidator {
    private final List<BqlOperator> operators;

    private BqlValidator(List<BqlOperator> operators) {
        this.operators = operators;
    }

    public static void validate(List<BqlOperator> operators) {
        BqlValidator validator = new BqlValidator(operators);
        validator.validate();
    }

    /**
     * Validates the `$size` operator in a BQL operator. This method ensures that:
     * - The `$size` operator contains only one value.
     * - The value must be of type `INT32`.
     *
     * @param operator The `BqlOperator` instance to be validated. It is expected to have an operator type of `$size`.
     * @throws IllegalArgumentSizeException If the `$size` operator contains more than one value.
     * @throws InvalidTypeException         If the value of the `$size` operator is not of type `INT32`.
     */
    private void validateSizeOperator(BqlOperator operator) {
        if (operator.getValues().size() > 1) {
            throw new IllegalArgumentSizeException("$size operator requires only one value");
        }
        BqlValue<?> bqlValue = operator.getValues().getFirst();
        if (!Objects.equals(bqlValue.bsonType(), BsonType.INT32)) {
            throw new InvalidTypeException("Argument type must be INT32 for $size operator");
        }
    }

    /**
     * Validates the `_id` field in a BQL operator. This method ensures that the `_id` field
     * adheres to the following constraints:
     * - It contains only one value.
     * - The value must be of type `STRING`.
     * - The value must have a length matching the specified encoded versionstamp size.
     *
     * @param rootIndex The index of the root operator in the list of operators.
     * @param operator  The `BqlOperator` instance to be validated. It is expected to be of type `BqlEqOperator`.
     * @throws IllegalArgumentSizeException If the `_id` field contains more than one value.
     * @throws InvalidTypeException         If the value of the `_id` field is not of type `STRING`.
     * @throws IllegalFieldException        If the length of the `_id` field value is invalid.
     */
    private void validateIdField(int rootIndex, BqlOperator operator) {
        BqlEqOperator eqOperator = (BqlEqOperator) operator;
        if (Objects.nonNull(eqOperator.getField()) && eqOperator.getField().equals(ReservedFieldName.ID.getValue())) {
            BqlOperator idValue = operators.get(rootIndex + 1);
            if (idValue.getValues().size() > 1) {
                throw new IllegalArgumentSizeException("_id field requires only one value");
            }
            BqlValue<?> rawValue = idValue.getValues().getFirst();
            if (!Objects.equals(rawValue.bsonType(), BsonType.STRING)) {
                throw new InvalidTypeException(String.format(
                        "Argument type must be STRING for '%s' field before normalization", ReservedFieldName.ID.getValue()
                ));
            }
            String value = (String) rawValue.value();
            if (value.length() != VersionstampUtil.EncodedVersionstampSize) {
                throw new IllegalFieldException(String.format(
                        "Size of '%s' field is invalid: %d",
                        ReservedFieldName.ID.getValue(),
                        value.length()
                ));
            }
        }
    }

    private void validate() {
        for (int index = 0; index < operators.size(); index++) {
            BqlOperator operator = operators.get(index);
            if (operator.getOperatorType().equals(OperatorType.SIZE)) {
                validateSizeOperator(operator);
            } else if (operator.getOperatorType().equals(OperatorType.EQ)) {
                validateIdField(index, operator);
            }
        }
    }
}
