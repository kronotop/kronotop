// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.validator;

import com.kronotop.bucket.bql.IllegalArgumentSizeException;
import com.kronotop.bucket.bql.IllegalFieldException;
import com.kronotop.bucket.bql.InvalidTypeException;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BqlValidatorTest {

    @Test
    void test_validate_SIZE_operator_when_argument_type_is_invalid() {
        List<BqlOperator> operators = BqlParser.parse("{ field: { $size: 'invalid-type' } }");

        Throwable throwable = assertThrows(InvalidTypeException.class, () -> BqlValidator.validate(operators));
        assertThat(throwable.getMessage()).isEqualTo("Argument type must be INT32 for $size operator");
    }

    @Test
    void test_validate_SIZE_operator_when_number_of_arguments_is_invalid() {
        List<BqlOperator> operators = BqlParser.parse("{ field: { $size: [2, 3] } }");

        Throwable throwable = assertThrows(IllegalArgumentSizeException.class, () -> BqlValidator.validate(operators));
        assertThat(throwable.getMessage()).isEqualTo("$size operator requires only one value");
    }

    @Test
    void should_validate_valid_id_field() {
        List<BqlOperator> operators = BqlParser.parse("{ _id: { $gte: \"00000U44O1LKI000000Gxxxx\" } }");
        BqlValidator.validate(operators);
    }

    @Test
    void should_throw_InvalidTypeException_for_invalid_id_value_type() {
        List<BqlOperator> operators = BqlParser.parse("{ _id: { $gte: 20 } }");
        Throwable throwable = assertThrows(InvalidTypeException.class, () -> BqlValidator.validate(operators));
        assertThat(throwable.getMessage()).isEqualTo("Argument type must be STRING for '_id' field before normalization");
    }

    @Test
    void should_throw_IllegalArgumentSizeException_for_invalid_id_value() {
        List<BqlOperator> operators = BqlParser.parse("{ _id: { $gte: [10, 20] } }");
        Throwable throwable = assertThrows(IllegalArgumentSizeException.class, () -> BqlValidator.validate(operators));
        assertThat(throwable.getMessage()).isEqualTo("_id field requires only one value");
    }

    @Test
    void should_throw_IllegalFieldException_for_invalid_id_field() {
        List<BqlOperator> operators = BqlParser.parse("{ _id: { $gte: \"some-string\" } }");
        Throwable throwable = assertThrows(IllegalFieldException.class, () -> BqlValidator.validate(operators));
        assertThat(throwable.getMessage()).isEqualTo("Size of '_id' field is invalid: 11");
    }
}