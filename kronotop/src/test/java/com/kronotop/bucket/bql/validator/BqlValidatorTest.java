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