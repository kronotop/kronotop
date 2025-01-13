package com.kronotop.bucket.bql.validator;

import com.kronotop.bucket.bql.IllegalArgumentSizeException;
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
}