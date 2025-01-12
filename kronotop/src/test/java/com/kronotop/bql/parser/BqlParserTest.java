package com.kronotop.bql.parser;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.comparison.BqlEqOperator;
import com.kronotop.bucket.bql.operators.comparison.BqlGtOperator;
import com.kronotop.bucket.bql.operators.comparison.BqlLtOperator;
import com.kronotop.bucket.bql.operators.comparison.BqlNinOperator;
import com.kronotop.bucket.bql.operators.logical.BqlNotOperator;
import com.kronotop.bucket.bql.parser.BqlParser;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class
BqlParserTest {
    @Test
    void test_parse() {
        //List<BqlOperator> result = BqlParser.parse("{ status: 'ALIVE', username: 'buraksezer', email: 'buraksezer@gmail.com', age: 36 }");
        //BqlParser.parse("{ status: {$eq: 'ALIVE'}, username: {$eq: 'buraksezer'} }");
        //List<BqlOperator> result = BqlParser.parse("{ tags: { $all: [ 'ssl' , 'security' ] } }");
        //List<BqlOperator> result = BqlParser.parse("{ quantity: { $nin: [ 5, 15 ] } }");

        Random rand = new Random();
        long total = 0;
        for (int i = 0; i < 100000; i++) {
            String query = String.format("{ $or: [ { status: {$eq: 'A' } }, { qty: { $lt: %d } } ], username: { $eq: 'buraksezer' }, tags: { $all: ['foo', 32]} }", rand.nextInt());
            long start = System.nanoTime();
            BqlParser.parse(query);
            long end = System.nanoTime();
            total += (end - start);
        }
        System.out.println(total / 100000);
        //for (BqlOperator operator : result) {
        //    System.out.println(operator);
        //};
    }

    @Test
    public void test_implicit_EQ() {
        BqlEqOperator eqOperator = new BqlEqOperator(1, "status");
        BqlValue<String> bqlValue = new BqlValue<>(BsonType.STRING);
        bqlValue.setValue("ALIVE");
        eqOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(eqOperator);

        List<BqlOperator> operators = BqlParser.parse("{ status: 'ALIVE' }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_explicit_EQ() {
        BqlEqOperator eqOperator = new BqlEqOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(20);
        eqOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "qty"),
                eqOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ qty: { $eq: 20 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_NOT() {
        BqlGtOperator gtOperator = new BqlGtOperator(3);
        BqlValue<Double> bqlValue = new BqlValue<>(BsonType.DOUBLE);
        bqlValue.setValue(1.99);
        gtOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "price"),
                new BqlNotOperator(2),
                gtOperator
        );

        List<BqlOperator> operators = BqlParser.parse("{ price: { $not: { $gt: 1.99 } } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_GT() {
        BqlGtOperator gtOperator = new BqlGtOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(20);
        gtOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                gtOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $gt: 20 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_LT() {
        BqlLtOperator ltOperator = new BqlLtOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(20);
        ltOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                ltOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $lt: 20 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_NIN() {
        BqlNinOperator ninOperator = new BqlNinOperator(2);
        for (int item : new int[]{5, 15}) {
            BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
            bqlValue.setValue(item);
            ninOperator.addValue(bqlValue);
        }
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                ninOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $nin: [ 5, 15 ] } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }
}