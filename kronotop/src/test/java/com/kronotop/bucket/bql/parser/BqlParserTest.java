// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.bql.parser;

import com.kronotop.bucket.bql.BqlValue;
import com.kronotop.bucket.bql.operators.BqlOperator;
import com.kronotop.bucket.bql.operators.array.BqlElemMatchOperator;
import com.kronotop.bucket.bql.operators.array.BqlSizeOperator;
import com.kronotop.bucket.bql.operators.comparison.*;
import com.kronotop.bucket.bql.operators.element.BqlExistsOperator;
import com.kronotop.bucket.bql.operators.logical.BqlAndOperator;
import com.kronotop.bucket.bql.operators.logical.BqlNorOperator;
import com.kronotop.bucket.bql.operators.logical.BqlNotOperator;
import com.kronotop.bucket.bql.operators.logical.BqlOrOperator;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

class BqlParserTest {
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

    @Test
    public void test_GTE() {
        BqlGteOperator gteOperator = new BqlGteOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(20);
        gteOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                gteOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $gte: 20 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_LTE() {
        BqlLteOperator lteOperator = new BqlLteOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(20);
        lteOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                lteOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $lte: 20 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_NE() {
        BqlNeOperator neOperator = new BqlNeOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(20);
        neOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                neOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $ne: 20 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_IN() {
        BqlInOperator inOperator = new BqlInOperator(2);
        for (int item : new int[]{5, 15}) {
            BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
            bqlValue.setValue(item);
            inOperator.addValue(bqlValue);
        }
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "quantity"),
                inOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ quantity: { $in: [ 5, 15 ] } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_AND() {
        BqlEqOperator eqOperator_status = new BqlEqOperator(4);
        BqlValue<String> bqlValue_status = new BqlValue<>(BsonType.STRING);
        bqlValue_status.setValue("ALIVE");
        eqOperator_status.addValue(bqlValue_status);

        BqlEqOperator eqOperator_kronotop = new BqlEqOperator(4);
        BqlValue<String> bqlValue_kronotop = new BqlValue<>(BsonType.STRING);
        bqlValue_kronotop.setValue("kronotop");
        eqOperator_kronotop.addValue(bqlValue_kronotop);

        List<BqlOperator> expectedOperators = List.of(
                new BqlAndOperator(1),
                new BqlEqOperator(3, "status"),
                eqOperator_status,
                new BqlEqOperator(3, "username"),
                eqOperator_kronotop
        );
        List<BqlOperator> operators = BqlParser.parse("{ $and: [ { status: {$eq: 'ALIVE'}, username: {$eq: 'kronotop'} } ] }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_OR() {
        BqlLtOperator eqOperator_quantity = new BqlLtOperator(4);
        BqlValue<Integer> bqlValue_quantity = new BqlValue<>(BsonType.INT32);
        bqlValue_quantity.setValue(20);
        eqOperator_quantity.addValue(bqlValue_quantity);

        BqlEqOperator eqOperator_price = new BqlEqOperator(3, "price");
        BqlValue<Integer> bqlValue_price = new BqlValue<>(BsonType.INT32);
        bqlValue_price.setValue(10);
        eqOperator_price.addValue(bqlValue_price);

        List<BqlOperator> expectedOperators = List.of(
                new BqlOrOperator(1),
                new BqlEqOperator(3, "quantity"),
                eqOperator_quantity,
                eqOperator_price
        );
        List<BqlOperator> operators = BqlParser.parse("{ $or: [ { quantity: { $lt: 20 } }, { price: 10 } ] }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_NOR() {
        BqlLtOperator eqOperator_quantity = new BqlLtOperator(4);
        BqlValue<Integer> bqlValue_quantity = new BqlValue<>(BsonType.INT32);
        bqlValue_quantity.setValue(20);
        eqOperator_quantity.addValue(bqlValue_quantity);

        BqlEqOperator eqOperator_price = new BqlEqOperator(3, "price");
        BqlValue<Double> bqlValue_price = new BqlValue<>(BsonType.DOUBLE);
        bqlValue_price.setValue(1.99);
        eqOperator_price.addValue(bqlValue_price);

        BqlEqOperator eqOperator_sale = new BqlEqOperator(3, "sale");
        BqlValue<Boolean> bqlValue_sale = new BqlValue<>(BsonType.BOOLEAN);
        bqlValue_sale.setValue(true);
        eqOperator_sale.addValue(bqlValue_sale);

        List<BqlOperator> expectedOperators = List.of(
                new BqlNorOperator(1),
                eqOperator_price,
                new BqlEqOperator(3, "qty"),
                eqOperator_quantity,
                eqOperator_sale
        );
        List<BqlOperator> operators = BqlParser.parse("{ $nor: [ { price: 1.99 }, { qty: { $lt: 20 } }, { sale: true } ] }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_SIZE() {
        BqlSizeOperator sizeOperator = new BqlSizeOperator(2);
        BqlValue<Integer> bqlValue = new BqlValue<>(BsonType.INT32);
        bqlValue.setValue(2);
        sizeOperator.addValue(bqlValue);
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "field"),
                sizeOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ field: { $size: 2 } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_ELEMMATCH() {
        BqlGteOperator gteOperator = new BqlGteOperator(3);
        BqlValue<Integer> bqlValue_gte = new BqlValue<>(BsonType.INT32);
        bqlValue_gte.setValue(80);
        gteOperator.addValue(bqlValue_gte);

        BqlLtOperator ltOperator = new BqlLtOperator(3);
        BqlValue<Integer> bqlValue_lt = new BqlValue<>(BsonType.INT32);
        bqlValue_lt.setValue(85);
        ltOperator.addValue(bqlValue_lt);

        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "results"),
                new BqlElemMatchOperator(2),
                gteOperator,
                ltOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ results: { $elemMatch: { $gte: 80, $lt: 85 } } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_ELEMMATCH_mixed_types() {
        BqlEqOperator eqOperator_product = new BqlEqOperator(3, "product");
        BqlValue<String> bqlValue_product = new BqlValue<>(BsonType.STRING);
        bqlValue_product.setValue("xyz");
        eqOperator_product.addValue(bqlValue_product);

        BqlGteOperator gteOperator = new BqlGteOperator(4);
        BqlValue<Integer> bqlValue_gte = new BqlValue<>(BsonType.INT32);
        bqlValue_gte.setValue(8);
        gteOperator.addValue(bqlValue_gte);

        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "results"),
                new BqlElemMatchOperator(2),
                eqOperator_product,
                new BqlEqOperator(3, "score"),
                gteOperator
        );
        List<BqlOperator> operators = BqlParser.parse("{ results: { $elemMatch: { product: 'xyz', score: { $gte: 8 } } } }");
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }

    @Test
    public void test_exists() {
        BqlExistsOperator existsOperator = new BqlExistsOperator(2);
        BqlValue<Boolean> bqlValue = new BqlValue<>(BsonType.BOOLEAN);
        bqlValue.setValue(true);
        existsOperator.addValue(bqlValue);
        List<BqlOperator> operators = BqlParser.parse("{ field: { $exists: true } }");
        List<BqlOperator> expectedOperators = List.of(
                new BqlEqOperator(1, "field"),
                existsOperator
        );
        assertThat(operators).usingRecursiveComparison().isEqualTo(expectedOperators);
    }
}