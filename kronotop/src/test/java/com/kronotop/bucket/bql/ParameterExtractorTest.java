/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.*;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParameterExtractorTest {

    private BqlExpr parse(String query) {
        return BqlParser.parse(query);
    }

    @Test
    void shouldExtractSingleValueFromEq() {
        // Behavior: $eq extracts its single value parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"age\": {\"$eq\": 25}}"));
        assertEquals(1, params.size());
        assertInstanceOf(Int32Val.class, params.get(0));
        assertEquals(25, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractSingleValueFromNe() {
        // Behavior: $ne extracts its single value parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"status\": {\"$ne\": \"inactive\"}}"));
        assertEquals(1, params.size());
        assertInstanceOf(StringVal.class, params.get(0));
        assertEquals("inactive", ((StringVal) params.get(0)).value());
    }

    @Test
    void shouldExtractSingleValueFromGt() {
        // Behavior: $gt extracts its single value parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"score\": {\"$gt\": 100}}"));
        assertEquals(1, params.size());
        assertInstanceOf(Int32Val.class, params.get(0));
        assertEquals(100, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractSingleValueFromGte() {
        // Behavior: $gte extracts its single value parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"score\": {\"$gte\": 50}}"));
        assertEquals(1, params.size());
        assertEquals(50, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractSingleValueFromLt() {
        // Behavior: $lt extracts its single value parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"price\": {\"$lt\": 99}}"));
        assertEquals(1, params.size());
        assertEquals(99, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractSingleValueFromLte() {
        // Behavior: $lte extracts its single value parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"quantity\": {\"$lte\": 10}}"));
        assertEquals(1, params.size());
        assertEquals(10, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractAllValuesFromInInOriginalOrder() {
        // Behavior: $in extracts all values in their original order
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"status\": {\"$in\": [1, 2, 3]}}"));
        assertEquals(3, params.size());
        assertEquals(1, ((Int32Val) params.get(0)).value());
        assertEquals(2, ((Int32Val) params.get(1)).value());
        assertEquals(3, ((Int32Val) params.get(2)).value());
    }

    @Test
    void shouldExtractAllValuesFromNinInOriginalOrder() {
        // Behavior: $nin extracts all values in their original order
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"code\": {\"$nin\": [\"a\", \"b\"]}}"));
        assertEquals(2, params.size());
        assertEquals("a", ((StringVal) params.get(0)).value());
        assertEquals("b", ((StringVal) params.get(1)).value());
    }

    @Test
    void shouldExtractAllValuesFromAllInOriginalOrder() {
        // Behavior: $all extracts all values in their original order
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"tags\": {\"$all\": [\"x\", \"y\", \"z\"]}}"));
        assertEquals(3, params.size());
        assertEquals("x", ((StringVal) params.get(0)).value());
        assertEquals("y", ((StringVal) params.get(1)).value());
        assertEquals("z", ((StringVal) params.get(2)).value());
    }

    @Test
    void shouldExtractSizeAsInt32Val() {
        // Behavior: $size extracts its int parameter wrapped as Int32Val
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"items\": {\"$size\": 5}}"));
        assertEquals(1, params.size());
        assertInstanceOf(Int32Val.class, params.get(0));
        assertEquals(5, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractNoParametersFromExists() {
        // Behavior: $exists has no parameters - boolean is part of the shape
        List<BqlValue> paramsTrue = ParameterExtractor.extract(parse("{\"field\": {\"$exists\": true}}"));
        assertTrue(paramsTrue.isEmpty());

        List<BqlValue> paramsFalse = ParameterExtractor.extract(parse("{\"field\": {\"$exists\": false}}"));
        assertTrue(paramsFalse.isEmpty());
    }

    @Test
    void shouldExtractParametersFromNestedNot() {
        // Behavior: $not recursively extracts parameters from its child
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"age\": {\"$not\": {\"$eq\": 30}}}"));
        assertEquals(1, params.size());
        assertEquals(30, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractParametersFromElemMatch() {
        // Behavior: $elemMatch recursively extracts parameters from its child
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"scores\": {\"$elemMatch\": {\"$gte\": 80}}}"));
        assertEquals(1, params.size());
        assertEquals(80, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldProduceSameParameterOrderForDifferentFieldOrdering() {
        // Behavior: Same-shape queries produce parameters in identical order regardless of field ordering
        List<BqlValue> params1 = ParameterExtractor.extract(parse("{\"a\": 1, \"b\": 2}"));
        List<BqlValue> params2 = ParameterExtractor.extract(parse("{\"b\": 20, \"a\": 10}"));

        // Canonical order: a, b
        assertEquals(2, params1.size());
        assertEquals(2, params2.size());

        assertEquals(1, ((Int32Val) params1.get(0)).value());  // a
        assertEquals(10, ((Int32Val) params2.get(0)).value()); // a

        assertEquals(2, ((Int32Val) params1.get(1)).value());  // b
        assertEquals(20, ((Int32Val) params2.get(1)).value()); // b
    }

    @Test
    void shouldProduceSameParameterOrderForExplicitAndWithDifferentOrder() {
        // Behavior: Explicit $and with reordered children produces the same parameter order
        List<BqlValue> params1 = ParameterExtractor.extract(parse("{\"$and\": [{\"a\": 1}, {\"b\": 2}]}"));
        List<BqlValue> params2 = ParameterExtractor.extract(parse("{\"$and\": [{\"b\": 20}, {\"a\": 10}]}"));

        // Canonical order: a, b
        assertEquals(2, params1.size());
        assertEquals(2, params2.size());

        assertEquals(1, ((Int32Val) params1.get(0)).value());  // a
        assertEquals(10, ((Int32Val) params2.get(0)).value()); // a

        assertEquals(2, ((Int32Val) params1.get(1)).value());  // b
        assertEquals(20, ((Int32Val) params2.get(1)).value()); // b
    }

    @Test
    void shouldProduceSameParameterOrderForOrWithDifferentOrder() {
        // Behavior: $or with reordered children produces the same parameter order
        List<BqlValue> params1 = ParameterExtractor.extract(parse("{\"$or\": [{\"a\": 1}, {\"b\": 2}]}"));
        List<BqlValue> params2 = ParameterExtractor.extract(parse("{\"$or\": [{\"b\": 20}, {\"a\": 10}]}"));

        // Canonical order: a, b
        assertEquals(2, params1.size());
        assertEquals(2, params2.size());

        assertEquals(1, ((Int32Val) params1.get(0)).value());  // a
        assertEquals(10, ((Int32Val) params2.get(0)).value()); // a

        assertEquals(2, ((Int32Val) params1.get(1)).value());  // b
        assertEquals(20, ((Int32Val) params2.get(1)).value()); // b
    }

    @Test
    void shouldExtractParametersFromNestedAndOr() {
        // Behavior: Nested AND/OR structures extract parameters in canonical order
        String query = "{\"$and\": [{\"a\": 1}, {\"$or\": [{\"b\": 2}, {\"c\": 3}]}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(3, params.size());
    }

    @Test
    void shouldProduceSameOrderForNestedAndWithDifferentOrder() {
        // Behavior: Nested $and with reordered children at all levels produces the same parameter order
        String query1 = "{\"$and\": [{\"a\": 1}, {\"$and\": [{\"b\": 2}, {\"c\": 3}]}]}";
        String query2 = "{\"$and\": [{\"$and\": [{\"c\": 30}, {\"b\": 20}]}, {\"a\": 10}]}";
        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: b, c, a
        assertEquals(3, params1.size());
        assertEquals(3, params2.size());

        assertEquals(2, ((Int32Val) params1.get(0)).value());  // b
        assertEquals(20, ((Int32Val) params2.get(0)).value()); // b

        assertEquals(3, ((Int32Val) params1.get(1)).value());  // c
        assertEquals(30, ((Int32Val) params2.get(1)).value()); // c

        assertEquals(1, ((Int32Val) params1.get(2)).value());  // a
        assertEquals(10, ((Int32Val) params2.get(2)).value()); // a
    }

    @Test
    void shouldExtractComplexQueryParameters() {
        // Behavior: Complex queries extract all parameters in canonical order
        String query = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"status\": {\"$in\": [\"active\", \"pending\"]}}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(3, params.size()); // 1 from $gte + 2 from $in
    }

    @Test
    void shouldProduceSameOrderForComplexQueryWithDifferentValues() {
        // Behavior: Same-shape complex queries with reordered children produce parameters in identical order
        String query1 = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"status\": {\"$in\": [\"active\", \"pending\"]}}]}";
        String query2 = "{\"$and\": [{\"status\": {\"$in\": [\"approved\", \"review\"]}}, {\"age\": {\"$gte\": 21}}]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: age, status[0], status[1]
        assertEquals(3, params1.size());
        assertEquals(3, params2.size());

        assertEquals(18, ((Int32Val) params1.get(0)).value());        // age
        assertEquals(21, ((Int32Val) params2.get(0)).value());        // age

        assertEquals("active", ((StringVal) params1.get(1)).value()); // status[0]
        assertEquals("approved", ((StringVal) params2.get(1)).value());

        assertEquals("pending", ((StringVal) params1.get(2)).value()); // status[1]
        assertEquals("review", ((StringVal) params2.get(2)).value());
    }

    @Test
    void shouldWorkWithExtractIntoForListReuse() {
        // Behavior: extractInto appends to an existing list, allowing list reuse
        List<BqlValue> reusableList = new ArrayList<>();

        ParameterExtractor.extractInto(parse("{\"a\": 1}"), reusableList);
        assertEquals(1, reusableList.size());

        reusableList.clear();
        ParameterExtractor.extractInto(parse("{\"b\": 2}"), reusableList);
        assertEquals(1, reusableList.size());
        assertEquals(2, ((Int32Val) reusableList.get(0)).value());
    }

    @Test
    void shouldHandleEmptyAndOr() {
        // Behavior: Empty $and/$or produces no parameters
        // Note: This tests the edge case where the children list is empty
        BqlAnd emptyAnd = new BqlAnd(List.of());
        List<BqlValue> params = ParameterExtractor.extract(emptyAnd);
        assertTrue(params.isEmpty());
    }

    @Test
    void shouldHandleMixedTypesInArrayOperators() {
        // Behavior: Mixed-type array operators extract all values in the original order
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": {\"$in\": [1, \"two\", true]}}"));
        assertEquals(3, params.size());
        assertInstanceOf(Int32Val.class, params.get(0));
        assertInstanceOf(StringVal.class, params.get(1));
        assertInstanceOf(BooleanVal.class, params.get(2));
    }

    // ===== Missing BqlValue Types =====

    @Test
    void shouldExtractInt64Val() {
        // Behavior: Large integer values are extracted as Int64Val
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"count\": 9223372036854775807}"));
        assertEquals(1, params.size());
        assertInstanceOf(Int64Val.class, params.get(0));
        assertEquals(9223372036854775807L, ((Int64Val) params.get(0)).value());
    }

    @Test
    void shouldExtractDoubleVal() {
        // Behavior: Floating-point values are extracted as DoubleVal
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"price\": 19.99}"));
        assertEquals(1, params.size());
        assertInstanceOf(DoubleVal.class, params.get(0));
        assertEquals(19.99, ((DoubleVal) params.get(0)).value(), 0.001);
    }

    @Test
    void shouldExtractNullVal() {
        // Behavior: Null values are extracted as NullVal
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": null}"));
        assertEquals(1, params.size());
        assertInstanceOf(NullVal.class, params.get(0));
    }

    @Test
    void shouldExtractBooleanValFalse() {
        // Behavior: Boolean false is extracted as BooleanVal
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"active\": false}"));
        assertEquals(1, params.size());
        assertInstanceOf(BooleanVal.class, params.get(0));
        assertFalse(((BooleanVal) params.get(0)).value());
    }

    @Test
    void shouldExtractBooleanValTrue() {
        // Behavior: Boolean true is extracted as BooleanVal
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"active\": true}"));
        assertEquals(1, params.size());
        assertInstanceOf(BooleanVal.class, params.get(0));
        assertTrue(((BooleanVal) params.get(0)).value());
    }

    // ===== Deep Nesting (3+ levels) =====

    @Test
    void shouldExtractFromThreeLevelNesting_AndOrAnd() {
        // Behavior: Three-level nesting (AND -> OR -> AND) extracts all parameters
        String query = "{\"$and\": [{\"a\": 1}, {\"$or\": [{\"b\": 2}, {\"$and\": [{\"c\": 3}, {\"d\": 4}]}]}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(4, params.size());
    }

    @Test
    void shouldExtractFromAlternatingNesting_OrAndOrLeaf() {
        // Behavior: Alternating nesting (OR -> AND -> OR -> leaf) extracts all parameters
        String query = "{\"$or\": [{\"$and\": [{\"$or\": [{\"x\": 1}, {\"y\": 2}]}, {\"z\": 3}]}, {\"w\": 4}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(4, params.size());
    }

    @Test
    void shouldProduceSameOrderForDeepNestedWithDifferentOrder() {
        // Behavior: Deep nested queries with reordered children produce the same parameter order
        // Using different values (x10) to verify ORDER is canonical
        String query1 = "{\"$and\": [{\"a\": 1}, {\"$or\": [{\"b\": 2}, {\"$and\": [{\"c\": 3}, {\"d\": 4}]}]}]}";
        String query2 = "{\"$and\": [{\"$or\": [{\"$and\": [{\"d\": 40}, {\"c\": 30}]}, {\"b\": 20}]}, {\"a\": 10}]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: b, d, c, a
        assertEquals(4, params1.size());
        assertEquals(4, params2.size());

        assertEquals(2, ((Int32Val) params1.get(0)).value());  // b
        assertEquals(20, ((Int32Val) params2.get(0)).value()); // b

        assertEquals(4, ((Int32Val) params1.get(1)).value());  // d
        assertEquals(40, ((Int32Val) params2.get(1)).value()); // d

        assertEquals(3, ((Int32Val) params1.get(2)).value());  // c
        assertEquals(30, ((Int32Val) params2.get(2)).value()); // c

        assertEquals(1, ((Int32Val) params1.get(3)).value());  // a
        assertEquals(10, ((Int32Val) params2.get(3)).value()); // a
    }

    // ===== $elemMatch with Complex Inner Queries =====

    @Test
    void shouldExtractFromElemMatchWithNestedAnd() {
        // Behavior: $elemMatch with nested $and extracts all parameters
        String query = "{\"scores\": {\"$elemMatch\": {\"$and\": [{\"$gte\": 80}, {\"$lte\": 100}]}}}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(2, params.size());
    }

    @Test
    void shouldExtractFromElemMatchWithNestedOr() {
        // Behavior: $elemMatch with nested $or extracts all parameters
        String query = "{\"scores\": {\"$elemMatch\": {\"$or\": [{\"$lt\": 50}, {\"$gt\": 90}]}}}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(2, params.size());
    }

    @Test
    void shouldExtractFromElemMatchInsideNot() {
        // Behavior: $elemMatch inside $not extracts parameters from inner query
        String query = "{\"scores\": {\"$not\": {\"$elemMatch\": {\"$gte\": 80}}}}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(1, params.size());
        assertEquals(80, ((Int32Val) params.get(0)).value());
    }

    // ===== $not with Array Operators =====

    @Test
    void shouldExtractFromNotWithIn() {
        // Behavior: $not with $in extracts all values from inner $in
        String query = "{\"status\": {\"$not\": {\"$in\": [\"deleted\", \"archived\"]}}}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(2, params.size());
        assertEquals("deleted", ((StringVal) params.get(0)).value());
        assertEquals("archived", ((StringVal) params.get(1)).value());
    }

    @Test
    void shouldExtractFromNotWithNin() {
        // Behavior: $not with $nin extracts all values from inner $nin
        String query = "{\"tags\": {\"$not\": {\"$nin\": [\"important\", \"urgent\"]}}}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(2, params.size());
        assertEquals("important", ((StringVal) params.get(0)).value());
        assertEquals("urgent", ((StringVal) params.get(1)).value());
    }

    // ===== Range Queries =====

    @Test
    void shouldExtractFromExplicitRangeQuery() {
        // Behavior: Explicit range using $and extracts both boundary values
        String query = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"age\": {\"$lte\": 65}}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(2, params.size());
    }

    @Test
    void shouldProduceSameOrderForRangeQueryWithDifferentOrder() {
        // Behavior: Range queries with reordered boundaries produce the same parameter order
        String query1 = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"age\": {\"$lte\": 65}}]}";
        String query2 = "{\"$and\": [{\"age\": {\"$lte\": 650}}, {\"age\": {\"$gte\": 180}}]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: $gte, $lte
        assertEquals(2, params1.size());
        assertEquals(2, params2.size());

        assertEquals(18, ((Int32Val) params1.get(0)).value());  // $gte
        assertEquals(180, ((Int32Val) params2.get(0)).value()); // $gte

        assertEquals(65, ((Int32Val) params1.get(1)).value());  // $lte
        assertEquals(650, ((Int32Val) params2.get(1)).value()); // $lte
    }

    // ===== Many Children in AND/OR (5+ children) =====

    @Test
    void shouldExtractFromAndWithFiveChildren() {
        // Behavior: $and with 5 children extracts all parameters
        String query = "{\"$and\": [{\"a\": 1}, {\"b\": 2}, {\"c\": 3}, {\"d\": 4}, {\"e\": 5}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(5, params.size());
    }

    @Test
    void shouldExtractFromOrWithFiveChildren() {
        // Behavior: $or with 5 children extracts all parameters
        String query = "{\"$or\": [{\"a\": 1}, {\"b\": 2}, {\"c\": 3}, {\"d\": 4}, {\"e\": 5}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(5, params.size());
    }

    @Test
    void shouldProduceSameOrderForFiveChildrenWithDifferentOrder() {
        // Behavior: $and with 5 reordered children produces the same parameter order
        String query1 = "{\"$and\": [{\"a\": 1}, {\"b\": 2}, {\"c\": 3}, {\"d\": 4}, {\"e\": 5}]}";
        String query2 = "{\"$and\": [{\"e\": 50}, {\"d\": 40}, {\"c\": 30}, {\"b\": 20}, {\"a\": 10}]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: d, e, a, b, c
        assertEquals(5, params1.size());
        assertEquals(5, params2.size());

        assertEquals(4, ((Int32Val) params1.get(0)).value());  // d
        assertEquals(40, ((Int32Val) params2.get(0)).value()); // d

        assertEquals(5, ((Int32Val) params1.get(1)).value());  // e
        assertEquals(50, ((Int32Val) params2.get(1)).value()); // e

        assertEquals(1, ((Int32Val) params1.get(2)).value());  // a
        assertEquals(10, ((Int32Val) params2.get(2)).value()); // a

        assertEquals(2, ((Int32Val) params1.get(3)).value());  // b
        assertEquals(20, ((Int32Val) params2.get(3)).value()); // b

        assertEquals(3, ((Int32Val) params1.get(4)).value());  // c
        assertEquals(30, ((Int32Val) params2.get(4)).value()); // c
    }

    // ===== Array Operator Edge Cases =====

    @Test
    void shouldExtractFromEmptyInArray() {
        // Behavior: Empty $in array produces no parameters
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": {\"$in\": []}}"));
        assertEquals(0, params.size());
    }

    @Test
    void shouldExtractFromSingleElementIn() {
        // Behavior: $in with a single element extracts one parameter
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": {\"$in\": [42]}}"));
        assertEquals(1, params.size());
        assertEquals(42, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractFromLargeInArray() {
        // Behavior: $in with 10 elements extracts all 10 parameters
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": {\"$in\": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}"));
        assertEquals(10, params.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(i + 1, ((Int32Val) params.get(i)).value());
        }
    }

    @Test
    void shouldExtractFromEmptyNinArray() {
        // Behavior: Empty $nin an array produces no parameters
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": {\"$nin\": []}}"));
        assertEquals(0, params.size());
    }

    @Test
    void shouldExtractFromEmptyAllArray() {
        // Behavior: Empty $all array produces no parameters
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"tags\": {\"$all\": []}}"));
        assertEquals(0, params.size());
    }

    // ===== $size Edge Cases =====

    @Test
    void shouldExtractSizeZero() {
        // Behavior: $size: 0 extracts Int32Val(0)
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"items\": {\"$size\": 0}}"));
        assertEquals(1, params.size());
        assertInstanceOf(Int32Val.class, params.get(0));
        assertEquals(0, ((Int32Val) params.get(0)).value());
    }

    @Test
    void shouldExtractLargeSize() {
        // Behavior: Large $size value extracts correctly
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"items\": {\"$size\": 1000000}}"));
        assertEquals(1, params.size());
        assertEquals(1000000, ((Int32Val) params.get(0)).value());
    }

    // ===== Complex Real-World Pattern =====

    @Test
    void shouldExtractFromEcommerceStyleQuery() {
        // Behavior: Complex e-commerce query extracts all parameters in canonical order
        String query = "{\"$and\": [" +
                "{\"category\": \"electronics\"}," +
                "{\"price\": {\"$gte\": 100}}," +
                "{\"price\": {\"$lte\": 500}}," +
                "{\"$or\": [{\"brand\": \"Apple\"}, {\"brand\": \"Samsung\"}]}," +
                "{\"inStock\": true}" +
                "]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(6, params.size()); // category, price gte, price lte, 2 brands, inStock
    }

    @Test
    void shouldProduceSameOrderForEcommerceQueryWithDifferentOrder() {
        // Behavior: Same-shape e-commerce queries produce identical parameter order
        // Note: $or children use different field names (brand/manufacturer) so they have distinct shapes
        String query1 = "{\"$and\": [" +
                "{\"category\": \"electronics\"}," +
                "{\"price\": {\"$gte\": 100}}," +
                "{\"price\": {\"$lte\": 500}}," +
                "{\"$or\": [{\"brand\": \"Apple\"}, {\"manufacturer\": \"Samsung\"}]}," +
                "{\"inStock\": true}" +
                "]}";
        String query2 = "{\"$and\": [" +
                "{\"inStock\": false}," +
                "{\"$or\": [{\"manufacturer\": \"LG\"}, {\"brand\": \"Sony\"}]}," +
                "{\"price\": {\"$lte\": 5000}}," +
                "{\"price\": {\"$gte\": 1000}}," +
                "{\"category\": \"computers\"}" +
                "]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: category, price $gte, price $lte, inStock, manufacturer, brand
        assertEquals(6, params1.size());
        assertEquals(6, params2.size());

        // Position 0: category
        assertEquals("electronics", ((StringVal) params1.get(0)).value());
        assertEquals("computers", ((StringVal) params2.get(0)).value());

        // Position 1: price $gte
        assertEquals(100, ((Int32Val) params1.get(1)).value());
        assertEquals(1000, ((Int32Val) params2.get(1)).value());

        // Position 2: price $lte
        assertEquals(500, ((Int32Val) params1.get(2)).value());
        assertEquals(5000, ((Int32Val) params2.get(2)).value());

        // Position 3: inStock
        assertTrue(((BooleanVal) params1.get(3)).value());
        assertFalse(((BooleanVal) params2.get(3)).value());

        // Position 4: manufacturer
        assertEquals("Samsung", ((StringVal) params1.get(4)).value());
        assertEquals("LG", ((StringVal) params2.get(4)).value());

        // Position 5: brand
        assertEquals("Apple", ((StringVal) params1.get(5)).value());
        assertEquals("Sony", ((StringVal) params2.get(5)).value());
    }

    // ===== Hash Collision Robustness =====

    @Test
    void shouldProduceDeterministicOrderForSimilarStructures() {
        // Behavior: Queries with the same structure but different fields produce deterministic ordering
        // Using different values (x10) to verify ORDER is canonical
        String query1 = "{\"$and\": [{\"aaa\": 1}, {\"bbb\": 2}, {\"ccc\": 3}]}";
        String query2 = "{\"$and\": [{\"ccc\": 30}, {\"bbb\": 20}, {\"aaa\": 10}]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Canonical order: ccc, bbb, aaa
        assertEquals(3, params1.size());
        assertEquals(3, params2.size());

        assertEquals(3, ((Int32Val) params1.get(0)).value());  // ccc
        assertEquals(30, ((Int32Val) params2.get(0)).value()); // ccc

        assertEquals(2, ((Int32Val) params1.get(1)).value());  // bbb
        assertEquals(20, ((Int32Val) params2.get(1)).value()); // bbb

        assertEquals(1, ((Int32Val) params1.get(2)).value());  // aaa
        assertEquals(10, ((Int32Val) params2.get(2)).value()); // aaa
    }

    @Test
    void shouldHandleMultipleSameFieldInOr() {
        // Behavior: Multiple conditions on the same field in $or extract all parameters
        String query = "{\"$or\": [{\"status\": \"active\"}, {\"status\": \"pending\"}, {\"status\": \"review\"}]}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(3, params.size());
    }

    // ===== Combined Edge Cases =====

    @Test
    void shouldExtractFromNotWithExists() {
        // Behavior: $not with $exists produces no parameters (exists boolean is part of shape)
        List<BqlValue> params = ParameterExtractor.extract(parse("{\"field\": {\"$not\": {\"$exists\": true}}}"));
        assertEquals(0, params.size());
    }

    @Test
    void shouldExtractFromDoubleNegation() {
        // Behavior: Double $not extracts parameters from innermost operator
        String query = "{\"age\": {\"$not\": {\"$not\": {\"$eq\": 25}}}}";
        List<BqlValue> params = ParameterExtractor.extract(parse(query));
        assertEquals(1, params.size());
        assertEquals(25, ((Int32Val) params.get(0)).value());
    }

    // ===== Identical-Shape Siblings Edge Case =====

    @Test
    void shouldPreserveInsertionOrderForIdenticalShapeSiblings() {
        // Behavior: When $or/$and children have identical shapes (same field, operator, value type),
        // their relative order is preserved from the original query. This is correct because
        // $or and $and are commutative - swapping identical-shape siblings produces semantically
        // equivalent results.
        String query1 = "{\"$or\": [{\"brand\": \"Apple\"}, {\"brand\": \"Samsung\"}]}";
        String query2 = "{\"$or\": [{\"brand\": \"Samsung\"}, {\"brand\": \"Apple\"}]}";

        // Both queries have the same shape (suitable for plan cache sharing)
        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        // Parameters preserve original insertion order (not canonicalized)
        assertEquals(2, params1.size());
        assertEquals("Apple", ((StringVal) params1.get(0)).value());
        assertEquals("Samsung", ((StringVal) params1.get(1)).value());

        assertEquals(2, params2.size());
        assertEquals("Samsung", ((StringVal) params2.get(0)).value());
        assertEquals("Apple", ((StringVal) params2.get(1)).value());
    }

    @Test
    void shouldPreserveInsertionOrderForIdenticalShapeSiblingsInAnd() {
        // Behavior: Same as $or - identical-shape siblings in $and preserve insertion order.
        // This is safe because $and is commutative.
        String query1 = "{\"$and\": [{\"status\": \"active\"}, {\"status\": \"verified\"}]}";
        String query2 = "{\"$and\": [{\"status\": \"verified\"}, {\"status\": \"active\"}]}";

        assertEquals(QueryShape.compute(parse(query1)), QueryShape.compute(parse(query2)));

        List<BqlValue> params1 = ParameterExtractor.extract(parse(query1));
        List<BqlValue> params2 = ParameterExtractor.extract(parse(query2));

        assertEquals("active", ((StringVal) params1.get(0)).value());
        assertEquals("verified", ((StringVal) params1.get(1)).value());

        assertEquals("verified", ((StringVal) params2.get(0)).value());
        assertEquals("active", ((StringVal) params2.get(1)).value());
    }
}
