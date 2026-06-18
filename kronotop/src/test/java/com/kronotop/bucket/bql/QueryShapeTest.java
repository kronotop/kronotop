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

import com.kronotop.bucket.bql.ast.BqlExpr;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class QueryShapeTest {

    private BqlExpr parse(String query) {
        return BqlParser.parse(query);
    }

    @Test
    void shouldProduceSameShapeForDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"age\": {\"$gt\": 25}}"));
        long shape2 = QueryShape.compute(parse("{\"age\": {\"$gt\": 100}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForDifferentOperators() {
        long shapeGt = QueryShape.compute(parse("{\"age\": {\"$gt\": 25}}"));
        long shapeLt = QueryShape.compute(parse("{\"age\": {\"$lt\": 25}}"));
        assertNotEquals(shapeGt, shapeLt);
    }

    @Test
    void shouldProduceDifferentShapeForDifferentFields() {
        long shape1 = QueryShape.compute(parse("{\"age\": {\"$gt\": 25}}"));
        long shape2 = QueryShape.compute(parse("{\"score\": {\"$gt\": 25}}"));
        assertNotEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForDifferentRegexPatterns() {
        // Behavior: two $regex queries on the same field share one shape; pattern and options are
        // parameters, not part of the shape. This is what lets regex queries share a cached plan.
        long shape1 = QueryShape.compute(parse("{\"name\": {\"$regex\": \"^Al\"}}"));
        long shape2 = QueryShape.compute(parse("{\"name\": {\"$regex\": \"^Bo\", \"$options\": \"i\"}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForRegexVsEq() {
        // Behavior: a $regex query and an $eq query on the same field produce different shapes.
        long shapeRegex = QueryShape.compute(parse("{\"name\": {\"$regex\": \"Alice\"}}"));
        long shapeEq = QueryShape.compute(parse("{\"name\": {\"$eq\": \"Alice\"}}"));
        assertNotEquals(shapeRegex, shapeEq);
    }

    @Test
    void shouldProduceDifferentShapeForDifferentValueTypes() {
        long shapeInt = QueryShape.compute(parse("{\"value\": {\"$eq\": 100}}"));
        long shapeString = QueryShape.compute(parse("{\"value\": {\"$eq\": \"100\"}}"));
        assertNotEquals(shapeInt, shapeString);
    }

    @Test
    void shouldProduceSameShapeForAndWithDifferentFieldOrder() {
        long shape1 = QueryShape.compute(parse("{\"a\": 1, \"b\": 2}"));
        long shape2 = QueryShape.compute(parse("{\"b\": 2, \"a\": 1}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForExplicitAndWithDifferentOrder() {
        long shape1 = QueryShape.compute(parse("{\"$and\": [{\"a\": 1}, {\"b\": 2}]}"));
        long shape2 = QueryShape.compute(parse("{\"$and\": [{\"b\": 2}, {\"a\": 1}]}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForOrWithDifferentOrder() {
        long shape1 = QueryShape.compute(parse("{\"$or\": [{\"a\": 1}, {\"b\": 2}]}"));
        long shape2 = QueryShape.compute(parse("{\"$or\": [{\"b\": 2}, {\"a\": 1}]}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForAndVsOr() {
        long shapeAnd = QueryShape.compute(parse("{\"$and\": [{\"a\": 1}, {\"b\": 2}]}"));
        long shapeOr = QueryShape.compute(parse("{\"$or\": [{\"a\": 1}, {\"b\": 2}]}"));
        assertNotEquals(shapeAnd, shapeOr);
    }

    @Test
    void shouldProduceSameShapeForInWithDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"status\": {\"$in\": [1, 2, 3]}}"));
        long shape2 = QueryShape.compute(parse("{\"status\": {\"$in\": [4, 5, 6]}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForInWithDifferentArraySize() {
        long shape1 = QueryShape.compute(parse("{\"status\": {\"$in\": [1, 2]}}"));
        long shape2 = QueryShape.compute(parse("{\"status\": {\"$in\": [1, 2, 3]}}"));
        assertNotEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForInWithMixedTypes() {
        long shapeHomogeneous = QueryShape.compute(parse("{\"status\": {\"$in\": [1, 2, 3]}}"));
        long shapeMixed = QueryShape.compute(parse("{\"status\": {\"$in\": [1, \"two\", 3]}}"));
        assertNotEquals(shapeHomogeneous, shapeMixed);
    }

    @Test
    void shouldProduceSameShapeForInWithSameTypesInDifferentOrder() {
        long shape1 = QueryShape.compute(parse("{\"status\": {\"$in\": [1, \"active\", true]}}"));
        long shape2 = QueryShape.compute(parse("{\"status\": {\"$in\": [\"active\", true, 1]}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForNinWithDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"status\": {\"$nin\": [1, 2]}}"));
        long shape2 = QueryShape.compute(parse("{\"status\": {\"$nin\": [3, 4]}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForNinWithMixedTypes() {
        long shapeInt = QueryShape.compute(parse("{\"status\": {\"$nin\": [1, 2]}}"));
        long shapeMixed = QueryShape.compute(parse("{\"status\": {\"$nin\": [1, \"two\"]}}"));
        assertNotEquals(shapeInt, shapeMixed);
    }

    @Test
    void shouldProduceSameShapeForAllWithDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"tags\": {\"$all\": [\"a\", \"b\"]}}"));
        long shape2 = QueryShape.compute(parse("{\"tags\": {\"$all\": [\"x\", \"y\"]}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForAllWithMixedTypes() {
        long shapeString = QueryShape.compute(parse("{\"tags\": {\"$all\": [\"a\", \"b\"]}}"));
        long shapeMixed = QueryShape.compute(parse("{\"tags\": {\"$all\": [\"a\", 1]}}"));
        assertNotEquals(shapeString, shapeMixed);
    }

    @Test
    void shouldProduceSameShapeForNotWithDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"age\": {\"$not\": {\"$eq\": 25}}}"));
        long shape2 = QueryShape.compute(parse("{\"age\": {\"$not\": {\"$eq\": 50}}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForNotVsPlain() {
        long shapeNot = QueryShape.compute(parse("{\"age\": {\"$not\": {\"$eq\": 25}}}"));
        long shapePlain = QueryShape.compute(parse("{\"age\": {\"$eq\": 25}}"));
        assertNotEquals(shapeNot, shapePlain);
    }

    @Test
    void shouldProduceSameShapeForElemMatchWithDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"scores\": {\"$elemMatch\": {\"$gte\": 80}}}"));
        long shape2 = QueryShape.compute(parse("{\"scores\": {\"$elemMatch\": {\"$gte\": 90}}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForElemMatchOnDifferentFields() {
        long shape1 = QueryShape.compute(parse("{\"scores\": {\"$elemMatch\": {\"$gte\": 80}}}"));
        long shape2 = QueryShape.compute(parse("{\"grades\": {\"$elemMatch\": {\"$gte\": 80}}}"));
        assertNotEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForSizeWithDifferentValues() {
        long shape1 = QueryShape.compute(parse("{\"tags\": {\"$size\": 3}}"));
        long shape2 = QueryShape.compute(parse("{\"tags\": {\"$size\": 5}}"));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForExistsTrue() {
        long shapeTrue = QueryShape.compute(parse("{\"field\": {\"$exists\": true}}"));
        long shapeFalse = QueryShape.compute(parse("{\"field\": {\"$exists\": false}}"));
        assertNotEquals(shapeTrue, shapeFalse);
    }

    @Test
    void shouldProduceSameShapeForComplexQueryWithDifferentValues() {
        String query1 = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"status\": {\"$in\": [\"active\", \"pending\"]}}]}";
        String query2 = "{\"$and\": [{\"age\": {\"$gte\": 21}}, {\"status\": {\"$in\": [\"approved\", \"review\"]}}]}";
        long shape1 = QueryShape.compute(parse(query1));
        long shape2 = QueryShape.compute(parse(query2));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForNestedAndWithDifferentOrder() {
        String query1 = "{\"$and\": [{\"a\": 1}, {\"$and\": [{\"b\": 2}, {\"c\": 3}]}]}";
        String query2 = "{\"$and\": [{\"$and\": [{\"c\": 3}, {\"b\": 2}]}, {\"a\": 1}]}";
        long shape1 = QueryShape.compute(parse(query1));
        long shape2 = QueryShape.compute(parse(query2));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForObjectIdWithDifferentValues() {
        // Behavior: Two $eq queries on the same field with different ObjectId hex values produce the same shape.
        String hex1 = new ObjectId().toHexString();
        String hex2 = new ObjectId().toHexString();
        long shape1 = QueryShape.compute(parse(String.format("{\"_id\": {\"$eq\": \"%s\"}}", hex1)));
        long shape2 = QueryShape.compute(parse(String.format("{\"_id\": {\"$eq\": \"%s\"}}", hex2)));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceDifferentShapeForObjectIdVsString() {
        // Behavior: An ObjectId value and a string value on the same field/operator produce different shapes.
        String hex = new ObjectId().toHexString();
        long shapeObjectId = QueryShape.compute(parse(String.format("{\"_id\": {\"$eq\": \"%s\"}}", hex)));
        long shapeString = QueryShape.compute(parse("{\"_id\": {\"$eq\": \"not-an-objectid\"}}"));
        assertNotEquals(shapeObjectId, shapeString);
    }

    @Test
    void shouldProduceSameShapeForInWithObjectIdsWithDifferentValues() {
        // Behavior: Two $in queries with different ObjectId lists (same size) produce the same shape.
        ObjectId a1 = new ObjectId();
        ObjectId a2 = new ObjectId();
        ObjectId b1 = new ObjectId();
        ObjectId b2 = new ObjectId();
        long shape1 = QueryShape.compute(parse(String.format("{\"_id\": {\"$in\": [\"%s\", \"%s\"]}}", a1.toHexString(), a2.toHexString())));
        long shape2 = QueryShape.compute(parse(String.format("{\"_id\": {\"$in\": [\"%s\", \"%s\"]}}", b1.toHexString(), b2.toHexString())));
        assertEquals(shape1, shape2);
    }

    @Test
    void shouldProduceSameShapeForInWithObjectIdsInDifferentOrder() {
        // Behavior: $in with ObjectIds in different order produces the same shape.
        ObjectId id1 = new ObjectId();
        ObjectId id2 = new ObjectId();
        long shape1 = QueryShape.compute(parse(String.format("{\"_id\": {\"$in\": [\"%s\", \"%s\"]}}", id1.toHexString(), id2.toHexString())));
        long shape2 = QueryShape.compute(parse(String.format("{\"_id\": {\"$in\": [\"%s\", \"%s\"]}}", id2.toHexString(), id1.toHexString())));
        assertEquals(shape1, shape2);
    }
}
