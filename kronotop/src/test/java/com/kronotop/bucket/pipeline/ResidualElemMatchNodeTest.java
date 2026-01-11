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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.bql.ast.BooleanVal;
import com.kronotop.bucket.bql.ast.DoubleVal;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.Int64Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResidualElemMatchNodeTest {

    private static ByteBuffer toBuffer(BsonDocument doc) {
        return BSONUtil.toByteBuffer(doc);
    }

    @Nested
    @DisplayName("Basic ElemMatch with Documents")
    class BasicElemMatchWithDocuments {

        @Test
        void shouldMatchWhenElementSatisfiesCondition() {
            // Document: { items: [{price: 50}, {price: 150}] }
            // Query: { items: { $elemMatch: { price: { $gt: 100 } } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)),
                    new BsonDocument("price", new BsonInt32(150))
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldNotMatchWhenNoElementSatisfiesCondition() {
            // Document: { items: [{price: 50}, {price: 80}] }
            // Query: { items: { $elemMatch: { price: { $gt: 100 } } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)),
                    new BsonDocument("price", new BsonInt32(80))
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldMatchWithMultipleConditions() {
            // Document: { items: [{price: 50, category: "toys"}, {price: 150, category: "electronics"}] }
            // Query: { items: { $elemMatch: { price: { $gt: 100 }, category: "electronics" } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)).append("category", new BsonString("toys")),
                    new BsonDocument("price", new BsonInt32(150)).append("category", new BsonString("electronics"))
            )));

            ResidualAndNode subPredicate = new ResidualAndNode(List.of(
                    new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100)),
                    new ResidualPredicate(2, "category", Operator.EQ, new StringVal("electronics"))
            ));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldNotMatchWhenNoSingleElementSatisfiesAllConditions() {
            // Document: { items: [{price: 50, category: "electronics"}, {price: 150, category: "toys"}] }
            // Query: { items: { $elemMatch: { price: { $gt: 100 }, category: "electronics" } } }
            // Neither element satisfies both conditions
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)).append("category", new BsonString("electronics")),
                    new BsonDocument("price", new BsonInt32(150)).append("category", new BsonString("toys"))
            )));

            ResidualAndNode subPredicate = new ResidualAndNode(List.of(
                    new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100)),
                    new ResidualPredicate(2, "category", Operator.EQ, new StringVal("electronics"))
            ));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        void shouldReturnFalseForMissingField() {
            BsonDocument doc = new BsonDocument("other", new BsonString("value"));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldReturnFalseForNonArrayField() {
            BsonDocument doc = new BsonDocument("items", new BsonString("not an array"));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldReturnFalseForEmptyArray() {
            BsonDocument doc = new BsonDocument("items", new BsonArray());

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldReturnFalseForNullField() {
            BsonDocument doc = new BsonDocument("items", org.bson.BsonNull.VALUE);

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }
    }

    @Nested
    @DisplayName("Nested Document Access")
    class NestedDocumentAccess {

        @Test
        void shouldMatchNestedFields() {
            // Document: { orders: [{details: {amount: 100}}, {details: {amount: 200}}] }
            // Query: { orders: { $elemMatch: { "details.amount": { $gte: 150 } } } }
            BsonDocument doc = new BsonDocument("orders", new BsonArray(List.of(
                    new BsonDocument("details", new BsonDocument("amount", new BsonInt32(100))),
                    new BsonDocument("details", new BsonDocument("amount", new BsonInt32(200)))
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "details.amount", Operator.GTE, new Int32Val(150));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("orders", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }
    }

    @Nested
    @DisplayName("Comparison Operators")
    class ComparisonOperators {

        private BsonDocument createTestDoc() {
            return new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("value", new BsonInt32(10)),
                    new BsonDocument("value", new BsonInt32(50)),
                    new BsonDocument("value", new BsonInt32(100))
            )));
        }

        @Test
        void shouldMatchWithEqOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.EQ, new Int32Val(50));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toBuffer(createTestDoc())));
        }

        @Test
        void shouldMatchWithNeOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.NE, new Int32Val(999));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toBuffer(createTestDoc())));
        }

        @Test
        void shouldMatchWithLtOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.LT, new Int32Val(50));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toBuffer(createTestDoc())));
        }

        @Test
        void shouldMatchWithLteOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.LTE, new Int32Val(10));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toBuffer(createTestDoc())));
        }

        @Test
        void shouldMatchWithGteOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.GTE, new Int32Val(100));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toBuffer(createTestDoc())));
        }
    }

    @Nested
    @DisplayName("Scalar String Arrays")
    class ScalarStringArrays {

        @Test
        void shouldMatchStringWithEqOperator() {
            // Document: { tags: ["urgent", "bug", "feature"] }
            // Query: { tags: { $elemMatch: { $eq: "urgent" } } }
            BsonDocument doc = new BsonDocument("tags", new BsonArray(List.of(
                    new BsonString("urgent"),
                    new BsonString("bug"),
                    new BsonString("feature")
            )));

            // For scalar arrays, the selector is empty string "" because elements are wrapped
            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new StringVal("urgent"));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldNotMatchStringWhenNoElementMatches() {
            // Document: { tags: ["bug", "feature", "enhancement"] }
            // Query: { tags: { $elemMatch: { $eq: "urgent" } } }
            BsonDocument doc = new BsonDocument("tags", new BsonArray(List.of(
                    new BsonString("bug"),
                    new BsonString("feature"),
                    new BsonString("enhancement")
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new StringVal("urgent"));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldMatchStringWithNeOperator() {
            // Document: { tags: ["urgent", "bug"] }
            // Query: { tags: { $elemMatch: { $ne: "critical" } } }
            // At least one element is not "critical"
            BsonDocument doc = new BsonDocument("tags", new BsonArray(List.of(
                    new BsonString("urgent"),
                    new BsonString("bug")
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.NE, new StringVal("critical"));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldHandleEmptyStringArray() {
            BsonDocument doc = new BsonDocument("tags", new BsonArray());

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new StringVal("urgent"));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }
    }

    @Nested
    @DisplayName("Scalar Number Arrays")
    class ScalarNumberArrays {

        @Test
        void shouldMatchInt32WithGtOperator() {
            // Document: { scores: [75, 88, 92] }
            // Query: { scores: { $elemMatch: { $gt: 90 } } }
            BsonDocument doc = new BsonDocument("scores", new BsonArray(List.of(
                    new BsonInt32(75),
                    new BsonInt32(88),
                    new BsonInt32(92)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GT, new Int32Val(90));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldNotMatchInt32WhenNoElementSatisfiesCondition() {
            // Document: { scores: [75, 88, 89] }
            // Query: { scores: { $elemMatch: { $gt: 90 } } }
            BsonDocument doc = new BsonDocument("scores", new BsonArray(List.of(
                    new BsonInt32(75),
                    new BsonInt32(88),
                    new BsonInt32(89)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GT, new Int32Val(90));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldMatchInt32WithLteOperator() {
            // Document: { values: [10, 20, 30] }
            // Query: { values: { $elemMatch: { $lte: 15 } } }
            BsonDocument doc = new BsonDocument("values", new BsonArray(List.of(
                    new BsonInt32(10),
                    new BsonInt32(20),
                    new BsonInt32(30)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.LTE, new Int32Val(15));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("values", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldMatchInt32WithEqOperator() {
            // Document: { values: [1, 2, 3, 4, 5] }
            // Query: { values: { $elemMatch: { $eq: 3 } } }
            BsonDocument doc = new BsonDocument("values", new BsonArray(List.of(
                    new BsonInt32(1),
                    new BsonInt32(2),
                    new BsonInt32(3),
                    new BsonInt32(4),
                    new BsonInt32(5)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Int32Val(3));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("values", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldHandleEmptyNumberArray() {
            BsonDocument doc = new BsonDocument("scores", new BsonArray());

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GT, new Int32Val(50));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldMatchInt64WithGteOperator() {
            // Document: { bigNumbers: [1000000000000, 2000000000000] }
            // Query: { bigNumbers: { $elemMatch: { $gte: 1500000000000 } } }
            BsonDocument doc = new BsonDocument("bigNumbers", new BsonArray(List.of(
                    new BsonInt64(1000000000000L),
                    new BsonInt64(2000000000000L)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GTE, new Int64Val(1500000000000L));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("bigNumbers", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldMatchDoubleWithLtOperator() {
            // Document: { prices: [9.99, 19.99, 29.99] }
            // Query: { prices: { $elemMatch: { $lt: 15.0 } } }
            BsonDocument doc = new BsonDocument("prices", new BsonArray(List.of(
                    new BsonDouble(9.99),
                    new BsonDouble(19.99),
                    new BsonDouble(29.99)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.LT, new DoubleVal(15.0));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("prices", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }
    }

    @Nested
    @DisplayName("Scalar Boolean Arrays")
    class ScalarBooleanArrays {

        @Test
        void shouldMatchBooleanWithEqTrue() {
            // Document: { flags: [false, true, false] }
            // Query: { flags: { $elemMatch: { $eq: true } } }
            BsonDocument doc = new BsonDocument("flags", new BsonArray(List.of(
                    BsonBoolean.FALSE,
                    BsonBoolean.TRUE,
                    BsonBoolean.FALSE
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new BooleanVal(true));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("flags", subPredicate);

            assertTrue(elemMatch.test(toBuffer(doc)));
        }

        @Test
        void shouldNotMatchBooleanWhenNoTrueExists() {
            // Document: { flags: [false, false, false] }
            // Query: { flags: { $elemMatch: { $eq: true } } }
            BsonDocument doc = new BsonDocument("flags", new BsonArray(List.of(
                    BsonBoolean.FALSE,
                    BsonBoolean.FALSE,
                    BsonBoolean.FALSE
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new BooleanVal(true));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("flags", subPredicate);

            assertFalse(elemMatch.test(toBuffer(doc)));
        }
    }
}
