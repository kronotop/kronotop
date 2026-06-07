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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResidualElemMatchNodeTest {

    private static ByteBuffer toBuffer(BsonDocument doc) {
        return BSONUtil.toByteBuffer(doc);
    }

    private static DocumentView toView(ByteBuffer buffer) {
        DocumentView view = new DocumentView();
        view.reset(null, buffer);
        return view;
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldNotMatchWhenNoElementSatisfiesCondition() {
            // Document: { items: [{price: 50}, {price: 80}] }
            // Query: { items: { $elemMatch: { price: { $gt: 100 } } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)),
                    new BsonDocument("price", new BsonInt32(80))
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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
                    new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100))),
                    new ResidualPredicate(2, "category", Operator.EQ, new Operand.Literal(new StringVal("electronics")))
            ));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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
                    new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100))),
                    new ResidualPredicate(2, "category", Operator.EQ, new Operand.Literal(new StringVal("electronics")))
            ));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCases {

        @Test
        void shouldReturnFalseForMissingField() {
            BsonDocument doc = new BsonDocument("other", new BsonString("value"));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForNonArrayField() {
            BsonDocument doc = new BsonDocument("items", new BsonString("not an array"));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForEmptyArray() {
            BsonDocument doc = new BsonDocument("items", new BsonArray());

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldReturnFalseForNullField() {
            BsonDocument doc = new BsonDocument("items", org.bson.BsonNull.VALUE);

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "details.amount", Operator.GTE, new Operand.Literal(new Int32Val(150)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("orders", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.EQ, new Operand.Literal(new Int32Val(50)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toView(toBuffer(createTestDoc())), Collections.emptyList()));
        }

        @Test
        void shouldMatchWithNeOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.NE, new Operand.Literal(new Int32Val(999)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toView(toBuffer(createTestDoc())), Collections.emptyList()));
        }

        @Test
        void shouldMatchWithLtOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.LT, new Operand.Literal(new Int32Val(50)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toView(toBuffer(createTestDoc())), Collections.emptyList()));
        }

        @Test
        void shouldMatchWithLteOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.LTE, new Operand.Literal(new Int32Val(10)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toView(toBuffer(createTestDoc())), Collections.emptyList()));
        }

        @Test
        void shouldMatchWithGteOperator() {
            ResidualPredicate subPredicate = new ResidualPredicate(1, "value", Operator.GTE, new Operand.Literal(new Int32Val(100)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);
            assertTrue(elemMatch.test(toView(toBuffer(createTestDoc())), Collections.emptyList()));
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
            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Operand.Literal(new StringVal("urgent")));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Operand.Literal(new StringVal("urgent")));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.NE, new Operand.Literal(new StringVal("critical")));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldHandleEmptyStringArray() {
            BsonDocument doc = new BsonDocument("tags", new BsonArray());

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Operand.Literal(new StringVal("urgent")));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("tags", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GT, new Operand.Literal(new Int32Val(90)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GT, new Operand.Literal(new Int32Val(90)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.LTE, new Operand.Literal(new Int32Val(15)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("values", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Operand.Literal(new Int32Val(3)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("values", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldHandleEmptyNumberArray() {
            BsonDocument doc = new BsonDocument("scores", new BsonArray());

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GT, new Operand.Literal(new Int32Val(50)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }

        @Test
        void shouldMatchInt64WithGteOperator() {
            // Document: { bigNumbers: [1000000000000, 2000000000000] }
            // Query: { bigNumbers: { $elemMatch: { $gte: 1500000000000 } } }
            BsonDocument doc = new BsonDocument("bigNumbers", new BsonArray(List.of(
                    new BsonInt64(1000000000000L),
                    new BsonInt64(2000000000000L)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GTE, new Operand.Literal(new Int64Val(1500000000000L)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("bigNumbers", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.LT, new Operand.Literal(new DoubleVal(15.0)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("prices", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Operand.Literal(new BooleanVal(true)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("flags", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
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

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.EQ, new Operand.Literal(new BooleanVal(true)));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("flags", subPredicate);

            assertFalse(elemMatch.test(toView(toBuffer(doc)), Collections.emptyList()));
        }
    }

    @Nested
    @DisplayName("Parameterized Execution")
    class ParameterizedExecution {

        private static Operand param(int index) {
            return new Operand.Param(new ParamRef(index));
        }

        private static Operand paramList(int... indices) {
            List<ParamRef> refs = Arrays.stream(indices).mapToObj(ParamRef::new).toList();
            return new Operand.ParamList(refs);
        }

        @Test
        void shouldMatchDocumentArrayWithParamOperand() {
            // Behavior: Param(0) in sub-predicate resolves and matches a document array element.
            // Document: { items: [{price: 50}, {price: 150}] }
            // Query: { items: { $elemMatch: { price: { $gt: ?0 } } } } with ?0 = 100
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)),
                    new BsonDocument("price", new BsonInt32(150))
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "price", Operator.GT, param(0));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), List.of(new Int32Val(100))));
            assertFalse(elemMatch.test(toView(toBuffer(doc)), List.of(new Int32Val(200))));
        }

        @Test
        void shouldMatchScalarArrayWithParamOperand() {
            // Behavior: Param(0) in sub-predicate resolves and matches a scalar array element.
            // Document: { scores: [75, 88, 92] }
            // Query: { scores: { $elemMatch: { $gte: ?0 } } } with ?0 = 90
            BsonDocument doc = new BsonDocument("scores", new BsonArray(List.of(
                    new BsonInt32(75),
                    new BsonInt32(88),
                    new BsonInt32(92)
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "", Operator.GTE, param(0));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("scores", subPredicate);

            assertTrue(elemMatch.test(toView(toBuffer(doc)), List.of(new Int32Val(90))));
            assertFalse(elemMatch.test(toView(toBuffer(doc)), List.of(new Int32Val(95))));
        }

        @Test
        void shouldMatchWithParamListInCondition() {
            // Behavior: ParamList resolves and matches with the IN operator within $elemMatch.
            // Document: { items: [{status: "pending"}, {status: "shipped"}] }
            // Query: { items: { $elemMatch: { status: { $in: [?0, ?1] } } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("status", new BsonString("pending")),
                    new BsonDocument("status", new BsonString("shipped"))
            )));

            ResidualPredicate subPredicate = new ResidualPredicate(1, "status", Operator.IN, paramList(0, 1));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            List<BqlValue> params = List.of(new StringVal("shipped"), new StringVal("delivered"));
            assertTrue(elemMatch.test(toView(toBuffer(doc)), params));

            List<BqlValue> params2 = List.of(new StringVal("cancelled"), new StringVal("returned"));
            assertFalse(elemMatch.test(toView(toBuffer(doc)), params2));
        }

        @Test
        void shouldMatchWithMultipleConditionsUsingParams() {
            // Behavior: The AND condition with multiple Param operands resolves correctly.
            // Document: { items: [{price: 50, qty: 2}, {price: 150, qty: 5}] }
            // Query: { items: { $elemMatch: { price: { $gt: ?0 }, qty: { $gte: ?1 } } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("price", new BsonInt32(50)).append("qty", new BsonInt32(2)),
                    new BsonDocument("price", new BsonInt32(150)).append("qty", new BsonInt32(5))
            )));

            ResidualAndNode subPredicate = new ResidualAndNode(List.of(
                    new ResidualPredicate(1, "price", Operator.GT, param(0)),
                    new ResidualPredicate(2, "qty", Operator.GTE, param(1))
            ));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            // Second element matches: price 150 > 100, qty 5 >= 3
            assertTrue(elemMatch.test(toView(toBuffer(doc)), List.of(new Int32Val(100), new Int32Val(3))));

            // No element matches: price > 100 requires qty >= 10, but the second element has qty=5
            assertFalse(elemMatch.test(toView(toBuffer(doc)), List.of(new Int32Val(100), new Int32Val(10))));
        }

        @Test
        void shouldMatchWithOrConditionUsingParams() {
            // Behavior: OR condition with Param operands matches if any branch satisfies.
            // Document: { items: [{status: "pending", priority: 1}, {status: "shipped", priority: 3}] }
            // Query: { items: { $elemMatch: { $or: [{ status: { $eq: ?0 } }, { priority: { $gte: ?1 } }] } } }
            BsonDocument doc = new BsonDocument("items", new BsonArray(List.of(
                    new BsonDocument("status", new BsonString("pending")).append("priority", new BsonInt32(1)),
                    new BsonDocument("status", new BsonString("shipped")).append("priority", new BsonInt32(3))
            )));

            ResidualOrNode subPredicate = new ResidualOrNode(List.of(
                    new ResidualPredicate(1, "status", Operator.EQ, param(0)),
                    new ResidualPredicate(2, "priority", Operator.GTE, param(1))
            ));
            ResidualElemMatchNode elemMatch = new ResidualElemMatchNode("items", subPredicate);

            // First element matches: status = "pending"
            assertTrue(elemMatch.test(toView(toBuffer(doc)), List.of(new StringVal("pending"), new Int32Val(5))));

            // The second element matches: priority 3 >= 2
            assertTrue(elemMatch.test(toView(toBuffer(doc)), List.of(new StringVal("cancelled"), new Int32Val(2))));

            // No element matches: status not "cancelled", priority not >= 5
            assertFalse(elemMatch.test(toView(toBuffer(doc)), List.of(new StringVal("cancelled"), new Int32Val(5))));
        }
    }
}
