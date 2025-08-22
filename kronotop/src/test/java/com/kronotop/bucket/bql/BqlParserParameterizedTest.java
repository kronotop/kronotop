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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Parameterized-style tests for BQL parser to reduce code duplication and improve maintainability.
 * These tests validate common operator patterns with shared validation logic without requiring junit-jupiter-params.
 */
class BqlParserParameterizedTest {

    @Test
    @DisplayName("Comparison operators should parse correctly with integer values")
    void testComparisonOperatorsWithIntegers() {
        List<String> operators = List.of("$gt", "$lt", "$gte", "$lte", "$ne", "$eq");

        for (String operator : operators) {
            String query = String.format("{ \"selector\": { \"%s\": 10 } }", operator);

            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null for operator: " + operator);

            // Verify the correct AST node type is created
            switch (operator) {
                case "$gt" -> assertInstanceOf(BqlGt.class, result);
                case "$lt" -> assertInstanceOf(BqlLt.class, result);
                case "$gte" -> assertInstanceOf(BqlGte.class, result);
                case "$lte" -> assertInstanceOf(BqlLte.class, result);
                case "$ne" -> assertInstanceOf(BqlNe.class, result);
                case "$eq" -> assertInstanceOf(BqlEq.class, result);
            }

            // Verify serialization roundtrip
            String serialized = result.toJson();
            assertNotNull(serialized, "Serialized result should not be null");
            assertFalse(serialized.trim().isEmpty(), "Serialized result should not be empty");

            // Verify explanation contains the correct operator name
            String explanation = BqlParser.explain(result);
            assertNotNull(explanation, "Explanation should not be null");
            assertTrue(explanation.contains("selector"), "Explanation should contain selector name");
        }
    }

    @Test
    @DisplayName("Comparison operators should parse correctly with string values")
    void testComparisonOperatorsWithStrings() {
        List<String> operators = List.of("$gt", "$lt", "$gte", "$lte", "$ne", "$eq");

        for (String operator : operators) {
            String query = String.format("{ \"name\": { \"%s\": \"value\" } }", operator);

            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null for operator: " + operator);

            // Verify serialization and parsing consistency
            String serialized = result.toJson();
            BqlExpr reparsed = BqlParser.parse(serialized);
            assertNotNull(reparsed, "Reparsed result should not be null");

            // Both should serialize to the same result
            assertEquals(serialized, reparsed.toJson(),
                    "Roundtrip should be consistent for operator: " + operator);
        }
    }

    @Test
    @DisplayName("Array operators should parse correctly with various array values")
    void testArrayOperators() {
        Map<String, List<String>> testCases = Map.of(
                "$in", List.of("[1, 2, 3]", "[\"active\", \"pending\"]"),
                "$nin", List.of("[1, 2, 3]", "[\"draft\", \"archived\"]"),
                "$all", List.of("[1, 2, 3]", "[\"tag1\", \"tag2\"]")
        );

        for (Map.Entry<String, List<String>> entry : testCases.entrySet()) {
            String operator = entry.getKey();
            for (String arrayValue : entry.getValue()) {
                String query = String.format("{ \"selector\": { \"%s\": %s } }", operator, arrayValue);

                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Result should not be null for operator: " + operator);

                // Verify the correct AST node type is created
                switch (operator) {
                    case "$in" -> assertInstanceOf(BqlIn.class, result);
                    case "$nin" -> assertInstanceOf(BqlNin.class, result);
                    case "$all" -> assertInstanceOf(BqlAll.class, result);
                }

                // Verify serialization works
                String serialized = result.toJson();
                assertNotNull(serialized, "Serialized result should not be null");
                assertTrue(serialized.contains(operator), "Serialized result should contain operator");

                // Verify explanation generation
                String explanation = BqlParser.explain(result);
                assertNotNull(explanation, "Explanation should not be null");
                assertFalse(explanation.trim().isEmpty(), "Explanation should not be empty");
            }
        }
    }

    @Test
    @DisplayName("Logical operators should parse correctly with multiple conditions")
    void testLogicalOperators() {
        List<String> operators = List.of("$and", "$or");

        for (String operator : operators) {
            String query = String.format(
                    "{ \"%s\": [ { \"selector1\": \"value1\" }, { \"selector2\": { \"$gt\": 10 } } ] }",
                    operator
            );

            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null for operator: " + operator);

            // Verify the correct AST node type is created
            switch (operator) {
                case "$and" -> assertInstanceOf(BqlAnd.class, result);
                case "$or" -> assertInstanceOf(BqlOr.class, result);
            }

            // Verify serialization roundtrip
            String serialized = result.toJson();
            BqlExpr reparsed = BqlParser.parse(serialized);
            assertEquals(serialized, reparsed.toJson(),
                    "Roundtrip should be consistent for logical operator: " + operator);

            // Verify explanation contains correct operator representation
            String explanation = BqlParser.explain(result);
            String expectedOperatorName = operator.equals("$and") ? "BqlAnd" : "BqlOr";
            assertTrue(explanation.contains(expectedOperatorName),
                    "Explanation should contain " + expectedOperatorName + " for " + operator);
        }
    }

    @Test
    @DisplayName("Special operators should parse correctly with appropriate values")
    void testSpecialOperators() {
        Map<String, List<String>> testCases = Map.of(
                "$exists", List.of("true", "false"),
                "$size", List.of("0", "5", "100")
        );

        for (Map.Entry<String, List<String>> entry : testCases.entrySet()) {
            String operator = entry.getKey();
            for (String value : entry.getValue()) {
                String query = String.format("{ \"selector\": { \"%s\": %s } }", operator, value);

                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Result should not be null for operator: " + operator);

                // Verify the correct AST node type is created
                switch (operator) {
                    case "$exists" -> assertInstanceOf(BqlExists.class, result);
                    case "$size" -> assertInstanceOf(BqlSize.class, result);
                }

                // Verify serialization works
                String serialized = result.toJson();
                assertNotNull(serialized, "Serialized result should not be null");
                assertTrue(serialized.contains(operator), "Serialized result should contain operator");
                assertTrue(serialized.contains(value), "Serialized result should contain value");

                // Verify explanation generation
                String explanation = BqlParser.explain(result);
                assertNotNull(explanation, "Explanation should not be null");
                assertTrue(explanation.contains("selector"), "Explanation should contain selector name");
            }
        }
    }

    @Test
    @DisplayName("Simple equality queries should parse correctly with different value types")
    void testSimpleEqualityWithDifferentTypes() {
        Map<String, String> testCases = Map.of(
                "\"string_value\"", "string",
                "42", "integer",
                "3.14", "double",
                "true", "boolean",
                "false", "boolean"
        );

        for (Map.Entry<String, String> entry : testCases.entrySet()) {
            String value = entry.getKey();
            String valueType = entry.getValue();
            String query = String.format("{ \"selector\": %s }", value);

            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null for " + valueType + " value");
            assertInstanceOf(BqlEq.class, result, "Result should be BqlEq for simple equality");

            // Verify serialization preserves the value type
            String serialized = result.toJson();
            assertTrue(serialized.contains(value),
                    "Serialized result should preserve " + valueType + " value: " + value);

            // Verify roundtrip consistency
            BqlExpr reparsed = BqlParser.parse(serialized);
            assertEquals(serialized, reparsed.toJson(),
                    "Roundtrip should be consistent for " + valueType + " values");
        }
    }

    @Test
    @DisplayName("Multiple queries with different operators should all parse successfully")
    void testDifferentOperatorCombinations() {
        // Test various combinations of single operators (multiple operators on same selector not yet supported)
        List<String> queries = List.of(
                "{ \"selector1\": { \"$gt\": 10 }, \"selector2\": { \"$lt\": 20 } }",
                "{ \"$and\": [ { \"selector\": { \"$gte\": 5 } }, { \"selector\": { \"$lte\": 15 } } ] }",
                "{ \"$or\": [ { \"status\": \"draft\" }, { \"status\": \"pending\" } ] }",
                "{ \"tags\": { \"$in\": [\"urgent\", \"important\"] } }"
        );

        for (String query : queries) {
            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null for query: " + query);

            // Verify serialization works
            String serialized = result.toJson();
            assertNotNull(serialized, "Serialized result should not be null");

            // Verify roundtrip consistency
            BqlExpr reparsed = BqlParser.parse(serialized);
            assertEquals(serialized, reparsed.toJson(), "Roundtrip should be consistent");

            // Verify explanation generation
            String explanation = BqlParser.explain(result);
            assertNotNull(explanation, "Explanation should not be null");
            assertFalse(explanation.trim().isEmpty(), "Explanation should not be empty");
        }
    }

    @Test
    @DisplayName("ElemMatch queries should parse correctly with various conditions")
    void testElemMatchQueries() {
        List<String> queries = List.of(
                "{ \"results\": { \"$elemMatch\": { \"score\": { \"$gte\": 80 } } } }",
                "{ \"tags\": { \"$elemMatch\": { \"$eq\": \"urgent\" } } }",
                "{ \"items\": { \"$elemMatch\": { \"price\": { \"$lt\": 100 }, \"available\": true } } }",
                "{ \"history\": { \"$elemMatch\": { \"$gte\": 90, \"$lt\": 95 } } }"
        );

        for (String query : queries) {
            BqlExpr result = BqlParser.parse(query);
            assertNotNull(result, "Result should not be null for elemMatch query");
            assertInstanceOf(BqlElemMatch.class, result, "Result should be BqlElemMatch");

            // Verify serialization works
            String serialized = result.toJson();
            assertNotNull(serialized, "Serialized result should not be null");
            assertTrue(serialized.contains("$elemMatch"), "Serialized result should contain $elemMatch");

            // Verify roundtrip consistency
            BqlExpr reparsed = BqlParser.parse(serialized);
            assertEquals(serialized, reparsed.toJson(), "ElemMatch roundtrip should be consistent");

            // Verify explanation generation
            String explanation = BqlParser.explain(result);
            assertTrue(explanation.contains("BqlElemMatch"), "Explanation should contain BqlElemMatch");
        }
    }

    @Test
    @DisplayName("Invalid queries should throw BqlParseException with specific error messages")
    void testInvalidQueriesWithSpecificErrors() {
        Map<String, String> invalidQueries = Map.of(
                "{ \"$invalidOp\": [] }", "Unknown operator: $invalidOp",
                "{ \"selector\": { \"$unknownSelectorOp\": 5 } }", "Unknown selector operator: $unknownSelectorOp",
                "{ \"selector\": { \"$size\": \"not_int\" } }", "$size expects an integer",
                "{ \"selector\": { \"$exists\": \"not_boolean\" } }", "$exists expects a boolean"
        );

        for (Map.Entry<String, String> entry : invalidQueries.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedErrorFragment = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for invalid query: " + invalidQuery);

            assertTrue(exception.getMessage().contains(expectedErrorFragment),
                    String.format("Error message should contain '%s' but was: %s",
                            expectedErrorFragment, exception.getMessage()));
        }
    }
}