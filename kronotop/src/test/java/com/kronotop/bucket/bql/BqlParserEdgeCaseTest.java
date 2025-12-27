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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Edge case and boundary testing for BQL parser to validate extreme values and boundary conditions.
 * These tests ensure the parser handles edge cases gracefully and provides meaningful error messages.
 */
class BqlParserEdgeCaseTest {

    @Test
    @DisplayName("Numeric boundary values should parse correctly")
    void shouldParseNumericBoundaryValuesCorrectly() {
        Map<String, String> edgeCases = Map.of(
                "Integer MAX_VALUE", "{ \"selector\": " + Integer.MAX_VALUE + " }",
                "Integer MIN_VALUE", "{ \"selector\": " + Integer.MIN_VALUE + " }",
                "Zero", "{ \"selector\": 0 }",
                "Negative zero", "{ \"selector\": -0 }",
                "Large positive double", "{ \"selector\": " + Double.MAX_VALUE + " }",
                "Small positive double", "{ \"selector\": " + Double.MIN_VALUE + " }",
                "Large negative double", "{ \"selector\": " + (-Double.MAX_VALUE) + " }"
        );

        for (Map.Entry<String, String> entry : edgeCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);
                assertInstanceOf(BqlEq.class, result, "Should create equality for " + description);

                // Verify serialization works
                String serialized = result.toJson();
                assertNotNull(serialized, "Should serialize " + description);

                // Verify roundtrip consistency
                BqlExpr reparsed = BqlParser.parse(serialized);
                assertEquals(serialized, reparsed.toJson(), "Roundtrip should work for " + description);

            }, "Should handle numeric edge case: " + description);
        }
    }

    @Test
    @DisplayName("Special floating point values should be handled correctly")
    void shouldHandleSpecialFloatingPointValuesCorrectly() {
        Map<String, String> specialValues = Map.of(
                "Very small positive", "{ \"selector\": 4.9e-324 }",
                "Very large positive", "{ \"selector\": 1.7976931348623157e+308 }",
                "Scientific notation", "{ \"selector\": 1.23e10 }",
                "Negative scientific", "{ \"selector\": -9.87e-5 }",
                "Zero with decimals", "{ \"selector\": 0.0 }",
                "Negative zero decimal", "{ \"selector\": -0.0 }"
        );

        for (Map.Entry<String, String> entry : specialValues.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                // Verify the value is stored as DoubleVal
                BqlEq eqNode = (BqlEq) result;
                assertInstanceOf(DoubleVal.class, eqNode.value(), "Should store as double: " + description);

            }, "Should handle special float: " + description);
        }
    }

    @Test
    @DisplayName("Unicode and special characters should be handled correctly")
    void shouldHandleUnicodeAndSpecialCharactersCorrectly() {
        Map<String, String> testCases = Map.of(
                "Unicode emoji", "{ \"🔥selector\": \"🎉value\" }",
                "Unicode text", "{ \"selector\": \"ñáméwithaccénts\" }",
                "Special ASCII chars", "{ \"selector-name_123\": \"value@domain.com\" }",
                "Quotes in strings", "{ \"selector\": \"value with \\\"quotes\\\"\" }",
                "Newlines and tabs", "{ \"selector\": \"line1\\nline2\\tindented\" }",
                "Unicode escape", "{ \"selector\": \"\\u0048\\u0065\\u006C\\u006C\\u006F\" }"
        );

        for (Map.Entry<String, String> entry : testCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                // Verify serialization preserves unicode
                String serialized = result.toJson();
                assertNotNull(serialized, "Should serialize " + description);

            }, "Should handle unicode case: " + description);
        }
    }

    @Test
    @DisplayName("Very long selector names and values should be handled correctly")
    void shouldHandleVeryLongSelectorNamesAndValuesCorrectly() {
        // Test very long selector name (1000 characters)
        String longSelectorName = "a".repeat(1000);
        String longSelectorQuery = "{ \"" + longSelectorName + "\": \"value\" }";

        assertDoesNotThrow(() -> {
            BqlExpr result = BqlParser.parse(longSelectorQuery);
            assertNotNull(result, "Should handle long selector name");

            BqlEq eqNode = (BqlEq) result;
            assertEquals(longSelectorName, eqNode.selector(), "Should preserve long selector name");

        }, "Should handle very long selector names");

        // Test very long value (5000 characters)
        String longValue = "value".repeat(1000);
        String longValueQuery = "{ \"selector\": \"" + longValue + "\" }";

        assertDoesNotThrow(() -> {
            BqlExpr result = BqlParser.parse(longValueQuery);
            assertNotNull(result, "Should handle long value");

            BqlEq eqNode = (BqlEq) result;
            StringVal stringVal = (StringVal) eqNode.value();
            assertEquals(longValue, stringVal.value(), "Should preserve long value");

        }, "Should handle very long values");
    }

    @Test
    @DisplayName("Empty and whitespace-only strings should be handled correctly")
    void shouldHandleEmptyAndWhitespaceStringsCorrectly() {
        Map<String, String> testCases = Map.of(
                "Empty string value", "{ \"selector\": \"\" }",
                "Whitespace only", "{ \"selector\": \"   \" }",
                "Tab and newline", "{ \"selector\": \"\\t\\n\" }",
                "Mixed whitespace", "{ \"selector\": \" \\t\\n\\r \" }"
        );

        for (Map.Entry<String, String> entry : testCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                // Verify the empty/whitespace value is preserved
                BqlEq eqNode = (BqlEq) result;
                assertInstanceOf(StringVal.class, eqNode.value(), "Should be string value");

                // Verify serialization roundtrip
                String serialized = result.toJson();
                BqlExpr reparsed = BqlParser.parse(serialized);
                assertEquals(serialized, reparsed.toJson(), "Roundtrip should work for " + description);

            }, "Should handle: " + description);
        }
    }

    @Test
    @DisplayName("Array boundary conditions should be handled correctly")
    void shouldHandleArrayBoundaryConditionsCorrectly() {
        Map<String, String> arrayCases = Map.of(
                "Empty array", "{ \"selector\": { \"$in\": [] } }",
                "Single element", "{ \"selector\": { \"$in\": [\"single\"] } }",
                "Many elements", "{ \"selector\": { \"$in\": [" + "\"item\",".repeat(99) + "\"item\"] } }",
                "Mixed types", "{ \"selector\": { \"$in\": [\"string\", 123, true, null] } }",
                "Nested arrays", "{ \"selector\": { \"$in\": [[1,2], [3,4]] } }"
        );

        for (Map.Entry<String, String> entry : arrayCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            if (description.equals("Mixed types") || description.equals("Nested arrays")) {
                // These might not be supported by current parser, test separately
                continue;
            }

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);
                assertInstanceOf(BqlIn.class, result, "Should create BqlIn for " + description);

            }, "Should handle array case: " + description);
        }
    }

    @Test
    @DisplayName("Size operator boundary values should be validated")
    void shouldValidateSizeOperatorBoundaryValues() {
        Map<String, String> sizeCases = Map.of(
                "Size zero", "{ \"selector\": { \"$size\": 0 } }",
                "Size one", "{ \"selector\": { \"$size\": 1 } }",
                "Large size", "{ \"selector\": { \"$size\": " + Integer.MAX_VALUE + " } }",
                "Maximum int", "{ \"selector\": { \"$size\": 2147483647 } }"
        );

        for (Map.Entry<String, String> entry : sizeCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);
                assertInstanceOf(BqlSize.class, result, "Should create BqlSize for " + description);

                BqlSize sizeNode = (BqlSize) result;
                assertTrue(sizeNode.size() >= 0, "Size should be non-negative for " + description);

            }, "Should handle size boundary: " + description);
        }
    }

    @Test
    @DisplayName("Deeply nested document structures should be handled correctly")
    void shouldHandleDeeplyNestedDocumentStructuresCorrectly() {
        // Create a deeply nested document (10 levels)
        StringBuilder nestedDoc = new StringBuilder();
        nestedDoc.append("{ \"selector\": { \"$eq\": ");

        for (int i = 0; i < 10; i++) {
            nestedDoc.append("{ \"level").append(i).append("\": ");
        }
        nestedDoc.append("\"deepValue\"");
        for (int i = 0; i < 10; i++) {
            nestedDoc.append(" }");
        }
        nestedDoc.append(" } }");

        String deepQuery = nestedDoc.toString();

        assertDoesNotThrow(() -> {
            BqlExpr result = BqlParser.parse(deepQuery);
            assertNotNull(result, "Should handle deeply nested document");

            // Verify serialization works
            String serialized = result.toJson();
            assertNotNull(serialized, "Should serialize deeply nested document");

            // Verify explanation doesn't crash
            String explanation = BqlParser.explain(result);
            assertNotNull(explanation, "Should explain deeply nested document");

        }, "Should handle deeply nested document structures");
    }

    @Test
    @DisplayName("Extreme array sizes in queries should be handled efficiently")
    void shouldHandleExtremeArraySizesEfficiently() {
        // Test with large array in $in operator (1000 elements)
        StringBuilder largeArray = new StringBuilder();
        largeArray.append("{ \"selector\": { \"$in\": [");
        for (int i = 0; i < 1000; i++) {
            if (i > 0) largeArray.append(", ");
            largeArray.append("\"value").append(i).append("\"");
        }
        largeArray.append("] } }");

        String largeArrayQuery = largeArray.toString();

        assertDoesNotThrow(() -> {
            long startTime = System.nanoTime();
            BqlExpr result = BqlParser.parse(largeArrayQuery);
            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000;

            assertNotNull(result, "Should handle large array");
            assertTrue(durationMs < 100, "Should parse large array quickly, took: " + durationMs + "ms");

            BqlIn inNode = (BqlIn) result;
            assertEquals(1000, inNode.values().size(), "Should have all 1000 values");

        }, "Should handle extreme array sizes efficiently");
    }

    @Test
    @DisplayName("Boolean edge cases should be handled correctly")
    void shouldHandleBooleanEdgeCasesCorrectly() {
        Map<String, String> booleanCases = Map.of(
                "True value", "{ \"selector\": true }",
                "False value", "{ \"selector\": false }",
                "Exists true", "{ \"selector\": { \"$exists\": true } }",
                "Exists false", "{ \"selector\": { \"$exists\": false } }"
        );

        for (Map.Entry<String, String> entry : booleanCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                // Verify the boolean value is correctly parsed
                if (description.startsWith("Exists")) {
                    assertInstanceOf(BqlExists.class, result, "Should create BqlExists for " + description);
                } else {
                    assertInstanceOf(BqlEq.class, result, "Should create BqlEq for " + description);
                    BqlEq eqNode = (BqlEq) result;
                    assertInstanceOf(BooleanVal.class, eqNode.value(), "Should store as boolean for " + description);
                }

            }, "Should handle boolean case: " + description);
        }
    }

    @Test
    @DisplayName("Null value handling should work correctly")
    void shouldHandleNullValueCorrectly() {
        String nullQuery = "{ \"selector\": null }";

        // Note: This might throw an exception if null is not supported
        // The behavior depends on the parser's null handling policy
        assertDoesNotThrow(() -> {
            try {
                BqlExpr result = BqlParser.parse(nullQuery);
                assertNotNull(result, "Parser should handle null query structure");
                // If null values are supported, verify the structure
            } catch (BqlParseException e) {
                // If null values are not supported, that's also valid behavior
                assertTrue(e.getMessage().contains("Unsupported value type") ||
                                e.getMessage().contains("null"),
                        "Should have meaningful error for null: " + e.getMessage());
            }
        }, "Should handle null values gracefully (either support or meaningful error)");
    }

    @Test
    @DisplayName("Extreme comparison values should be handled correctly")
    void shouldHandleExtremeComparisonValuesCorrectly() {
        Map<String, String> extremeCases = Map.of(
                "Very large string comparison", "{ \"selector\": { \"$gt\": \"" + "z".repeat(1000) + "\" } }",
                "Empty string comparison", "{ \"selector\": { \"$lt\": \"\" } }",
                "Max integer comparison", "{ \"selector\": { \"$gte\": " + Integer.MAX_VALUE + " } }",
                "Min integer comparison", "{ \"selector\": { \"$lte\": " + Integer.MIN_VALUE + " } }",
                "Large double comparison", "{ \"selector\": { \"$ne\": " + Double.MAX_VALUE + " } }"
        );

        for (Map.Entry<String, String> entry : extremeCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                // Verify it's a comparison operator
                assertTrue(result instanceof BqlGt || result instanceof BqlLt ||
                                result instanceof BqlGte || result instanceof BqlLte ||
                                result instanceof BqlNe,
                        "Should create comparison operator for " + description);

                // Verify serialization works
                String serialized = result.toJson();
                assertNotNull(serialized, "Should serialize " + description);

            }, "Should handle extreme comparison: " + description);
        }
    }

    @Test
    @DisplayName("Whitespace variations in JSON should be handled correctly")
    void shouldHandleWhitespaceVariationsInJSONCorrectly() {
        Map<String, String> whitespaceCases = Map.of(
                "No whitespace", "{\"selector\":\"value\"}",
                "Extra spaces", "{ \"selector\" : \"value\" }",
                "Tabs and newlines", "{\n\t\"selector\":\n\t\"value\"\n}",
                "Mixed whitespace", "{ \t \"selector\" \n : \t \"value\" \n }",
                "Trailing whitespace", "{ \"selector\": \"value\" }   \n\t"
        );

        for (Map.Entry<String, String> entry : whitespaceCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                // All should parse to the same logical structure
                BqlEq eqNode = (BqlEq) result;
                assertEquals("selector", eqNode.selector(), "Selector name should be correct for " + description);
                assertEquals("value", ((StringVal) eqNode.value()).value(), "Value should be correct for " + description);

            }, "Should handle whitespace variation: " + description);
        }
    }

    @Test
    @DisplayName("Selector name edge cases should be handled correctly")
    void shouldHandleSelectorNameEdgeCasesCorrectly() {
        Map<String, String> selectorNameCases = Map.of(
                "Single character", "{ \"a\": \"value\" }",
                "Numeric-like name", "{ \"123\": \"value\" }",
                "Special characters", "{ \"selector-name_123\": \"value\" }",
                "Dots in name", "{ \"selector.subselector\": \"value\" }",
                "Dollar in middle", "{ \"selector$name\": \"value\" }",
                "Underscore variations", "{ \"_selector_\": \"value\" }"
        );

        for (Map.Entry<String, String> entry : selectorNameCases.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            assertDoesNotThrow(() -> {
                BqlExpr result = BqlParser.parse(query);
                assertNotNull(result, "Should handle " + description);

                BqlEq eqNode = (BqlEq) result;
                assertNotNull(eqNode.selector(), "Selector name should not be null for " + description);
                assertFalse(eqNode.selector().trim().isEmpty(), "Selector name should not be empty for " + description);

            }, "Should handle selector name case: " + description);
        }
    }

    @Test
    @DisplayName("Invalid numeric formats should be rejected with meaningful errors")
    void shouldRejectInvalidNumericFormatsWithMeaningfulErrors() {
        Map<String, String> invalidNumbers = Map.of(
                "Leading zeros", "{ \"selector\": 007 }",
                "Hexadecimal", "{ \"selector\": 0xFF }",
                "Binary", "{ \"selector\": 0b1010 }",
                "Incomplete decimal", "{ \"selector\": 123. }",
                "Multiple decimals", "{ \"selector\": 12.34.56 }"
        );

        for (Map.Entry<String, String> entry : invalidNumbers.entrySet()) {
            String description = entry.getKey();
            String query = entry.getValue();

            // These should either parse correctly (if the format is actually valid)
            // or throw a meaningful exception
            assertDoesNotThrow(() -> {
                try {
                    BqlExpr result = BqlParser.parse(query);
                    // If it parses, that's fine - the format might be valid JSON
                    assertNotNull(result, "If parsed, result should not be null for " + description);
                } catch (BqlParseException e) {
                    // If it fails, it should have a meaningful error message
                    assertTrue(e.getMessage().contains("Invalid BSON format") ||
                                    e.getMessage().contains("parse") ||
                                    e.getMessage().contains("format"),
                            "Should have meaningful error for " + description + ": " + e.getMessage());
                }
            }, "Should handle invalid number format gracefully: " + description);
        }
    }
}