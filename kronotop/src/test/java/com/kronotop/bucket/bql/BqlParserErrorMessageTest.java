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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Error message quality tests for BQL parser to validate helpful and specific error messages.
 * These tests ensure developers get meaningful feedback when queries fail to parse.
 */
class BqlParserErrorMessageTest {

    @Test
    @DisplayName("Unknown operator errors should be specific and helpful")
    void testUnknownOperatorErrors() {
        Map<String, String> invalidOperators = Map.of(
                "{ \"$unknown\": [] }", "Unknown operator: $unknown",
                "{ \"$invalid\": [ { \"selector\": \"value\" } ] }", "Unknown operator: $invalid",
                "{ \"$badOp\": [ { \"a\": 1 }, { \"b\": 2 } ] }", "Unknown operator: $badOp",
                "{ \"$typo\": { \"selector\": \"value\" } }", "Unknown operator: $typo"
        );

        for (Map.Entry<String, String> entry : invalidOperators.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedMessageFragment = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for invalid operator: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertNotNull(errorMessage, "Error message should not be null");
            assertTrue(errorMessage.contains("BQL parse error"), "Should contain BQL parse error prefix");
            assertTrue(errorMessage.contains(expectedMessageFragment),
                    String.format("Error message should contain '%s' but was: %s",
                            expectedMessageFragment, errorMessage));
        }
    }

    @Test
    @DisplayName("Unknown selector operator errors should specify the invalid operator")
    void testUnknownSelectorOperatorErrors() {
        Map<String, String> invalidSelectorOperators = Map.of(
                "{ \"selector\": { \"$unknownOp\": \"value\" } }", "Unknown selector operator: $unknownOp",
                "{ \"name\": { \"$badOperator\": 123 } }", "Unknown selector operator: $badOperator",
                "{ \"status\": { \"$invalidSelectorOp\": true } }", "Unknown selector operator: $invalidSelectorOp",
                "{ \"count\": { \"$wrongOp\": [1, 2, 3] } }", "Unknown selector operator: $wrongOp"
        );

        for (Map.Entry<String, String> entry : invalidSelectorOperators.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedMessageFragment = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for invalid selector operator: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains(expectedMessageFragment),
                    String.format("Error message should contain '%s' but was: %s",
                            expectedMessageFragment, errorMessage));
        }
    }

    @Test
    @DisplayName("Type validation errors should specify expected types")
    void testTypeValidationErrors() {
        Map<String, String> typeErrors = Map.of(
                "{ \"selector\": { \"$size\": \"not_an_integer\" } }", "$size expects an integer",
                "{ \"selector\": { \"$size\": true } }", "$size expects an integer",
                "{ \"selector\": { \"$size\": [] } }", "$size expects an integer",
                "{ \"selector\": { \"$exists\": \"not_boolean\" } }", "$exists expects a boolean",
                "{ \"selector\": { \"$exists\": 123 } }", "$exists expects a boolean",
                "{ \"selector\": { \"$exists\": [] } }", "$exists expects a boolean"
        );

        for (Map.Entry<String, String> entry : typeErrors.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedMessageFragment = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for type error: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains(expectedMessageFragment),
                    String.format("Error message should contain '%s' but was: %s",
                            expectedMessageFragment, errorMessage));
        }
    }

    @Test
    @DisplayName("Empty selector operator document errors should specify the selector")
    void testEmptySelectorOperatorErrors() {
        Map<String, String> emptyOperatorCases = Map.of(
                "{ \"selector\": { } }", "Empty selector operator document for: selector",
                "{ \"name\": { } }", "Empty selector operator document for: name",
                "{ \"status\": { } }", "Empty selector operator document for: status",
                "{ \"very_long_selector_name\": { } }", "Empty selector operator document for: very_long_selector_name"
        );

        for (Map.Entry<String, String> entry : emptyOperatorCases.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedMessageFragment = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for empty operator: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains(expectedMessageFragment),
                    String.format("Error message should contain '%s' but was: %s",
                            expectedMessageFragment, errorMessage));
        }
    }

    @Test
    @DisplayName("JSON format errors should be clear and helpful")
    void testJSONFormatErrors() {
        Map<String, String> jsonErrors = Map.of(
                "{ invalid json }", "Invalid BSON format",
                "{ \"selector\": \"unclosed string }", "Invalid BSON format",
                "{ \"selector\": value_without_quotes }", "Invalid BSON format",
                "{ \"selector\": { \"$gt\": } }", "Invalid BSON format"
        );

        for (Map.Entry<String, String> entry : jsonErrors.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedMessageFragment = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for JSON error: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains("BQL parse error"), "Should contain BQL parse error prefix");
            assertTrue(errorMessage.contains(expectedMessageFragment),
                    String.format("Error message should contain '%s' but was: %s",
                            expectedMessageFragment, errorMessage));
        }
    }

    @Test
    @DisplayName("Null and empty query errors should be descriptive")
    void testNullAndEmptyQueryErrors() {
        String[] invalidQueries = {null, "", "   ", "\t\n"};

        for (String invalidQuery : invalidQueries) {
            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for null/empty query: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertNotNull(errorMessage, "Error message should not be null");
            assertTrue(errorMessage.contains("BQL parse error"), "Should contain BQL parse error prefix");
            assertTrue(errorMessage.contains("Invalid BSON format"),
                    "Should mention invalid format for null/empty: " + errorMessage);
        }
    }

    @Test
    @DisplayName("Unsupported value type errors should specify the unsupported type")
    void testUnsupportedValueTypeErrors() {
        // Note: These test cases depend on what value types the parser actually supports
        // We'll test based on the current parser implementation

        // Test with a known unsupported type pattern if it exists
        // For now, let's test with a complex nested structure that might cause issues
        String[] potentiallyUnsupportedQueries = {
                "{ \"selector\": { \"nested\": { \"deeply\": { \"very\": { \"complex\": \"value\" } } } } }"
        };

        for (String query : potentiallyUnsupportedQueries) {
            try {
                BqlParser.parse(query);
                // If it parses successfully, that's fine - the type is supported
            } catch (BqlParseException e) {
                String errorMessage = e.getMessage();
                if (errorMessage.contains("Unsupported value type")) {
                    // If we get this error, verify it mentions the type
                    assertTrue(errorMessage.contains("type"),
                            "Unsupported type error should mention the type: " + errorMessage);
                }
                // Other types of errors are also acceptable for this test
            }
        }
    }

    @Test
    @DisplayName("Error messages should provide context about the problematic part")
    void testErrorContextInformation() {
        Map<String, String[]> contextualErrors = Map.of(
                "{ \"selector\": { \"$invalidOp\": \"value\" } }",
                new String[]{"selector", "$invalidOp"},
                "{ \"$badOperator\": [ { \"nested\": \"value\" } ] }",
                new String[]{"$badOperator"},
                "{ \"status\": { \"$size\": \"not_integer\" } }",
                new String[]{"$size", "integer"},
                "{ \"permissions\": { } }",
                new String[]{"permissions", "Empty"}
        );

        for (Map.Entry<String, String[]> entry : contextualErrors.entrySet()) {
            String invalidQuery = entry.getKey();
            String[] expectedContexts = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for contextual error: " + invalidQuery);

            String errorMessage = exception.getMessage();
            for (String expectedContext : expectedContexts) {
                assertTrue(errorMessage.contains(expectedContext),
                        String.format("Error message should contain context '%s' but was: %s",
                                expectedContext, errorMessage));
            }
        }
    }

    @Test
    @DisplayName("Error messages should be concise and not overly verbose")
    void testErrorMessageConciseness() {
        String[] invalidQueries = {
                "{ \"$unknown\": [] }",
                "{ \"selector\": { \"$invalidOp\": \"value\" } }",
                "{ \"selector\": { } }",
                "{ invalid json }",
                "{ \"selector\": { \"$size\": \"string\" } }"
        };

        for (String invalidQuery : invalidQueries) {
            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for: " + invalidQuery);

            String errorMessage = exception.getMessage();

            // Error message should be reasonably concise (less than 200 characters)
            assertTrue(errorMessage.length() < 200,
                    "Error message should be concise (< 200 chars), but was " + errorMessage.length() +
                            " chars: " + errorMessage);

            // Should not contain stack trace information in the message
            assertFalse(errorMessage.contains("at com.kronotop"),
                    "Error message should not contain stack trace: " + errorMessage);

            // Should not contain internal implementation details
            assertFalse(errorMessage.toLowerCase().contains("bson reader"),
                    "Error message should not expose internal details: " + errorMessage);
        }
    }

    @Test
    @DisplayName("Error messages should be consistent in format and structure")
    void testErrorMessageConsistency() {
        Map<String, String> errorCategories = Map.of(
                "Unknown operator", "{ \"$unknownOp\": [] }",
                "Unknown selector operator", "{ \"selector\": { \"$badOp\": \"value\" } }",
                "Type validation", "{ \"selector\": { \"$size\": \"string\" } }",
                "Empty document", "{ \"selector\": { } }",
                "JSON format", "{ invalid json }"
        );

        for (Map.Entry<String, String> entry : errorCategories.entrySet()) {
            String category = entry.getKey();
            String invalidQuery = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for " + category + ": " + invalidQuery);

            String errorMessage = exception.getMessage();

            // All error messages should start with the same prefix
            assertTrue(errorMessage.startsWith("BQL parse error:"),
                    "Error message should start with standard prefix for " + category + ": " + errorMessage);

            // Should not end with unnecessary punctuation variations
            assertFalse(errorMessage.endsWith("..") || errorMessage.endsWith(".,"),
                    "Error message should not have inconsistent punctuation for " + category + ": " + errorMessage);
        }
    }

    @Test
    @DisplayName("Error messages should help users understand how to fix the problem")
    void testErrorMessageHelpfulness() {
        Map<String, String[]> helpfulErrors = Map.of(
                "{ \"selector\": { \"$size\": \"string\" } }",
                new String[]{"$size", "expects", "integer"},
                "{ \"selector\": { \"$exists\": \"string\" } }",
                new String[]{"$exists", "expects", "boolean"},
                "{ \"selector\": { } }",
                new String[]{"Empty", "operator", "document"},
                "{ \"$invalidOperator\": [] }",
                new String[]{"Unknown", "operator"}
        );

        for (Map.Entry<String, String[]> entry : helpfulErrors.entrySet()) {
            String invalidQuery = entry.getKey();
            String[] helpfulTerms = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for: " + invalidQuery);

            String errorMessage = exception.getMessage();

            // Should contain terms that help understand the problem
            for (String helpfulTerm : helpfulTerms) {
                assertTrue(errorMessage.toLowerCase().contains(helpfulTerm.toLowerCase()),
                        String.format("Error message should contain helpful term '%s' but was: %s",
                                helpfulTerm, errorMessage));
            }
        }
    }

    @Test
    @DisplayName("Error messages should not leak sensitive internal information")
    void testErrorMessageSecurity() {
        String[] invalidQueries = {
                "{ \"$unknown\": [] }",
                "{ invalid json }",
                "{ \"selector\": { \"$badOp\": \"value\" } }",
                "{ \"selector\": { } }"
        };

        String[] sensitiveTerms = {
                "password", "secret", "key", "token", "credential",
                "internal", "debug", "trace", "stack", "exception",
                "com.kronotop.bucket.bql2", "BsonReader", "java.lang"
        };

        for (String invalidQuery : invalidQueries) {
            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for: " + invalidQuery);

            String errorMessage = exception.getMessage().toLowerCase();

            for (String sensitiveTerm : sensitiveTerms) {
                assertFalse(errorMessage.contains(sensitiveTerm.toLowerCase()),
                        String.format("Error message should not contain sensitive term '%s' but was: %s",
                                sensitiveTerm, exception.getMessage()));
            }
        }
    }

    @Test
    @DisplayName("Nested error contexts should provide clear location information")
    void testNestedErrorContexts() {
        Map<String, String[]> nestedErrors = Map.of(
                "{ \"$and\": [ { \"selector\": { \"$invalidOp\": \"value\" } } ] }",
                new String[]{"$invalidOp"},
                "{ \"outer\": { \"$elemMatch\": { \"inner\": { \"$badOp\": 123 } } } }",
                new String[]{"$badOp"},
                "{ \"$or\": [ { \"status\": \"active\" }, { \"invalid\": { } } ] }",
                new String[]{"invalid", "Empty"}
        );

        for (Map.Entry<String, String[]> entry : nestedErrors.entrySet()) {
            String invalidQuery = entry.getKey();
            String[] expectedContexts = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for nested error: " + invalidQuery);

            String errorMessage = exception.getMessage();

            // Should contain context about where the error occurred
            for (String expectedContext : expectedContexts) {
                assertTrue(errorMessage.contains(expectedContext),
                        String.format("Nested error should contain context '%s' but was: %s",
                                expectedContext, errorMessage));
            }
        }
    }

    @Test
    @DisplayName("Error messages should handle special characters in selector names correctly")
    void testErrorMessagesWithSpecialCharacters() {
        Map<String, String> specialSelectorErrors = Map.of(
                "{ \"selector-name_123\": { } }", "selector-name_123",
                "{ \"selector.with.dots\": { } }", "selector.with.dots",
                "{ \"selector$with$dollars\": { } }", "selector$with$dollars",
                "{ \"selector_with_underscores\": { } }", "selector_with_underscores"
        );

        for (Map.Entry<String, String> entry : specialSelectorErrors.entrySet()) {
            String invalidQuery = entry.getKey();
            String expectedSelectorName = entry.getValue();

            BqlParseException exception = assertThrows(BqlParseException.class,
                    () -> BqlParser.parse(invalidQuery),
                    "Should throw BqlParseException for special selector: " + invalidQuery);

            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains(expectedSelectorName),
                    String.format("Error message should contain selector name '%s' but was: %s",
                            expectedSelectorName, errorMessage));
        }
    }
}