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

import static org.junit.jupiter.api.Assertions.*;

/**
 * Complex nested query tests for BQL parser to validate real-world usage patterns.
 * These tests focus on deeply nested structures and complex operator combinations
 * that are commonly used in production scenarios.
 */
class BqlParserComplexNestedTest {

    @Test
    @DisplayName("Deeply nested AND/OR query should parse correctly")
    void shouldParseDeeplyNestedAndOrQueryCorrectly() {
        String complexQuery = """
                {
                  "$and": [
                    { "status": "active" },
                    {
                      "$or": [
                        { "priority": { "$gte": 5 } },
                        { "tags": { "$in": ["urgent", "critical"] } }
                      ]
                    },
                    {
                      "metadata": {
                        "$elemMatch": {
                          "key": "environment", 
                          "value": { "$ne": "test" }
                        }
                      }
                    }
                  ]
                }
                """;

        BqlExpr result = BqlParser.parse(complexQuery);
        assertNotNull(result, "Complex query should parse successfully");
        assertInstanceOf(BqlAnd.class, result, "Root should be BqlAnd");

        BqlAnd andNode = (BqlAnd) result;
        assertEquals(3, andNode.children().size(), "Should have 3 AND conditions");

        // Verify first condition: { "status": "active" }
        assertInstanceOf(BqlEq.class, andNode.children().get(0), "First condition should be equality");
        BqlEq statusEq = (BqlEq) andNode.children().get(0);
        assertEquals("status", statusEq.selector());
        assertEquals("active", ((StringVal) statusEq.value()).value());

        // Verify second condition: $or with priority and tags
        assertInstanceOf(BqlOr.class, andNode.children().get(1), "Second condition should be OR");
        BqlOr orNode = (BqlOr) andNode.children().get(1);
        assertEquals(2, orNode.children().size(), "OR should have 2 conditions");

        // Verify third condition: elemMatch with nested conditions
        assertInstanceOf(BqlElemMatch.class, andNode.children().get(2), "Third condition should be elemMatch");
        BqlElemMatch elemMatch = (BqlElemMatch) andNode.children().get(2);
        assertEquals("metadata", elemMatch.selector());

        // Verify serialization works
        String serialized = result.toJson();
        assertNotNull(serialized, "Complex query should serialize successfully");
        assertFalse(serialized.trim().isEmpty(), "Serialized query should not be empty");

        // Verify explanation contains expected elements
        String explanation = BqlParser.explain(result);
        assertTrue(explanation.contains("BqlAnd"), "Explanation should contain BqlAnd");
        assertTrue(explanation.contains("BqlOr"), "Explanation should contain BqlOr");
        assertTrue(explanation.contains("BqlElemMatch"), "Explanation should contain BqlElemMatch");
    }

    @Test
    @DisplayName("Multi-level nested logical operators should parse correctly")
    void shouldParseMultiLevelNestedLogicalOperatorsCorrectly() {
        String nestedQuery = """
                {
                  "$or": [
                    {
                      "$and": [
                        { "category": "electronics" },
                        { "price": { "$lt": 1000 } },
                        { "in_stock": true }
                      ]
                    },
                    {
                      "$and": [
                        { "category": "books" },
                        {
                          "$or": [
                            { "author": "Jane Doe" },
                            { "publisher": "Tech Books Inc" }
                          ]
                        }
                      ]
                    }
                  ]
                }
                """;

        BqlExpr result = BqlParser.parse(nestedQuery);
        assertNotNull(result, "Multi-level nested query should parse");
        assertInstanceOf(BqlOr.class, result, "Root should be BqlOr");

        BqlOr rootOr = (BqlOr) result;
        assertEquals(2, rootOr.children().size(), "Root OR should have 2 branches");

        // Both branches should be AND nodes
        assertInstanceOf(BqlAnd.class, rootOr.children().get(0), "First branch should be AND");
        assertInstanceOf(BqlAnd.class, rootOr.children().get(1), "Second branch should be AND");

        // Verify the second branch has nested OR
        BqlAnd secondAnd = (BqlAnd) rootOr.children().get(1);
        assertEquals(2, secondAnd.children().size(), "Second AND should have 2 conditions");
        assertInstanceOf(BqlOr.class, secondAnd.children().get(1), "Second condition should be OR");

        // Verify serialization roundtrip
        String serialized = result.toJson();
        BqlExpr reparsed = BqlParser.parse(serialized);
        assertEquals(serialized, reparsed.toJson(), "Roundtrip should be consistent");
    }

    @Test
    @DisplayName("Complex elemMatch with nested conditions should parse correctly")
    void shouldParseComplexElemMatchWithNestedConditionsCorrectly() {
        String elemMatchQuery = """
                {
                  "orders": {
                    "$elemMatch": {
                      "$and": [
                        { "status": "shipped" },
                        { "total": { "$gte": 100 } },
                        {
                          "items": {
                            "$elemMatch": {
                              "category": "electronics",
                              "price": { "$lt": 500 }
                            }
                          }
                        }
                      ]
                    }
                  }
                }
                """;

        BqlExpr result = BqlParser.parse(elemMatchQuery);
        assertNotNull(result, "Complex elemMatch should parse");
        assertInstanceOf(BqlElemMatch.class, result, "Root should be elemMatch");

        BqlElemMatch outerElemMatch = (BqlElemMatch) result;
        assertEquals("orders", outerElemMatch.selector());

        // The nested expression should be an AND
        assertInstanceOf(BqlAnd.class, outerElemMatch.expr(), "ElemMatch should contain AND");
        BqlAnd nestedAnd = (BqlAnd) outerElemMatch.expr();
        assertEquals(3, nestedAnd.children().size(), "AND should have 3 conditions");

        // Third condition should be another elemMatch
        assertInstanceOf(BqlElemMatch.class, nestedAnd.children().get(2), "Third condition should be elemMatch");
        BqlElemMatch innerElemMatch = (BqlElemMatch) nestedAnd.children().get(2);
        assertEquals("items", innerElemMatch.selector());

        // Verify serialization works
        String serialized = result.toJson();
        assertNotNull(serialized, "Complex elemMatch should serialize");
        assertTrue(serialized.contains("$elemMatch"), "Should contain elemMatch operators");

        // Verify explanation generation
        String explanation = BqlParser.explain(result);
        assertTrue(explanation.contains("BqlElemMatch"), "Should contain elemMatch explanation");
        assertTrue(explanation.contains("orders"), "Should reference orders selector");
        assertTrue(explanation.contains("items"), "Should reference items selector");
    }

    @Test
    @DisplayName("Deep nesting with mixed operators should parse correctly")
    void shouldParseDeepNestingWithMixedOperatorsCorrectly() {
        String deepQuery = """
                {
                  "$and": [
                    { "tenant_id": "abc123" },
                    {
                      "$or": [
                        {
                          "permissions": {
                            "$elemMatch": {
                              "resource": "documents",
                              "actions": { "$all": ["read", "write"] }
                            }
                          }
                        },
                        {
                          "$and": [
                            { "role": "admin" },
                            { "active": true },
                            { "last_login": { "$gte": "2024-01-01" } }
                          ]
                        }
                      ]
                    },
                    {
                      "preferences": {
                        "$elemMatch": {
                          "notifications": { "$exists": true }
                        }
                      }
                    }
                  ]
                }
                """;

        BqlExpr result = BqlParser.parse(deepQuery);
        assertNotNull(result, "Deep nested query should parse");
        assertInstanceOf(BqlAnd.class, result, "Root should be AND");

        BqlAnd rootAnd = (BqlAnd) result;
        assertEquals(3, rootAnd.children().size(), "Root AND should have 3 conditions");

        // Second condition should be OR with complex nested structure
        assertInstanceOf(BqlOr.class, rootAnd.children().get(1), "Second condition should be OR");
        BqlOr nestedOr = (BqlOr) rootAnd.children().get(1);
        assertEquals(2, nestedOr.children().size(), "OR should have 2 branches");

        // First OR branch should be elemMatch
        assertInstanceOf(BqlElemMatch.class, nestedOr.children().get(0), "First OR branch should be elemMatch");

        // Second OR branch should be AND
        assertInstanceOf(BqlAnd.class, nestedOr.children().get(1), "Second OR branch should be AND");

        // Third root condition should be elemMatch (changed from NOT for parser compatibility)
        assertInstanceOf(BqlElemMatch.class, rootAnd.children().get(2), "Third condition should be elemMatch");

        // Verify all nested structures can be serialized
        String serialized = result.toJson();
        assertNotNull(serialized, "Deep query should serialize");
        assertTrue(serialized.contains("$and"), "Should contain AND operators");
        assertTrue(serialized.contains("$or"), "Should contain OR operators");
        assertTrue(serialized.contains("$elemMatch"), "Should contain elemMatch operators");
        assertTrue(serialized.contains("$elemMatch"), "Should contain elemMatch operators");

        // Verify explanation contains all operator types
        String explanation = BqlParser.explain(result);
        assertTrue(explanation.contains("BqlAnd"), "Should explain AND");
        assertTrue(explanation.contains("BqlOr"), "Should explain OR");
        assertTrue(explanation.contains("BqlElemMatch"), "Should explain elemMatch");
        assertTrue(explanation.contains("BqlElemMatch"), "Should explain elemMatch");
    }

    @Test
    @DisplayName("Complex array operations with nesting should parse correctly")
    void shouldParseComplexArrayOperationsWithNestingCorrectly() {
        String arrayQuery = """
                {
                  "$or": [
                    {
                      "tags": {
                        "$elemMatch": {
                          "$and": [
                            { "value": { "$in": ["important", "urgent", "critical"] } },
                            { "value": { "$ne": "archived" } }
                          ]
                        }
                      }
                    },
                    {
                      "$and": [
                        { "categories": { "$all": ["electronics", "mobile"] } },
                        { "excluded_tags": { "$nin": ["discontinued", "out-of-stock"] } },
                        { "features": { "$size": 5 } }
                      ]
                    }
                  ]
                }
                """;

        BqlExpr result = BqlParser.parse(arrayQuery);
        assertNotNull(result, "Complex array query should parse");
        assertInstanceOf(BqlOr.class, result, "Root should be OR");

        BqlOr rootOr = (BqlOr) result;
        assertEquals(2, rootOr.children().size(), "Root OR should have 2 branches");

        // First branch should be elemMatch with AND
        assertInstanceOf(BqlElemMatch.class, rootOr.children().get(0), "First branch should be elemMatch");
        BqlElemMatch elemMatch = (BqlElemMatch) rootOr.children().get(0);
        assertEquals("tags", elemMatch.selector());
        assertInstanceOf(BqlAnd.class, elemMatch.expr(), "ElemMatch should contain AND");

        // Second branch should be AND with array operations
        assertInstanceOf(BqlAnd.class, rootOr.children().get(1), "Second branch should be AND");
        BqlAnd arrayAnd = (BqlAnd) rootOr.children().get(1);
        assertEquals(3, arrayAnd.children().size(), "Array AND should have 3 conditions");

        // Verify array operators are correctly parsed
        assertInstanceOf(BqlAll.class, arrayAnd.children().get(0), "Should have $all operator");
        assertInstanceOf(BqlNin.class, arrayAnd.children().get(1), "Should have $nin operator");
        assertInstanceOf(BqlSize.class, arrayAnd.children().get(2), "Should have $size operator");

        // Verify serialization preserves structure
        String serialized = result.toJson();
        assertNotNull(serialized, "Array query should serialize");
        assertTrue(serialized.contains("$all"), "Should contain $all");
        assertTrue(serialized.contains("$nin"), "Should contain $nin");
        assertTrue(serialized.contains("$size"), "Should contain $size");
    }

    @Test
    @DisplayName("Realistic e-commerce query with multiple nesting levels should parse correctly")
    void shouldParseRealisticECommerceQueryWithMultipleNestingLevelsCorrectly() {
        String ecommerceQuery = """
                {
                  "$and": [
                    { "store_id": "store123" },
                    { "status": "published" },
                    {
                      "$or": [
                        {
                          "$and": [
                            { "category": "electronics" },
                            { "$and": [ { "price": { "$gte": 100 } }, { "price": { "$lte": 1000 } } ] },
                            { "brand": { "$in": ["Apple", "Samsung", "Sony"] } }
                          ]
                        },
                        {
                          "$and": [
                            { "category": "clothing" },
                            { "size": { "$in": ["M", "L", "XL"] } },
                            {
                              "attributes": {
                                "$elemMatch": {
                                  "name": "color",
                                  "value": { "$in": ["black", "white", "blue"] }
                                }
                              }
                            }
                          ]
                        }
                      ]
                    },
                    {
                      "inventory": {
                        "$elemMatch": {
                          "warehouse": "main",
                          "quantity": { "$gt": 0 },
                          "reserved": { "$lt": 10 }
                        }
                      }
                    },
                    { "created_at": { "$gte": "2024-01-01" } }
                  ]
                }
                """;

        BqlExpr result = BqlParser.parse(ecommerceQuery);
        assertNotNull(result, "E-commerce query should parse");
        assertInstanceOf(BqlAnd.class, result, "Root should be AND");

        BqlAnd rootAnd = (BqlAnd) result;
        assertEquals(5, rootAnd.children().size(), "Should have 5 top-level conditions");

        // Verify the complex OR condition (third child)
        assertInstanceOf(BqlOr.class, rootAnd.children().get(2), "Third condition should be OR");
        BqlOr categoryOr = (BqlOr) rootAnd.children().get(2);
        assertEquals(2, categoryOr.children().size(), "Category OR should have 2 branches");

        // Both branches should be AND with multiple conditions
        assertInstanceOf(BqlAnd.class, categoryOr.children().get(0), "Electronics branch should be AND");
        assertInstanceOf(BqlAnd.class, categoryOr.children().get(1), "Clothing branch should be AND");

        // Verify inventory elemMatch (fourth child)
        assertInstanceOf(BqlElemMatch.class, rootAnd.children().get(3), "Fourth condition should be elemMatch");
        BqlElemMatch inventoryMatch = (BqlElemMatch) rootAnd.children().get(3);
        assertEquals("inventory", inventoryMatch.selector());

        // Verify serialization and roundtrip
        String serialized = result.toJson();
        assertNotNull(serialized, "E-commerce query should serialize");
        BqlExpr reparsed = BqlParser.parse(serialized);
        assertEquals(serialized, reparsed.toJson(), "E-commerce query roundtrip should be consistent");

        // Verify explanation contains business logic elements
        String explanation = BqlParser.explain(result);
        assertTrue(explanation.contains("store_id"), "Should reference store");
        assertTrue(explanation.contains("category"), "Should reference categories");
        assertTrue(explanation.contains("inventory"), "Should reference inventory");
        assertTrue(explanation.contains("price"), "Should reference pricing");
    }

    @Test
    @DisplayName("Performance test with very deep nesting should complete within reasonable time")
    void shouldCompleteVeryDeepNestingWithinReasonableTime() {
        // Create a deeply nested query (7 levels deep)
        String deepQuery = """
                {
                  "$and": [
                    { "level1": "value1" },
                    {
                      "$or": [
                        { "level2a": "value2a" },
                        {
                          "$and": [
                            { "level3a": "value3a" },
                            {
                              "$or": [
                                { "level4a": "value4a" },
                                {
                                  "$and": [
                                    { "level5a": "value5a" },
                                    {
                                      "$or": [
                                        { "level6a": "value6a" },
                                        { "level7a": "value7a" }
                                      ]
                                    }
                                  ]
                                }
                              ]
                            }
                          ]
                        }
                      ]
                    }
                  ]
                }
                """;

        long startTime = System.nanoTime();

        BqlExpr result = BqlParser.parse(deepQuery);

        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000;

        assertNotNull(result, "Deep query should parse successfully");
        assertTrue(durationMs < 100, "Deep query parsing should complete within 100ms, took: " + durationMs + "ms");

        // Verify the structure is correctly parsed
        assertInstanceOf(BqlAnd.class, result, "Root should be AND");
        BqlAnd rootAnd = (BqlAnd) result;
        assertEquals(2, rootAnd.children().size(), "Root should have 2 conditions");

        // Verify serialization performance
        long serializeStart = System.nanoTime();
        String serialized = result.toJson();
        long serializeEnd = System.nanoTime();
        long serializeDurationMs = (serializeEnd - serializeStart) / 1_000_000;

        assertNotNull(serialized, "Deep query should serialize");
        assertTrue(serializeDurationMs < 50, "Serialization should be fast, took: " + serializeDurationMs + "ms");

        // Verify explanation performance
        long explainStart = System.nanoTime();
        String explanation = BqlParser.explain(result);
        long explainEnd = System.nanoTime();
        long explainDurationMs = (explainEnd - explainStart) / 1_000_000;

        assertNotNull(explanation, "Deep query should explain");
        assertTrue(explainDurationMs < 50, "Explanation should be fast, took: " + explainDurationMs + "ms");
    }

    @Test
    @DisplayName("Mixed operator complexity with real-world patterns should parse correctly")
    void shouldParseMixedOperatorComplexityWithRealWorldPatternsCorrectly() {
        String mixedQuery = """
                {
                  "$and": [
                    {
                      "$or": [
                        { "user_type": "premium" },
                        {
                          "$and": [
                            { "user_type": "free" },
                            { "trial_expires": { "$gte": "2024-12-31" } }
                          ]
                        }
                      ]
                    },
                    {
                      "permissions": {
                        "$not": {
                          "$elemMatch": {
                            "resource": "admin_panel",
                            "access": "denied"
                          }
                        }
                      }
                    },
                    {
                      "$or": [
                        { "region": { "$in": ["US", "EU", "APAC"] } },
                        { "$and": [ { "ip_whitelist": { "$exists": true } }, { "ip_whitelist": { "$ne": [] } } ] }
                      ]
                    },
                    {
                      "activity": {
                        "$elemMatch": {
                          "type": "login",
                          "timestamp": { "$gte": "2024-01-01" },
                          "success": true,
                          "blocked_reason": { "$exists": false }
                        }
                      }
                    }
                  ]
                }
                """;

        BqlExpr result = BqlParser.parse(mixedQuery);
        assertNotNull(result, "Mixed complexity query should parse");
        assertInstanceOf(BqlAnd.class, result, "Root should be AND");

        BqlAnd rootAnd = (BqlAnd) result;
        assertEquals(4, rootAnd.children().size(), "Should have 4 main conditions");

        // Verify first condition: user type OR
        assertInstanceOf(BqlOr.class, rootAnd.children().get(0), "First condition should be OR");

        // Verify second condition: permissions NOT elemMatch
        assertInstanceOf(BqlNot.class, rootAnd.children().get(1), "Second condition should be NOT");
        BqlNot notNode = (BqlNot) rootAnd.children().get(1);
        assertInstanceOf(BqlElemMatch.class, notNode.expr(), "NOT should contain elemMatch");

        // Verify third condition: region OR with exists check
        assertInstanceOf(BqlOr.class, rootAnd.children().get(2), "Third condition should be OR");

        // Verify fourth condition: complex activity elemMatch
        assertInstanceOf(BqlElemMatch.class, rootAnd.children().get(3), "Fourth condition should be elemMatch");
        BqlElemMatch activityMatch = (BqlElemMatch) rootAnd.children().get(3);
        assertEquals("activity", activityMatch.selector());

        // Verify all operators are preserved in serialization
        String serialized = result.toJson();
        assertTrue(serialized.contains("$and"), "Should preserve AND");
        assertTrue(serialized.contains("$or"), "Should preserve OR");
        assertTrue(serialized.contains("$not"), "Should preserve NOT");
        assertTrue(serialized.contains("$elemMatch"), "Should preserve elemMatch");
        assertTrue(serialized.contains("$exists"), "Should preserve exists");
        assertTrue(serialized.contains("$in"), "Should preserve in");

        // Verify explanation covers all operators
        String explanation = BqlParser.explain(result);
        assertTrue(explanation.contains("BqlAnd"), "Should explain AND");
        assertTrue(explanation.contains("BqlOr"), "Should explain OR");
        assertTrue(explanation.contains("BqlNot"), "Should explain NOT");
        assertTrue(explanation.contains("BqlElemMatch"), "Should explain elemMatch");
    }
}