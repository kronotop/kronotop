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

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generator for creating random valid BQL queries for property-based testing.
 * Generates syntactically valid queries with various operators and structures.
 */
public class BqlQueryGenerator {

    private static final List<String> FIELD_NAMES = List.of(
            "id", "name", "status", "age", "score", "priority", "tags", "metadata",
            "created_at", "updated_at", "user_id", "category", "active", "count"
    );

    private static final List<String> STRING_VALUES = List.of(
            "active", "inactive", "pending", "completed", "draft", "published",
            "admin", "user", "guest", "premium", "basic", "trial"
    );

    private static final List<String> COMPARISON_OPERATORS = List.of(
            "$gt", "$lt", "$gte", "$lte", "$ne", "$eq"
    );

    private static final List<String> ARRAY_OPERATORS = List.of(
            "$in", "$nin", "$all"
    );

    private static final List<String> LOGICAL_OPERATORS = List.of(
            "$and", "$or"
    );

    private final Random random;
    private final int maxDepth;

    public BqlQueryGenerator() {
        this(ThreadLocalRandom.current(), 3);
    }

    public BqlQueryGenerator(Random random, int maxDepth) {
        this.random = random;
        this.maxDepth = maxDepth;
    }

    /**
     * Generates a random valid BQL query.
     *
     * @return A valid JSON string representing a BQL query
     */
    public String generateRandomQuery() {
        return generateRandomQuery(0);
    }

    private String generateRandomQuery(int depth) {
        if (depth >= maxDepth || random.nextDouble() < 0.3) {
            // Generate simple selector query at max depth or randomly
            return generateSimpleSelectorQuery();
        }

        // Choose between logical operator or simple selector query
        if (random.nextDouble() < 0.4) {
            return generateLogicalQuery(depth);
        } else {
            return generateSimpleSelectorQuery();
        }
    }

    private String generateSimpleSelectorQuery() {
        String selectorName = randomSelectorName();

        // Choose operator type
        double rand = random.nextDouble();
        if (rand < 0.3) {
            // Simple equality
            return String.format("{ \"%s\": %s }", selectorName, randomValue());
        } else if (rand < 0.6) {
            // Comparison operator
            String operator = randomElement(COMPARISON_OPERATORS);
            return String.format("{ \"%s\": { \"%s\": %s } }", selectorName, operator, randomValue());
        } else if (rand < 0.8) {
            // Array operator
            String operator = randomElement(ARRAY_OPERATORS);
            return String.format("{ \"%s\": { \"%s\": %s } }", selectorName, operator, randomArray());
        } else if (rand < 0.9) {
            // Special operators
            return generateSpecialOperatorQuery(selectorName);
        } else {
            // ElemMatch
            return String.format("{ \"%s\": { \"$elemMatch\": %s } }", selectorName, generateSimpleSelectorQuery());
        }
    }

    private String generateSpecialOperatorQuery(String selectorName) {
        double rand = random.nextDouble();
        if (rand < 0.33) {
            // $exists
            return String.format("{ \"%s\": { \"$exists\": %s } }", selectorName, random.nextBoolean());
        } else if (rand < 0.66) {
            // $size
            return String.format("{ \"%s\": { \"$size\": %d } }", selectorName, random.nextInt(10));
        } else {
            // $not with simple comparison
            String operator = randomElement(COMPARISON_OPERATORS);
            return String.format("{ \"%s\": { \"$not\": { \"%s\": %s } } }",
                    selectorName, operator, randomValue());
        }
    }

    private String generateLogicalQuery(int depth) {
        String operator = randomElement(LOGICAL_OPERATORS);
        int numConditions = 2 + random.nextInt(3); // 2-4 conditions

        StringBuilder sb = new StringBuilder();
        sb.append("{ \"").append(operator).append("\": [");

        for (int i = 0; i < numConditions; i++) {
            if (i > 0) sb.append(", ");
            sb.append(generateRandomQuery(depth + 1));
        }

        sb.append("] }");
        return sb.toString();
    }

    private String randomSelectorName() {
        return randomElement(FIELD_NAMES);
    }

    private String randomValue() {
        double rand = random.nextDouble();
        if (rand < 0.33) {
            // String value
            return "\"" + randomElement(STRING_VALUES) + "\"";
        } else if (rand < 0.66) {
            // Integer value
            return String.valueOf(random.nextInt(1000));
        } else {
            // Boolean value
            return String.valueOf(random.nextBoolean());
        }
    }

    private String randomArray() {
        int size = 1 + random.nextInt(4); // 1-4 elements
        StringBuilder sb = new StringBuilder("[");

        for (int i = 0; i < size; i++) {
            if (i > 0) sb.append(", ");
            sb.append(randomValue());
        }

        sb.append("]");
        return sb.toString();
    }

    private <T> T randomElement(List<T> list) {
        return list.get(random.nextInt(list.size()));
    }

    /**
     * Generates a simple valid query for testing basic functionality.
     */
    public String generateSimpleValidQuery() {
        return generateSimpleSelectorQuery();
    }

    /**
     * Generates a complex nested query for stress testing.
     */
    public String generateComplexQuery() {
        return new BqlQueryGenerator(random, 5).generateRandomQuery();
    }
}