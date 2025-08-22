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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test data builder for BQL queries using fluent API pattern.
 * Provides a clean, readable way to construct BQL query strings for testing.
 */
public class BqlTestDataBuilder {

    /**
     * Creates a new BQL query builder.
     */
    public static BqlQueryBuilder query() {
        return new BqlQueryBuilder();
    }

    /**
     * Creates a selector selector for building selector-based conditions.
     */
    public static SelectorBuilder selector(String selectorName) {
        return new SelectorBuilder(selectorName);
    }

    /**
     * Creates an AND logical operator builder.
     */
    public static LogicalBuilder and() {
        return new LogicalBuilder("$and");
    }

    /**
     * Creates an OR logical operator builder.
     */
    public static LogicalBuilder or() {
        return new LogicalBuilder("$or");
    }

    /**
     * Creates a NOT logical operator builder.
     */
    public static NotBuilder not() {
        return new NotBuilder();
    }

    /**
     * Main query builder class providing fluent API for BQL construction.
     */
    public static class BqlQueryBuilder {
        private String content;

        private BqlQueryBuilder() {
        }

        private BqlQueryBuilder(String content) {
            this.content = content;
        }

        /**
         * Sets the query content directly.
         */
        public BqlQueryBuilder content(String content) {
            this.content = content;
            return this;
        }

        /**
         * Creates an equality condition.
         */
        public BqlQueryBuilder eq(String selector, Object value) {
            this.content = String.format("{ \"%s\": %s }", selector, formatValue(value));
            return this;
        }

        /**
         * Creates a greater than condition.
         */
        public BqlQueryBuilder gt(String selector, Object value) {
            this.content = String.format("{ \"%s\": { \"$gt\": %s } }", selector, formatValue(value));
            return this;
        }

        /**
         * Creates a greater than or equal condition.
         */
        public BqlQueryBuilder gte(String selector, Object value) {
            this.content = String.format("{ \"%s\": { \"$gte\": %s } }", selector, formatValue(value));
            return this;
        }

        /**
         * Creates a less than condition.
         */
        public BqlQueryBuilder lt(String selector, Object value) {
            this.content = String.format("{ \"%s\": { \"$lt\": %s } }", selector, formatValue(value));
            return this;
        }

        /**
         * Creates a less than or equal condition.
         */
        public BqlQueryBuilder lte(String selector, Object value) {
            this.content = String.format("{ \"%s\": { \"$lte\": %s } }", selector, formatValue(value));
            return this;
        }

        /**
         * Creates a not equal condition.
         */
        public BqlQueryBuilder ne(String selector, Object value) {
            this.content = String.format("{ \"%s\": { \"$ne\": %s } }", selector, formatValue(value));
            return this;
        }

        /**
         * Creates an in condition.
         */
        public BqlQueryBuilder in(String selector, Object... values) {
            String valueList = Arrays.stream(values)
                    .map(this::formatValue)
                    .collect(Collectors.joining(", "));
            this.content = String.format("{ \"%s\": { \"$in\": [%s] } }", selector, valueList);
            return this;
        }

        /**
         * Creates a not in condition.
         */
        public BqlQueryBuilder nin(String selector, Object... values) {
            String valueList = Arrays.stream(values)
                    .map(this::formatValue)
                    .collect(Collectors.joining(", "));
            this.content = String.format("{ \"%s\": { \"$nin\": [%s] } }", selector, valueList);
            return this;
        }

        /**
         * Creates an all condition.
         */
        public BqlQueryBuilder all(String selector, Object... values) {
            String valueList = Arrays.stream(values)
                    .map(this::formatValue)
                    .collect(Collectors.joining(", "));
            this.content = String.format("{ \"%s\": { \"$all\": [%s] } }", selector, valueList);
            return this;
        }

        /**
         * Creates a size condition.
         */
        public BqlQueryBuilder size(String selector, int size) {
            this.content = String.format("{ \"%s\": { \"$size\": %d } }", selector, size);
            return this;
        }

        /**
         * Creates an exists condition.
         */
        public BqlQueryBuilder exists(String selector, boolean exists) {
            this.content = String.format("{ \"%s\": { \"$exists\": %s } }", selector, exists);
            return this;
        }

        /**
         * Creates an elemMatch condition.
         */
        public BqlQueryBuilder elemMatch(String selector, BqlQueryBuilder condition) {
            this.content = String.format("{ \"%s\": { \"$elemMatch\": %s } }", selector, condition.removeOuterBraces());
            return this;
        }

        /**
         * Creates an elemMatch condition with multiple conditions (implicit AND).
         */
        public BqlQueryBuilder elemMatch(String selector, BqlQueryBuilder... conditions) {
            if (conditions.length == 1) {
                return elemMatch(selector, conditions[0]);
            }

            String conditionList = Arrays.stream(conditions)
                    .map(BqlQueryBuilder::removeOuterBraces)
                    .collect(Collectors.joining(", "));
            this.content = String.format("{ \"%s\": { \"$elemMatch\": { %s } } }", selector, conditionList);
            return this;
        }

        /**
         * Creates an AND logical condition.
         */
        public BqlQueryBuilder and(BqlQueryBuilder... conditions) {
            String conditionList = Arrays.stream(conditions)
                    .map(BqlQueryBuilder::build)
                    .collect(Collectors.joining(", "));
            this.content = String.format("{ \"$and\": [%s] }", conditionList);
            return this;
        }

        /**
         * Creates an OR logical condition.
         */
        public BqlQueryBuilder or(BqlQueryBuilder... conditions) {
            String conditionList = Arrays.stream(conditions)
                    .map(BqlQueryBuilder::build)
                    .collect(Collectors.joining(", "));
            this.content = String.format("{ \"$or\": [%s] }", conditionList);
            return this;
        }

        /**
         * Creates a NOT logical condition.
         */
        public BqlQueryBuilder not(BqlQueryBuilder condition) {
            this.content = String.format("{ \"$not\": %s }", condition.build());
            return this;
        }

        /**
         * Creates an empty query (select all).
         */
        public BqlQueryBuilder empty() {
            this.content = "{}";
            return this;
        }

        /**
         * Helper method to format values appropriately for JSON.
         */
        private String formatValue(Object value) {
            if (value == null) {
                return "null";
            } else if (value instanceof String) {
                return "\"" + value + "\"";
            } else if (value instanceof Boolean || value instanceof Number) {
                return value.toString();
            } else {
                return "\"" + value.toString() + "\"";
            }
        }

        /**
         * Removes outer braces from the content (used for nested conditions).
         */
        private String removeOuterBraces() {
            if (content != null && content.startsWith("{") && content.endsWith("}")) {
                return content.substring(1, content.length() - 1).trim();
            }
            return content;
        }

        /**
         * Builds and returns the final query string.
         */
        public String build() {
            return content != null ? content : "{}";
        }

        @Override
        public String toString() {
            return build();
        }
    }

    /**
     * Selector builder for creating selector-based conditions with fluent API.
     */
    public static class SelectorBuilder {
        private final String selectorName;

        private SelectorBuilder(String selectorName) {
            this.selectorName = selectorName;
        }

        public BqlQueryBuilder eq(Object value) {
            return new BqlQueryBuilder().eq(selectorName, value);
        }

        public BqlQueryBuilder gt(Object value) {
            return new BqlQueryBuilder().gt(selectorName, value);
        }

        public BqlQueryBuilder gte(Object value) {
            return new BqlQueryBuilder().gte(selectorName, value);
        }

        public BqlQueryBuilder lt(Object value) {
            return new BqlQueryBuilder().lt(selectorName, value);
        }

        public BqlQueryBuilder lte(Object value) {
            return new BqlQueryBuilder().lte(selectorName, value);
        }

        public BqlQueryBuilder ne(Object value) {
            return new BqlQueryBuilder().ne(selectorName, value);
        }

        public BqlQueryBuilder in(Object... values) {
            return new BqlQueryBuilder().in(selectorName, values);
        }

        public BqlQueryBuilder nin(Object... values) {
            return new BqlQueryBuilder().nin(selectorName, values);
        }

        public BqlQueryBuilder all(Object... values) {
            return new BqlQueryBuilder().all(selectorName, values);
        }

        public BqlQueryBuilder size(int size) {
            return new BqlQueryBuilder().size(selectorName, size);
        }

        public BqlQueryBuilder exists(boolean exists) {
            return new BqlQueryBuilder().exists(selectorName, exists);
        }

        public BqlQueryBuilder exists() {
            return exists(true);
        }

        public BqlQueryBuilder doesNotExist() {
            return exists(false);
        }

        public BqlQueryBuilder elemMatch(BqlQueryBuilder condition) {
            return new BqlQueryBuilder().elemMatch(selectorName, condition);
        }

        public BqlQueryBuilder elemMatch(BqlQueryBuilder... conditions) {
            return new BqlQueryBuilder().elemMatch(selectorName, conditions);
        }
    }

    /**
     * Logical operator builder for creating complex logical conditions.
     */
    public static class LogicalBuilder {
        private final String operator;
        private final List<BqlQueryBuilder> conditions;

        private LogicalBuilder(String operator) {
            this.operator = operator;
            this.conditions = new ArrayList<>();
        }

        public LogicalBuilder condition(BqlQueryBuilder condition) {
            this.conditions.add(condition);
            return this;
        }

        public LogicalBuilder eq(String selector, Object value) {
            return condition(selector(selector).eq(value));
        }

        public LogicalBuilder gt(String selector, Object value) {
            return condition(selector(selector).gt(value));
        }

        public LogicalBuilder gte(String selector, Object value) {
            return condition(selector(selector).gte(value));
        }

        public LogicalBuilder lt(String selector, Object value) {
            return condition(selector(selector).lt(value));
        }

        public LogicalBuilder lte(String selector, Object value) {
            return condition(selector(selector).lte(value));
        }

        public LogicalBuilder ne(String selector, Object value) {
            return condition(selector(selector).ne(value));
        }

        public LogicalBuilder in(String selector, Object... values) {
            return condition(selector(selector).in(values));
        }

        public LogicalBuilder nin(String selector, Object... values) {
            return condition(selector(selector).nin(values));
        }

        public LogicalBuilder exists(String selector, boolean exists) {
            return condition(selector(selector).exists(exists));
        }

        public LogicalBuilder exists(String selector) {
            return exists(selector, true);
        }

        public LogicalBuilder size(String selector, int size) {
            return condition(selector(selector).size(size));
        }

        public LogicalBuilder elemMatch(String selector, BqlQueryBuilder condition) {
            return condition(selector(selector).elemMatch(condition));
        }

        public BqlQueryBuilder build() {
            String conditionList = conditions.stream()
                    .map(BqlQueryBuilder::build)
                    .collect(Collectors.joining(", "));
            return new BqlQueryBuilder(String.format("{ \"%s\": [%s] }", operator, conditionList));
        }
    }

    /**
     * NOT operator builder for creating negation conditions.
     */
    public static class NotBuilder {

        private NotBuilder() {
        }

        public BqlQueryBuilder condition(BqlQueryBuilder condition) {
            return new BqlQueryBuilder(String.format("{ \"$not\": %s }", condition.build()));
        }

        public BqlQueryBuilder eq(String selector, Object value) {
            return condition(selector(selector).eq(value));
        }

        public BqlQueryBuilder gt(String selector, Object value) {
            return condition(selector(selector).gt(value));
        }

        public BqlQueryBuilder gte(String selector, Object value) {
            return condition(selector(selector).gte(value));
        }

        public BqlQueryBuilder lt(String selector, Object value) {
            return condition(selector(selector).lt(value));
        }

        public BqlQueryBuilder lte(String selector, Object value) {
            return condition(selector(selector).lte(value));
        }

        public BqlQueryBuilder ne(String selector, Object value) {
            return condition(selector(selector).ne(value));
        }

        public BqlQueryBuilder in(String selector, Object... values) {
            return condition(selector(selector).in(values));
        }

        public BqlQueryBuilder exists(String selector, boolean exists) {
            return condition(selector(selector).exists(exists));
        }

        public BqlQueryBuilder exists(String selector) {
            return exists(selector, true);
        }

        public BqlQueryBuilder elemMatch(String selector, BqlQueryBuilder condition) {
            return condition(selector(selector).elemMatch(condition));
        }
    }

    /**
     * Predefined common query patterns for testing.
     */
    public static class CommonQueries {

        public static BqlQueryBuilder activeStatus() {
            return selector("status").eq("active");
        }

        public static BqlQueryBuilder priceRange(double min, double max) {
            return and()
                    .gte("price", min)
                    .lte("price", max)
                    .build();
        }

        public static BqlQueryBuilder categoryIn(String... categories) {
            return selector("category").in((Object[]) categories);
        }

        public static BqlQueryBuilder userPermissions(String userId, String... permissions) {
            return and()
                    .eq("user_id", userId)
                    .condition(selector("permissions").in((Object[]) permissions))
                    .build();
        }

        public static BqlQueryBuilder ecommerceQuery(String category, double minPrice, double maxPrice, String... brands) {
            return and()
                    .eq("category", category)
                    .gte("price", minPrice)
                    .lte("price", maxPrice)
                    .condition(selector("brand").in((Object[]) brands))
                    .eq("in_stock", true)
                    .build();
        }

        public static BqlQueryBuilder metadataSearch(String key, Object value) {
            return selector("metadata").elemMatch(
                    and().eq("key", key).eq("value", value).build()
            );
        }

        public static BqlQueryBuilder dateRangeQuery(String selector, String startDate, String endDate) {
            return and()
                    .gte(selector, startDate)
                    .lte(selector, endDate)
                    .build();
        }

        public static BqlQueryBuilder complexNestedQuery() {
            return and()
                    .eq("tenant_id", "tenant123")
                    .condition(
                            or()
                                    .eq("role", "admin")
                                    .condition(selector("permissions").elemMatch(
                                            and()
                                                    .eq("resource", "documents")
                                                    .condition(selector("actions").in("read", "write"))
                                                    .build()
                                    ))
                                    .build()
                    )
                    .exists("active")
                    .build();
        }
    }

    /**
     * Invalid query builders for testing error conditions.
     */
    public static class InvalidQueries {

        public static String unknownOperator(String operator) {
            return String.format("{ \"%s\": [] }", operator);
        }

        public static String unknownSelectorOperator(String selector, String operator) {
            return String.format("{ \"%s\": { \"%s\": \"value\" } }", selector, operator);
        }

        public static String emptySelectorOperator(String selector) {
            return String.format("{ \"%s\": { } }", selector);
        }

        public static String invalidJson(String malformedJson) {
            return malformedJson;
        }

        public static String typeError(String selector, String operator, Object wrongTypeValue) {
            return String.format("{ \"%s\": { \"%s\": %s } }", selector, operator,
                    wrongTypeValue instanceof String ? "\"" + wrongTypeValue + "\"" : wrongTypeValue);
        }

        public static String nullQuery() {
            return null;
        }

        public static String emptyQuery() {
            return "";
        }

        public static String whitespaceQuery() {
            return "   \t\n  ";
        }
    }

    /**
     * Performance test query builders.
     */
    public static class PerformanceQueries {

        public static BqlQueryBuilder simpleQuery() {
            return selector("status").eq("active");
        }

        public static BqlQueryBuilder complexQuery() {
            return and()
                    .eq("status", "active")
                    .condition(
                            or()
                                    .gte("priority", 5)
                                    .condition(selector("tags").in("urgent", "critical"))
                                    .build()
                    )
                    .condition(selector("metadata").elemMatch(
                            and()
                                    .eq("key", "environment")
                                    .ne("value", "test")
                                    .build()
                    ))
                    .build();
        }

        public static BqlQueryBuilder largeQuery(int conditionCount) {
            LogicalBuilder andBuilder = and();
            for (int i = 0; i < conditionCount; i++) {
                andBuilder.eq("selector" + i, "value" + i);
            }
            return andBuilder.build();
        }

        public static BqlQueryBuilder deeplyNestedQuery(int depth) {
            BqlQueryBuilder current = selector("level" + depth).eq("value" + depth);

            for (int i = depth - 1; i >= 0; i--) {
                current = and()
                        .eq("level" + i, "value" + i)
                        .condition(current)
                        .build();
            }

            return current;
        }
    }
}