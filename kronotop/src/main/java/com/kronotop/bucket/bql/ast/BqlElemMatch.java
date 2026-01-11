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

package com.kronotop.bucket.bql.ast;

/**
 * Represents an $elemMatch query expression for matching array elements.
 * <p>
 * For scalar arrays, inner operators use an empty selector which is stripped
 * during JSON serialization to produce clean output like {@code {"$eq": "value"}}
 * instead of {@code {"": "value"}}.
 */
public record BqlElemMatch(String selector, BqlExpr expr) implements BqlExpr {
    private static final String EMPTY_SELECTOR_PREFIX = "{\"\": ";

    public String toJson() {
        String innerJson = stripEmptySelectors(expr.toJson());
        return "{\"" + selector + "\": {\"$elemMatch\": " + innerJson + "}}";
    }

    /**
     * Converts empty selector wrappers to explicit operators in JSON output.
     * For scalar array operators:
     * - {@code {"": value}} becomes {@code {"$eq": value}} (implicit equality)
     * - {@code {"": {"$gte": value}}} becomes {@code {"$gte": value}} (explicit operator)
     * This ensures roundtrip parsing consistency.
     */
    private String stripEmptySelectors(String json) {
        if (json.startsWith(EMPTY_SELECTOR_PREFIX) && json.endsWith("}")) {
            // Extract the value part: {"": VALUE} -> VALUE
            String value = json.substring(EMPTY_SELECTOR_PREFIX.length(), json.length() - 1);

            // If value is already an operator object (e.g., {"$gte": 80}), return as-is
            // If it's a plain value (e.g., "urgent", 80), wrap in $eq for roundtrip consistency
            if (value.startsWith("{")) {
                return value;
            }
            return "{\"$eq\": " + value + "}";
        }

        // Handle $and arrays containing empty selectors
        if (json.startsWith("{\"$and\": [")) {
            return processAndArray(json);
        }

        // Handle $or arrays containing empty selectors
        if (json.startsWith("{\"$or\": [")) {
            return processOrArray(json);
        }

        return json;
    }

    private String processAndArray(String json) {
        return processLogicalArray(json, "{\"$and\": [", "]}");
    }

    private String processOrArray(String json) {
        return processLogicalArray(json, "{\"$or\": [", "]}");
    }

    private String processLogicalArray(String json, String prefix, String suffix) {
        if (!json.startsWith(prefix) || !json.endsWith(suffix)) {
            return json;
        }

        String arrayContent = json.substring(prefix.length(), json.length() - suffix.length());
        String[] elements = splitJsonArray(arrayContent);

        StringBuilder result = new StringBuilder(prefix);
        for (int i = 0; i < elements.length; i++) {
            if (i > 0) {
                result.append(", ");
            }
            result.append(stripEmptySelectors(elements[i].trim()));
        }
        result.append(suffix);

        return result.toString();
    }

    private String[] splitJsonArray(String content) {
        // Simple split that handles nested braces
        java.util.List<String> elements = new java.util.ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                depth--;
            } else if (c == ',' && depth == 0) {
                elements.add(content.substring(start, i));
                start = i + 1;
            }
        }

        if (start < content.length()) {
            elements.add(content.substring(start));
        }

        return elements.toArray(new String[0]);
    }
}
