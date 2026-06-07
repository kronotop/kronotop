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

import com.kronotop.internal.StringUtil;
import org.bson.BsonValue;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class UpdateOptions {
    public static final String SET = "$set";
    public static final String UNSET = "$unset";
    public static final String ARRAY_FILTERS = "array_filters";
    public static final String UPSERT = "upsert";
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_]*");
    private final Map<String, BsonValue> setOps;
    private final Set<String> unsetOps;
    private final Map<String, ArrayFilter> arrayFilters;
    private final boolean upsert;
    private final Set<String> positionalFields;

    private UpdateOptions(Builder b) {
        this.setOps = Collections.unmodifiableMap(new LinkedHashMap<>(b.setOps));
        this.unsetOps = Collections.unmodifiableSet(new LinkedHashSet<>(b.unsetOps));
        this.arrayFilters = Collections.unmodifiableMap(new LinkedHashMap<>(b.arrayFilters));
        this.upsert = b.upsert;
        this.positionalFields = Collections.unmodifiableSet(extractPositionalFields(this.setOps, this.unsetOps));
        validatePositionalOperator();
    }

    private static Set<String> extractPositionalFields(Map<String, BsonValue> setOps, Set<String> unsetOps) {
        Set<String> fields = new HashSet<>();
        Stream.concat(setOps.keySet().stream(), unsetOps.stream())
                .peek(UpdateOptions::validatePositionalOperatorSyntax)
                .filter(UpdateOptions::containsPositionalOperator)
                .forEach(path -> {
                    validateNoMultiplePositionalOperators(path);
                    fields.add(extractArrayPath(path));
                });
        return fields;
    }

    private static boolean containsPositionalOperator(String path) {
        String[] segments = StringUtil.split(path);
        for (String segment : segments) {
            if (segment.equals("$")) {
                return true;
            }
        }
        return false;
    }

    private static void validateNoMultiplePositionalOperators(String path) {
        String[] segments = StringUtil.split(path);
        int count = 0;
        for (String segment : segments) {
            if (segment.equals("$")) {
                count++;
            }
        }
        if (count > 1) {
            throw new IllegalArgumentException(
                    "Multiple positional operators not supported in path: " + path);
        }
    }

    private static void validatePositionalOperatorSyntax(String path) {
        String[] segments = StringUtil.split(path);
        for (String segment : segments) {
            if (segment.startsWith("$") && !isValidPositionalOperator(segment)) {
                throw new IllegalArgumentException(
                        "Invalid positional operator '" + segment + "' in path: " + path);
            }
        }
    }

    private static boolean isValidPositionalOperator(String segment) {
        // Valid: exactly "$"
        if (segment.equals("$")) {
            return true;
        }
        // Valid: exactly "$[]"
        if (segment.equals("$[]")) {
            return true;
        }
        // Valid: "$[identifier]" where identifier matches [a-zA-Z_][a-zA-Z0-9_]*
        if (segment.startsWith("$[") && segment.endsWith("]")) {
            String inner = segment.substring(2, segment.length() - 1);
            return IDENTIFIER_PATTERN.matcher(inner).matches();
        }
        // Invalid: anything else starting with $
        return false;
    }

    private static String extractArrayPath(String path) {
        String[] segments = StringUtil.split(path);
        StringBuilder arrayPath = new StringBuilder();
        for (String segment : segments) {
            if (segment.equals("$")) {
                break;
            }
            if (!arrayPath.isEmpty()) {
                arrayPath.append(".");
            }
            arrayPath.append(segment);
        }
        if (arrayPath.isEmpty()) {
            throw new IllegalArgumentException(
                    "Invalid positional operator path: " + path + " - array field required before $");
        }
        return arrayPath.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    private void validatePositionalOperator() {
        if (upsert && !positionalFields.isEmpty()) {
            throw new IllegalArgumentException("Positional operator $ cannot be used with upsert=true");
        }
    }

    public Map<String, BsonValue> setOps() {
        return setOps;
    }

    public Set<String> unsetOps() {
        return unsetOps;
    }

    public Map<String, ArrayFilter> arrayFilters() {
        return arrayFilters;
    }

    public boolean upsert() {
        return upsert;
    }

    public Set<String> positionalFields() {
        return positionalFields;
    }

    public static final class Builder {
        private final Map<String, BsonValue> setOps = new LinkedHashMap<>();
        private final Set<String> unsetOps = new LinkedHashSet<>();
        private final Map<String, ArrayFilter> arrayFilters = new LinkedHashMap<>();
        private boolean upsert = false;

        private static void requireField(String f) {
            if (f == null || f.isBlank()) {
                throw new IllegalArgumentException("field is null/blank");
            }
        }

        // --- fluent ops ---
        public Builder set(String field, BsonValue value) {
            requireField(field);
            setOps.put(field, value);
            return this;
        }

        public Builder unset(String field) {
            requireField(field);
            unsetOps.add(field);
            return this;
        }

        public Builder arrayFilter(ArrayFilter filter) {
            if (filter == null || filter.identifier() == null || filter.identifier().isBlank()) {
                throw new IllegalArgumentException("arrayFilter identifier is null/blank");
            }
            // Extract the base identifier (before any dot) for the map key
            // e.g., "expensive.price" → map key is "expensive", matching $[expensive]
            String identifier = filter.identifier();
            int dotIndex = identifier.indexOf('.');
            String mapKey = dotIndex > 0 ? identifier.substring(0, dotIndex) : identifier;
            arrayFilters.put(mapKey, filter);
            return this;
        }

        public Builder upsert(boolean upsert) {
            this.upsert = upsert;
            return this;
        }

        // --- build ---
        public UpdateOptions build() {
            if (!unsetOps.isEmpty()) {
                unsetOps.forEach(setOps::remove);
            }
            return new UpdateOptions(this);
        }
    }
}
