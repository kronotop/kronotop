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

import com.kronotop.bucket.Collation;
import com.kronotop.bucket.handlers.protocol.SortDirection;


/**
 * Immutable configuration options for query execution in the pipeline system.
 * This class provides a builder pattern for configuring query behavior such as
 * result limits, sort ordering, field selection, and deletion operations.
 *
 * <p>QueryOptions are used in conjunction with {@link QueryContext} to control
 * how queries are executed against the document database.
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Basic query with default options
 * QueryOptions options = QueryOptions.builder().build();
 *
 * // Query with limit
 * QueryOptions limited = QueryOptions.builder()
 *     .limit(50)
 *     .build();
 *
 * // Query with descending sort ordering and limit
 * QueryOptions descending = QueryOptions.builder()
 *     .sortDirection(SortDirection.DESC)
 *     .limit(20)
 *     .build();
 *
 * // Query with update options
 * QueryOptions withUpdate = QueryOptions.builder()
 *     .limit(50)
 *     .build();
 * }</pre>
 *
 * @see QueryContext
 * @since 1.0
 */
public class QueryOptions {
    /**
     * The field name to use for sorting results. Defaults to null (no explicit sort).
     */
    private final String sortByField;

    /**
     * The sort direction (ASC or DESC). Defaults to ASC.
     */
    private final SortDirection sortDirection;

    /**
     * Maximum number of documents to return in a single batch.
     */
    private final int limit;

    /**
     * The field name to use for in-memory result sorting. Defaults to null (no result sort).
     */
    private final String resultSortField;

    /**
     * The sort direction for in-memory result sorting (ASC or DESC). Defaults to ASC.
     */
    private final SortDirection resultSortDirection;

    private final UpdateOptions update;

    private final Collation collation;

    /**
     * Private constructor used by the Builder pattern.
     *
     * @param builder the Builder instance containing the configuration values
     */
    private QueryOptions(Builder builder) {
        this.sortByField = builder.sortByField;
        this.sortDirection = builder.sortDirection;
        this.resultSortField = builder.resultSortField;
        this.resultSortDirection = builder.resultSortDirection;
        this.limit = builder.limit;
        this.update = builder.update;
        this.collation = builder.collation;
    }

    /**
     * Creates a new Builder instance for constructing QueryOptions.
     *
     * @return a new Builder with default values
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns whether results should be returned in reverse order.
     * Derived from the sort direction: DESC means reverse ordering.
     *
     * @return true if the sort direction is DESC, false otherwise
     */
    public boolean isReverse() {
        return sortDirection == SortDirection.DESC;
    }

    /**
     * Returns the field name used for sorting results, or null if no explicit sort was requested.
     *
     * @return the sort field name, or null
     */
    public String getSortByField() {
        return sortByField;
    }

    /**
     * Returns the sort direction (ASC or DESC).
     * Defaults to ASC if not specified.
     *
     * @return the sort direction
     */
    public SortDirection getSortDirection() {
        return sortDirection;
    }

    /**
     * Returns the maximum number of documents to return in a single batch.
     * This controls pagination behavior and memory usage during query execution.
     *
     * @return the result limit (0 to {@value QueryContext#MAXIMUM_LIMIT})
     */
    public int limit() {
        return limit;
    }

    /**
     * Returns the field name used for in-memory result sorting, or null if not requested.
     *
     * @return the result sort field name, or null
     */
    public String getResultSortField() {
        return resultSortField;
    }

    /**
     * Returns the sort direction for in-memory result sorting.
     *
     * @return the result sort direction
     */
    public SortDirection getResultSortDirection() {
        return resultSortDirection;
    }

    public UpdateOptions update() {
        return update;
    }

    public Collation getCollation() {
        return collation;
    }

    /**
     * Builder class for constructing QueryOptions instances using the builder pattern.
     * Provides a fluent API for configuring query options with method chaining.
     *
     * <p>All builder methods return the Builder instance to allow method chaining.
     * Call {@link #build()} to create the final immutable QueryOptions instance.
     */
    public static class Builder {
        private UpdateOptions update;
        private Collation collation;

        /**
         * Field name for in-memory result sorting. Defaults to null.
         */
        private String resultSortField;

        /**
         * Result sort direction. Defaults to ASC.
         */
        private SortDirection resultSortDirection = SortDirection.ASC;

        /**
         * Field name for sorting. Defaults to null (no explicit sort).
         */
        private String sortByField;

        /**
         * Sort direction. Defaults to ASC.
         */
        private SortDirection sortDirection = SortDirection.ASC;

        /**
         * Result limit. Defaults to {@value QueryContext#DEFAULT_LIMIT}.
         */
        private int limit = QueryContext.DEFAULT_LIMIT;

        /**
         * Sets the field name to use for sorting results.
         *
         * @param sortByField the field name to sort by (must not be null)
         * @return this Builder instance for method chaining
         * @throws IllegalArgumentException if sortByField is null
         */
        public Builder sortByField(String sortByField) {
            this.sortByField = sortByField;
            return this;
        }

        /**
         * Sets the sort direction (ASC or DESC).
         *
         * @param sortDirection the sort direction
         * @return this Builder instance for method chaining
         */
        public Builder sortDirection(SortDirection sortDirection) {
            this.sortDirection = sortDirection;
            return this;
        }

        /**
         * Sets the maximum number of documents to return in a single batch.
         * This controls pagination and memory usage during query execution.
         *
         * @param limit the maximum result count (0 to {@value QueryContext#MAXIMUM_LIMIT})
         * @return this Builder instance for method chaining
         * @throws IllegalArgumentException if the limit is negative or exceeds the maximum
         */
        public Builder limit(int limit) {
            if (limit >= QueryContext.MAXIMUM_LIMIT) {
                throw new IllegalArgumentException("Maximum limit value is " + QueryContext.MAXIMUM_LIMIT);
            }
            if (limit < 0) {
                throw new IllegalArgumentException("limit must be a non-negative integer");
            }
            this.limit = limit;
            return this;
        }

        /**
         * Sets the field name to use for in-memory result sorting.
         *
         * @param resultSortField the field name to sort results by
         * @return this Builder instance for method chaining
         */
        public Builder resultSortField(String resultSortField) {
            this.resultSortField = resultSortField;
            return this;
        }

        /**
         * Sets the sort direction for in-memory result sorting.
         *
         * @param resultSortDirection the sort direction
         * @return this Builder instance for method chaining
         */
        public Builder resultSortDirection(SortDirection resultSortDirection) {
            this.resultSortDirection = resultSortDirection;
            return this;
        }

        public Builder update(UpdateOptions update) {
            this.update = update;
            return this;
        }

        public Builder collation(Collation collation) {
            this.collation = collation;
            return this;
        }

        /**
         * Builds and returns an immutable QueryOptions instance with the configured values.
         *
         * @return a new QueryOptions instance
         */
        public QueryOptions build() {
            return new QueryOptions(this);
        }
    }
}
