package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.DefaultIndexDefinition;

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
 * // Query with reverse ordering and limit
 * QueryOptions reversed = QueryOptions.builder()
 *     .reverse(true)
 *     .limit(20)
 *     .build();
 * }</pre>
 *
 * @see QueryContext
 * @see DefaultIndexDefinition
 * @since 1.0
 */
public class QueryOptions {
    /**
     * Whether results should be returned in reverse order.
     */
    private final boolean reverse;

    /**
     * The field name to use for sorting results. Defaults to document ID.
     */
    private final String sortByField;

    /**
     * Maximum number of documents to return in a single batch.
     */
    private final int limit;

    /**
     * Private constructor used by the Builder pattern.
     *
     * @param builder the Builder instance containing the configuration values
     */
    private QueryOptions(Builder builder) {
        this.reverse = builder.reverse;
        this.sortByField = builder.sortByField;
        this.limit = builder.limit;
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
     * When true, results are sorted in descending order by the sort field.
     *
     * @return true if reverse ordering is enabled, false otherwise
     */
    public boolean isReverse() {
        return reverse;
    }

    /**
     * Returns the field name used for sorting results.
     * Defaults to the document ID field if not specified.
     *
     * @return the sort field name
     */
    public String getSortByField() {
        return sortByField;
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
     * Builder class for constructing QueryOptions instances using the builder pattern.
     * Provides a fluent API for configuring query options with method chaining.
     *
     * <p>All builder methods return the Builder instance to allow method chaining.
     * Call {@link #build()} to create the final immutable QueryOptions instance.
     */
    public static class Builder {
        /**
         * Whether to enable reverse ordering. Defaults to false.
         */
        private boolean reverse = false;

        /**
         * Field name for sorting. Defaults to document ID.
         */
        private String sortByField = DefaultIndexDefinition.ID.selector();

        /**
         * Result limit. Defaults to {@value QueryContext#DEFAULT_LIMIT}.
         */
        private int limit = QueryContext.DEFAULT_LIMIT;

        /**
         * Sets whether results should be returned in reverse (descending) order.
         *
         * @param reverse true for descending order, false for ascending order (default)
         * @return this Builder instance for method chaining
         */
        public Builder reverse(boolean reverse) {
            this.reverse = reverse;
            return this;
        }

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
         * Sets the maximum number of documents to return in a single batch.
         * This controls pagination and memory usage during query execution.
         *
         * @param limit the maximum result count (0 to {@value QueryContext#MAXIMUM_LIMIT})
         * @return this Builder instance for method chaining
         * @throws IllegalArgumentException if limit is negative or exceeds maximum
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
         * Builds and returns an immutable QueryOptions instance with the configured values.
         *
         * @return a new QueryOptions instance
         */
        public QueryOptions build() {
            return new QueryOptions(this);
        }
    }
}
