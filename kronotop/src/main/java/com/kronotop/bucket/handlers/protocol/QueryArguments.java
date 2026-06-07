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

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.bucket.Collation;

/**
 * Holds optional query arguments parsed from bucket commands (QUERY, DELETE, UPDATE).
 * All fields are optional and populated during protocol message parsing.
 *
 * <p>Supported arguments:</p>
 * <ul>
 *   <li>LIMIT n - maximum documents to return per batch</li>
 *   <li>SORTBY field ASC|DESC - sort field and direction</li>
 * </ul>
 */
public class QueryArguments {
    private int limit;
    private String sortBy;
    private SortDirection sortDirection;
    private String resultSortBy;
    private SortDirection resultSortDirection;
    private byte[] projection;
    private Collation collation;

    /**
     * Returns the result limit, or 0 if not specified.
     */
    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    /**
     * Returns the sort field name, or null if not specified.
     */
    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    /**
     * Returns the sort direction (ASC or DESC), or null if not specified.
     */
    public SortDirection getSortDirection() {
        return sortDirection;
    }

    public void setSortDirection(SortDirection sortDirection) {
        this.sortDirection = sortDirection;
    }

    /**
     * Returns the result sort field name, or null if not specified.
     */
    public String getResultSortBy() {
        return resultSortBy;
    }

    public void setResultSortBy(String resultSortBy) {
        this.resultSortBy = resultSortBy;
    }

    /**
     * Returns the result sort direction (ASC or DESC), or null if not specified.
     */
    public SortDirection getResultSortDirection() {
        return resultSortDirection;
    }

    public void setResultSortDirection(SortDirection resultSortDirection) {
        this.resultSortDirection = resultSortDirection;
    }

    /**
     * Returns the projection specification bytes, or null if not specified.
     */
    public byte[] getProjection() {
        return projection;
    }

    public void setProjection(byte[] projection) {
        this.projection = projection;
    }

    public Collation getCollation() {
        return collation;
    }

    public void setCollation(Collation collation) {
        this.collation = collation;
    }
}
