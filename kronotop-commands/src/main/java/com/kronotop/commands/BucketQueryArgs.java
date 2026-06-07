/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.commands;

import io.lettuce.core.protocol.CommandArgs;

public class BucketQueryArgs {
    private int limit;
    private String sortBy;
    private String sortDirection;
    private String resultSortBy;
    private String resultSortDirection;
    private String projection;
    private String collation;

    public BucketQueryArgs limit(int limit) {
        this.limit = limit;
        return this;
    }

    public BucketQueryArgs sortBy(String field, String direction) {
        this.sortBy = field;
        this.sortDirection = direction;
        return this;
    }

    public BucketQueryArgs resultSort(String field, String direction) {
        this.resultSortBy = field;
        this.resultSortDirection = direction;
        return this;
    }

    public BucketQueryArgs projection(String projection) {
        this.projection = projection;
        return this;
    }

    public BucketQueryArgs collation(String collation) {
        this.collation = collation;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (limit > 0) {
            args.add("LIMIT");
            args.add(limit);
        }

        if (sortBy != null) {
            args.add("SORTBY");
            args.add(sortBy);
            args.add(sortDirection);
        }

        if (resultSortBy != null) {
            args.add("RESULTSORT");
            args.add(resultSortBy);
            args.add(resultSortDirection);
        }

        if (projection != null) {
            args.add("PROJECTION");
            args.add(projection);
        }

        if (collation != null) {
            args.add("COLLATION");
            args.add(collation);
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static BucketQueryArgs limit(int limit) {
            return new BucketQueryArgs().limit(limit);
        }

        public static BucketQueryArgs sortBy(String field, String direction) {
            return new BucketQueryArgs().sortBy(field, direction);
        }

        public static BucketQueryArgs resultSort(String field, String direction) {
            return new BucketQueryArgs().resultSort(field, direction);
        }

        public static BucketQueryArgs projection(String projection) {
            return new BucketQueryArgs().projection(projection);
        }

        public static BucketQueryArgs collation(String collation) {
            return new BucketQueryArgs().collation(collation);
        }
    }
}