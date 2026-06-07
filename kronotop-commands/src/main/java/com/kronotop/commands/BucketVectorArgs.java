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

public class BucketVectorArgs {
    private Integer top;
    private String filter;
    private Float threshold;
    private Integer maxScanCandidates;
    private Float overquery;
    private String projection;

    public BucketVectorArgs top(int top) {
        this.top = top;
        return this;
    }

    public BucketVectorArgs filter(String filter) {
        this.filter = filter;
        return this;
    }

    public BucketVectorArgs threshold(float threshold) {
        this.threshold = threshold;
        return this;
    }

    public BucketVectorArgs maxScanCandidates(int maxScanCandidates) {
        this.maxScanCandidates = maxScanCandidates;
        return this;
    }

    public BucketVectorArgs overquery(float overquery) {
        this.overquery = overquery;
        return this;
    }

    public BucketVectorArgs projection(String projection) {
        this.projection = projection;
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (top != null) {
            args.add("TOP");
            args.add(top);
        }
        if (filter != null) {
            args.add("FILTER");
            args.add(filter);
        }
        if (threshold != null) {
            args.add("THRESHOLD");
            args.add(threshold.toString());
        }
        if (maxScanCandidates != null) {
            args.add("MAX-SCAN-CANDIDATES");
            args.add(maxScanCandidates);
        }
        if (overquery != null) {
            args.add("OVERQUERY");
            args.add(overquery.toString());
        }
        if (projection != null) {
            args.add("PROJECTION");
            args.add(projection);
        }
    }

    public static class Builder {
        private Builder() {
        }

        public static BucketVectorArgs top(int top) {
            return new BucketVectorArgs().top(top);
        }

        public static BucketVectorArgs filter(String filter) {
            return new BucketVectorArgs().filter(filter);
        }

        public static BucketVectorArgs threshold(float threshold) {
            return new BucketVectorArgs().threshold(threshold);
        }

        public static BucketVectorArgs maxScanCandidates(int maxScanCandidates) {
            return new BucketVectorArgs().maxScanCandidates(maxScanCandidates);
        }

        public static BucketVectorArgs overquery(float overquery) {
            return new BucketVectorArgs().overquery(overquery);
        }

        public static BucketVectorArgs projection(String projection) {
            return new BucketVectorArgs().projection(projection);
        }
    }
}
