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

package com.kronotop.benchmark;

import com.kronotop.benchmark.bucket.InsertBenchmark;
import com.kronotop.benchmark.bucket.MixedBenchmark;
import com.kronotop.benchmark.bucket.QueryBenchmark;
import com.kronotop.benchmark.vector.HnmBenchmark;
import com.kronotop.benchmark.vector.TweetsBenchmark;

import java.util.Arrays;

/**
 * Entry point that routes to the appropriate benchmark based on positional subcommands.
 * <p>
 * Usage: kronotop-benchmark &lt;group&gt; &lt;subcommand&gt; [options]
 * <p>
 * Groups:
 * vector tweets   -- US Senator Tweets benchmark (384-dim)
 * vector hnm      -- H&amp;M fashion benchmark (2048-dim)
 * bucket insert     -- Bucket insert benchmark (docs/sec, latency percentiles)
 * bucket query      -- Bucket query benchmark (QPS, latency percentiles across 5 scenarios)
 * bucket mixed      -- Mixed workload benchmark (QUERY/INSERT/UPDATE/DELETE)
 */
public class BenchmarkMain {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            printUsage();
            System.exit(1);
        }

        String group = args[0];
        String sub = args[1];
        String[] forwarded = Arrays.copyOfRange(args, 2, args.length);

        switch (group) {
            case "vector" -> {
                switch (sub) {
                    case "tweets" -> TweetsBenchmark.main(forwarded);
                    case "hnm" -> HnmBenchmark.main(forwarded);
                    default -> {
                        System.err.println("Unknown vector benchmark: " + sub);
                        System.err.println("Available: tweets, hnm");
                        System.exit(1);
                    }
                }
            }
            case "bucket" -> {
                switch (sub) {
                    case "insert" -> InsertBenchmark.main(forwarded);
                    case "query" -> QueryBenchmark.main(forwarded);
                    case "mixed" -> MixedBenchmark.main(forwarded);
                    default -> {
                        System.err.println("Unknown bucket benchmark: " + sub);
                        System.err.println("Available: insert, query, mixed");
                        System.exit(1);
                    }
                }
            }
            default -> {
                System.err.println("Unknown command group: " + group);
                System.err.println("Available groups: vector, bucket");
                System.exit(1);
            }
        }
    }

    private static void printUsage() {
        System.err.println("Usage: kronotop-benchmark <group> <subcommand> [options]");
        System.err.println("  vector tweets   -- US Senator Tweets benchmark (384-dim)");
        System.err.println("  vector hnm      -- H&M fashion benchmark (2048-dim)");
        System.err.println("  bucket insert     -- Bucket insert benchmark (docs/sec, latency percentiles)");
        System.err.println("  bucket query      -- Bucket query benchmark (QPS, latency percentiles)");
        System.err.println("  bucket mixed      -- Mixed workload benchmark (QUERY/INSERT/UPDATE/DELETE)");
    }
}
