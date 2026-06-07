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

package com.kronotop.bucket.bql;

import com.kronotop.bucket.bql.ast.BqlExpr;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Simple benchmark for QueryShape.compute() performance.
 * Run with: mvn test -Dtest=QueryShapeBenchmark
 */
@Disabled
class QueryShapeBenchmark {

    private static final int WARMUP_ITERATIONS = 10_000;
    private static final int BENCHMARK_ITERATIONS = 1_000_000;

    @Test
    void benchmarkSimpleEquality() {
        BqlExpr expr = BqlParser.parse("{\"age\": 25}");
        runBenchmark("Simple equality", expr);
    }

    @Test
    void benchmarkComparisonOperator() {
        BqlExpr expr = BqlParser.parse("{\"age\": {\"$gte\": 18}}");
        runBenchmark("Comparison ($gte)", expr);
    }

    @Test
    void benchmarkInOperator() {
        BqlExpr expr = BqlParser.parse("{\"status\": {\"$in\": [1, 2, 3, 4, 5]}}");
        runBenchmark("$in with 5 values", expr);
    }

    @Test
    void benchmarkAndWithTwoFields() {
        BqlExpr expr = BqlParser.parse("{\"age\": {\"$gte\": 18}, \"status\": \"active\"}");
        runBenchmark("$and with 2 fields", expr);
    }

    @Test
    void benchmarkAndWithFiveFields() {
        BqlExpr expr = BqlParser.parse("{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5}");
        runBenchmark("$and with 5 fields", expr);
    }

    @Test
    void benchmarkNestedAndOr() {
        BqlExpr expr = BqlParser.parse(
                "{\"$or\": [{\"$and\": [{\"a\": 1}, {\"b\": 2}]}, {\"$and\": [{\"c\": 3}, {\"d\": 4}]}]}"
        );
        runBenchmark("Nested $or/$and", expr);
    }

    @Test
    void benchmarkElemMatch() {
        BqlExpr expr = BqlParser.parse("{\"scores\": {\"$elemMatch\": {\"$gte\": 80, \"$lte\": 100}}}");
        runBenchmark("$elemMatch", expr);
    }

    @Test
    void benchmarkComplexQuery() {
        BqlExpr expr = BqlParser.parse(
                "{\"$and\": [" +
                        "{\"age\": {\"$gte\": 18}}," +
                        "{\"status\": {\"$in\": [\"active\", \"pending\"]}}," +
                        "{\"tags\": {\"$all\": [\"premium\", \"verified\"]}}," +
                        "{\"score\": {\"$not\": {\"$lt\": 50}}}" +
                        "]}"
        );
        runBenchmark("Complex query", expr);
    }

    private void runBenchmark(String name, BqlExpr expr) {
        // Warmup
        long blackhole = 0;
        for (int i = 0; i < WARMUP_ITERATIONS; i++) {
            blackhole += QueryShape.compute(expr);
        }

        // Benchmark
        long startTime = System.nanoTime();
        for (int i = 0; i < BENCHMARK_ITERATIONS; i++) {
            blackhole += QueryShape.compute(expr);
        }
        long endTime = System.nanoTime();

        // Prevent dead code elimination
        if (blackhole == Long.MIN_VALUE) {
            System.out.println("Never happens");
        }

        long totalNanos = endTime - startTime;
        double avgNanos = (double) totalNanos / BENCHMARK_ITERATIONS;
        double opsPerSecond = 1_000_000_000.0 / avgNanos;

        System.out.printf("%-20s: %8.1f ns/op, %,.0f ops/sec%n", name, avgNanos, opsPerSecond);
    }
}
