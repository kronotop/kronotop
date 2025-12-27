/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.kronotop.BaseStandaloneInstanceTest;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.statistics.Histogram;
import com.kronotop.bucket.index.statistics.HistogramUtils;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PlannerContext;
import org.bson.BsonInt32;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

import com.kronotop.bucket.bql.ast.Int64Val;

class SelectivityEstimatorTest extends BaseStandaloneInstanceTest {

    private Histogram buildHistogramWithRange(int start, int end) {
        TreeSet<BsonValue> values = new TreeSet<>(BSONUtil::compareBsonValues);
        for (int i = start; i <= end; i++) {
            values.add(new BsonInt32(i));
        }
        return HistogramUtils.buildHistogram(values);
    }

    @Test
    void shouldSelectLowerValueForLT() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two IndexScanNodes with LT predicates:
        // age < 10 → ~10% percentile → estimation = 0.10 * 1000 = ~100 rows
        // score < 90 → ~90% percentile → estimation = 0.90 * 1000 = ~900 rows
        IndexScanPredicate lowValuePredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(10));
        IndexScanNode lowValueNode = new IndexScanNode(1, ageIndex, lowValuePredicate);

        IndexScanPredicate highValuePredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(90));
        IndexScanNode highValueNode = new IndexScanNode(2, scoreIndex, highValuePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(lowValueNode, highValueNode));

        // Lower value (< 10) should be selected: lower percentile = fewer estimated rows
        assertSame(lowValueNode, selected);
    }

    @Test
    void shouldSelectHigherValueForGT() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two IndexScanNodes with GT predicates:
        // age > 90 → ~90% percentile → estimation = (1 - 0.90) * 1000 = ~100 rows
        // score > 10 → ~10% percentile → estimation = (1 - 0.10) * 1000 = ~900 rows
        IndexScanPredicate highValuePredicate = new IndexScanPredicate(1, "age", Operator.GT, new Int32Val(90));
        IndexScanNode highValueNode = new IndexScanNode(1, ageIndex, highValuePredicate);

        IndexScanPredicate lowValuePredicate = new IndexScanPredicate(2, "score", Operator.GT, new Int32Val(10));
        IndexScanNode lowValueNode = new IndexScanNode(2, scoreIndex, lowValuePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(highValueNode, lowValueNode));

        // Higher value (> 90) should be selected: higher percentile = smaller (1-percentile) = fewer rows
        assertSame(highValueNode, selected);
    }

    @Test
    void shouldPreferLTOverGTWhenMoreSelective() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Compare LT vs GT at same value (20):
        // age < 20 → ~20% percentile → estimation = 0.20 * 1000 = ~200 rows
        // score > 20 → ~20% percentile → estimation = (1 - 0.20) * 1000 = ~800 rows
        IndexScanPredicate ltPredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(20));
        IndexScanNode ltNode = new IndexScanNode(1, ageIndex, ltPredicate);

        IndexScanPredicate gtPredicate = new IndexScanPredicate(2, "score", Operator.GT, new Int32Val(20));
        IndexScanNode gtNode = new IndexScanNode(2, scoreIndex, gtPredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ltNode, gtNode));

        // LT (< 20) should be selected: 20% < 80%
        assertSame(ltNode, selected);
    }

    @Test
    void shouldPreferGTOverLTWhenMoreSelective() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Compare LT vs GT at same value (80):
        // age < 80 → ~80% percentile → estimation = 0.80 * 1000 = ~800 rows
        // score > 80 → ~80% percentile → estimation = (1 - 0.80) * 1000 = ~200 rows
        IndexScanPredicate ltPredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(80));
        IndexScanNode ltNode = new IndexScanNode(1, ageIndex, ltPredicate);

        IndexScanPredicate gtPredicate = new IndexScanPredicate(2, "score", Operator.GT, new Int32Val(80));
        IndexScanNode gtNode = new IndexScanNode(2, scoreIndex, gtPredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ltNode, gtNode));

        // GT (> 80) should be selected: 20% < 80%
        assertSame(gtNode, selected);
    }

    @Test
    void shouldClampLowPercentileToEpsilon() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 50-100 (not starting from 1)
        Histogram histogram = buildHistogramWithRange(50, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two IndexScanNodes with LT predicates:
        // age < 10 → below histogram range → percentile clamped to EPSILON (0.01)
        //          → estimation = 0.01 * 1000 = 10 rows
        // score < 75 → ~50% percentile → estimation = 0.50 * 1000 = 500 rows
        IndexScanPredicate belowRangePredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(10));
        IndexScanNode belowRangeNode = new IndexScanNode(1, ageIndex, belowRangePredicate);

        IndexScanPredicate inRangePredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(75));
        IndexScanNode inRangeNode = new IndexScanNode(2, scoreIndex, inRangePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(belowRangeNode, inRangeNode));

        // Below-range value should be selected: clamped EPSILON (1%) < 50%
        assertSame(belowRangeNode, selected);
    }

    @Test
    void shouldClampHighPercentileToOneMinusEpsilon() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-50 (not going to 100)
        Histogram histogram = buildHistogramWithRange(1, 50);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two IndexScanNodes with GT predicates:
        // age > 100 → above histogram range → percentile clamped to 0.99
        //           → estimation = (1 - 0.99) * 1000 = 10 rows
        // score > 25 → ~50% percentile → estimation = (1 - 0.50) * 1000 = 500 rows
        IndexScanPredicate aboveRangePredicate = new IndexScanPredicate(1, "age", Operator.GT, new Int32Val(100));
        IndexScanNode aboveRangeNode = new IndexScanNode(1, ageIndex, aboveRangePredicate);

        IndexScanPredicate inRangePredicate = new IndexScanPredicate(2, "score", Operator.GT, new Int32Val(25));
        IndexScanNode inRangeNode = new IndexScanNode(2, scoreIndex, inRangePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(aboveRangeNode, inRangeNode));

        // Above-range value should be selected: (1 - 0.99) = 1% < 50%
        assertSame(aboveRangeNode, selected);
    }

    @Test
    void shouldHandleEqualSelectivity() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build identical histogram for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject identical statistics for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two IndexScanNodes with identical predicates (same operator, same value):
        // age < 50 → ~50% percentile → estimation = 0.50 * 1000 = 500 rows
        // score < 50 → ~50% percentile → estimation = 0.50 * 1000 = 500 rows
        IndexScanPredicate agePredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(50));
        IndexScanNode ageNode = new IndexScanNode(1, ageIndex, agePredicate);

        IndexScanPredicate scorePredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(50));
        IndexScanNode scoreNode = new IndexScanNode(2, scoreIndex, scorePredicate);

        // Run SelectivityEstimator with ageNode first
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ageNode, scoreNode));

        // When selectivity is equal, first node in list order should be selected
        assertSame(ageNode, selected);

        // Verify order matters: swap the order
        PipelineNode selectedReversed = SelectivityEstimator.estimate(ctx, List.of(scoreNode, ageNode));
        assertSame(scoreNode, selectedReversed);
    }

    // ==================== RangeScan Tests ====================

    @Test
    void shouldSelectNarrowerRangeForRangeScan() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two RangeScanNodes:
        // age: 40-60 → ~40% to ~60% → (0.60 - 0.40) * 1000 = ~200 rows
        // score: 20-80 → ~20% to ~80% → (0.80 - 0.20) * 1000 = ~600 rows
        RangeScanPredicate narrowPredicate = new RangeScanPredicate(
                "age", new Int32Val(40), new Int32Val(60), true, true);
        RangeScanNode narrowNode = new RangeScanNode(1, ageIndex, narrowPredicate);

        RangeScanPredicate widePredicate = new RangeScanPredicate(
                "score", new Int32Val(20), new Int32Val(80), true, true);
        RangeScanNode wideNode = new RangeScanNode(2, scoreIndex, widePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(narrowNode, wideNode));

        // Narrower range (40-60) should be selected: 20% width < 60% width
        assertSame(narrowNode, selected);
    }

    @Test
    void shouldSelectRangeWithSameWidthByListOrder() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create two RangeScanNodes with identical ranges on different indexes:
        // age: 40-60 → same percentiles → same estimation
        // score: 40-60 → same percentiles → same estimation
        RangeScanPredicate agePredicate = new RangeScanPredicate(
                "age", new Int32Val(40), new Int32Val(60), true, true);
        RangeScanNode ageNode = new RangeScanNode(1, ageIndex, agePredicate);

        RangeScanPredicate scorePredicate = new RangeScanPredicate(
                "score", new Int32Val(40), new Int32Val(60), true, true);
        RangeScanNode scoreNode = new RangeScanNode(2, scoreIndex, scorePredicate);

        // Run SelectivityEstimator with ageNode first
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ageNode, scoreNode));

        // Same range = same selectivity, first in list should win
        assertSame(ageNode, selected);

        // Verify order matters: swap the order
        PipelineNode selectedReversed = SelectivityEstimator.estimate(ctx, List.of(scoreNode, ageNode));
        assertSame(scoreNode, selectedReversed);
    }

    @Test
    void shouldReturnUnknownWhenUpperLessThanLower() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create inverted range (upper < lower) → returns UNKNOWN
        // age: 60-40 (inverted) → high percentile < low percentile → UNKNOWN
        RangeScanPredicate invertedPredicate = new RangeScanPredicate(
                "age", new Int32Val(60), new Int32Val(40), true, true);
        RangeScanNode invertedNode = new RangeScanNode(1, ageIndex, invertedPredicate);

        // Create valid range for comparison
        // score: 40-60 → valid range → produces finite estimate
        RangeScanPredicate validPredicate = new RangeScanPredicate(
                "score", new Int32Val(40), new Int32Val(60), true, true);
        RangeScanNode validNode = new RangeScanNode(2, scoreIndex, validPredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(invertedNode, validNode));

        // Valid range should be selected (UNKNOWN = POSITIVE_INFINITY is worst)
        assertSame(validNode, selected);
    }

    @Test
    void shouldClampLowerBoundBelowHistogramRange() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 50-100 (not starting from 1)
        Histogram histogram = buildHistogramWithRange(50, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create range with lower bound below histogram range:
        // age: 10-75 → lower clamped to EPSILON (0.01), upper ~50%
        //            → (0.50 - 0.01) * 1000 = ~490 rows
        RangeScanPredicate belowRangePredicate = new RangeScanPredicate(
                "age", new Int32Val(10), new Int32Val(75), true, true);
        RangeScanNode belowRangeNode = new RangeScanNode(1, ageIndex, belowRangePredicate);

        // Create range with lower bound inside histogram range:
        // score: 60-75 → lower ~20%, upper ~50%
        //              → (0.50 - 0.20) * 1000 = ~300 rows
        RangeScanPredicate inRangePredicate = new RangeScanPredicate(
                "score", new Int32Val(60), new Int32Val(75), true, true);
        RangeScanNode inRangeNode = new RangeScanNode(2, scoreIndex, inRangePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(belowRangeNode, inRangeNode));

        // In-range lower bound produces narrower effective range, so more selective
        assertSame(inRangeNode, selected);
    }

    @Test
    void shouldClampUpperBoundAboveHistogramRange() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-50 (not going to 100)
        Histogram histogram = buildHistogramWithRange(1, 50);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create range with upper bound above histogram range:
        // age: 25-150 → lower ~50%, upper clamped to 0.99
        //             → (0.99 - 0.50) * 1000 = ~490 rows
        RangeScanPredicate aboveRangePredicate = new RangeScanPredicate(
                "age", new Int32Val(25), new Int32Val(150), true, true);
        RangeScanNode aboveRangeNode = new RangeScanNode(1, ageIndex, aboveRangePredicate);

        // Create range with upper bound inside histogram range:
        // score: 25-40 → lower ~50%, upper ~80%
        //              → (0.80 - 0.50) * 1000 = ~300 rows
        RangeScanPredicate inRangePredicate = new RangeScanPredicate(
                "score", new Int32Val(25), new Int32Val(40), true, true);
        RangeScanNode inRangeNode = new RangeScanNode(2, scoreIndex, inRangePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(aboveRangeNode, inRangeNode));

        // In-range upper bound produces narrower effective range, so more selective
        assertSame(inRangeNode, selected);
    }

    @Test
    void shouldPreferRangeScanOverIndexScanWhenMoreSelective() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create RangeScan with narrow range:
        // age: 45-55 → ~45% to ~55% → (0.55 - 0.45) * 1000 = ~100 rows
        RangeScanPredicate rangePredicate = new RangeScanPredicate(
                "age", new Int32Val(45), new Int32Val(55), true, true);
        RangeScanNode rangeNode = new RangeScanNode(1, ageIndex, rangePredicate);

        // Create IndexScan with wider coverage:
        // score < 50 → ~50% percentile → 0.50 * 1000 = ~500 rows
        IndexScanPredicate indexPredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(50));
        IndexScanNode indexNode = new IndexScanNode(2, scoreIndex, indexPredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(rangeNode, indexNode));

        // RangeScan (10% width) should be selected over IndexScan (50%)
        assertSame(rangeNode, selected);
    }

    @Test
    void shouldPreferIndexScanOverRangeScanWhenMoreSelective() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram with values 1-100 for both indexes
        Histogram histogram = buildHistogramWithRange(1, 100);

        // Inject statistics: cardinality=1000 for both indexes
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create IndexScan with narrow coverage:
        // age < 10 → ~10% percentile → 0.10 * 1000 = ~100 rows
        IndexScanPredicate indexPredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(10));
        IndexScanNode indexNode = new IndexScanNode(1, ageIndex, indexPredicate);

        // Create RangeScan with wide range:
        // score: 20-80 → ~20% to ~80% → (0.80 - 0.20) * 1000 = ~600 rows
        RangeScanPredicate rangePredicate = new RangeScanPredicate(
                "score", new Int32Val(20), new Int32Val(80), true, true);
        RangeScanNode rangeNode = new RangeScanNode(2, scoreIndex, rangePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(indexNode, rangeNode));

        // IndexScan (10%) should be selected over RangeScan (60%)
        assertSame(indexNode, selected);
    }

    // ==================== Input Validation Tests ====================

    @Test
    void shouldThrowExceptionWhenChildrenIsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                SelectivityEstimator.estimate(null, null)
        );
    }

    @Test
    void shouldThrowExceptionWhenChildrenIsEmpty() {
        assertThrows(IllegalArgumentException.class, () ->
                SelectivityEstimator.estimate(null, new ArrayList<>())
        );
    }

    // ==================== Missing Statistics Tests ====================

    @Test
    void shouldSelectNodeWithStatsOverNodeWithoutStats() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram and inject stats only for scoreIndex
        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        // ageIndex has NO statistics (will return UNKNOWN)
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create predicates
        IndexScanPredicate agePredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(50));
        IndexScanNode ageNode = new IndexScanNode(1, ageIndex, agePredicate);

        IndexScanPredicate scorePredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(50));
        IndexScanNode scoreNode = new IndexScanNode(2, scoreIndex, scorePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ageNode, scoreNode));

        // scoreNode with stats should be selected over ageNode without stats (UNKNOWN)
        assertSame(scoreNode, selected);
    }

    @Test
    void shouldReturnUnknownWhenHistogramIsNull() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // Build histogram only for scoreIndex, ageIndex has stats with null histogram
        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, null)); // null histogram
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Create predicates
        IndexScanPredicate agePredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(50));
        IndexScanNode ageNode = new IndexScanNode(1, ageIndex, agePredicate);

        IndexScanPredicate scorePredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(50));
        IndexScanNode scoreNode = new IndexScanNode(2, scoreIndex, scorePredicate);

        // Run SelectivityEstimator
        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ageNode, scoreNode));

        // scoreNode with valid histogram should be selected
        assertSame(scoreNode, selected);
    }

    // ==================== Operator Coverage Tests ====================

    @Test
    void shouldEstimateForLTEOperator() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // age <= 10 → ~10% → ~100 rows
        // score <= 90 → ~90% → ~900 rows
        IndexScanPredicate lowPredicate = new IndexScanPredicate(1, "age", Operator.LTE, new Int32Val(10));
        IndexScanNode lowNode = new IndexScanNode(1, ageIndex, lowPredicate);

        IndexScanPredicate highPredicate = new IndexScanPredicate(2, "score", Operator.LTE, new Int32Val(90));
        IndexScanNode highNode = new IndexScanNode(2, scoreIndex, highPredicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(lowNode, highNode));

        assertSame(lowNode, selected);
    }

    @Test
    void shouldEstimateForGTEOperator() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // age >= 90 → (1 - 0.90) * 1000 = ~100 rows
        // score >= 10 → (1 - 0.10) * 1000 = ~900 rows
        IndexScanPredicate highPredicate = new IndexScanPredicate(1, "age", Operator.GTE, new Int32Val(90));
        IndexScanNode highNode = new IndexScanNode(1, ageIndex, highPredicate);

        IndexScanPredicate lowPredicate = new IndexScanPredicate(2, "score", Operator.GTE, new Int32Val(10));
        IndexScanNode lowNode = new IndexScanNode(2, scoreIndex, lowPredicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(highNode, lowNode));

        assertSame(highNode, selected);
    }

    @Test
    void shouldReturnUnknownForEQOperator() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // EQ operator returns UNKNOWN
        IndexScanPredicate eqPredicate = new IndexScanPredicate(1, "age", Operator.EQ, new Int32Val(50));
        IndexScanNode eqNode = new IndexScanNode(1, ageIndex, eqPredicate);

        // LT operator has valid estimation
        IndexScanPredicate ltPredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(50));
        IndexScanNode ltNode = new IndexScanNode(2, scoreIndex, ltPredicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(eqNode, ltNode));

        // LT node should be selected (EQ returns UNKNOWN = infinity)
        assertSame(ltNode, selected);
    }

    @Test
    void shouldReturnUnknownForNEOperator() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // NE operator returns UNKNOWN
        IndexScanPredicate nePredicate = new IndexScanPredicate(1, "age", Operator.NE, new Int32Val(50));
        IndexScanNode neNode = new IndexScanNode(1, ageIndex, nePredicate);

        // GT operator has valid estimation
        IndexScanPredicate gtPredicate = new IndexScanPredicate(2, "score", Operator.GT, new Int32Val(90));
        IndexScanNode gtNode = new IndexScanNode(2, scoreIndex, gtPredicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(neNode, gtNode));

        // GT node should be selected (NE returns UNKNOWN = infinity)
        assertSame(gtNode, selected);
    }

    // ==================== Type Mismatch Tests ====================

    @Test
    void shouldReturnUnknownWhenValueTypeDoesNotMatchIndex() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        stats.put(scoreIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        // Int64Val with INT32 index → type mismatch → UNKNOWN
        IndexScanPredicate mismatchPredicate = new IndexScanPredicate(1, "age", Operator.LT, new Int64Val(50L));
        IndexScanNode mismatchNode = new IndexScanNode(1, ageIndex, mismatchPredicate);

        // Int32Val with INT32 index → valid
        IndexScanPredicate validPredicate = new IndexScanPredicate(2, "score", Operator.LT, new Int32Val(50));
        IndexScanNode validNode = new IndexScanNode(2, scoreIndex, validPredicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(mismatchNode, validNode));

        // Valid node should be selected (type mismatch returns UNKNOWN)
        assertSame(validNode, selected);
    }

    // ==================== Single/Edge Node Tests ====================

    @Test
    void shouldReturnSingleNodeWhenOnlyOneProvided() {
        // Create bucket and index
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        Histogram histogram = buildHistogramWithRange(1, 100);
        Map<Long, IndexStatistics> stats = new HashMap<>();
        stats.put(ageIndex.id(), new IndexStatistics(1000, histogram));
        metadata.indexes().updateStatistics(stats);

        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.LT, new Int32Val(50));
        IndexScanNode node = new IndexScanNode(1, ageIndex, predicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(node));

        assertSame(node, selected);
    }

    @Test
    void shouldReturnFirstNodeWhenAllHaveUnknownSelectivity() {
        // Create bucket and indexes
        IndexDefinition ageIndex = IndexDefinition.create("age-index", "age", BsonType.INT32);
        IndexDefinition scoreIndex = IndexDefinition.create("score-index", "score", BsonType.INT32);
        createIndexThenWaitForReadiness(ageIndex, scoreIndex);
        BucketMetadata metadata = refreshBucketMetadata(TEST_NAMESPACE, TEST_BUCKET);

        // No statistics injected → all nodes return UNKNOWN

        // Both use EQ operator which returns UNKNOWN
        IndexScanPredicate agePredicate = new IndexScanPredicate(1, "age", Operator.EQ, new Int32Val(50));
        IndexScanNode ageNode = new IndexScanNode(1, ageIndex, agePredicate);

        IndexScanPredicate scorePredicate = new IndexScanPredicate(2, "score", Operator.EQ, new Int32Val(50));
        IndexScanNode scoreNode = new IndexScanNode(2, scoreIndex, scorePredicate);

        PlannerContext ctx = new PlannerContext(metadata);
        PipelineNode selected = SelectivityEstimator.estimate(ctx, List.of(ageNode, scoreNode));

        // First node should be selected when all have UNKNOWN (equal) selectivity
        assertSame(ageNode, selected);

        // Verify order matters
        PipelineNode selectedReversed = SelectivityEstimator.estimate(ctx, List.of(scoreNode, ageNode));
        assertSame(scoreNode, selectedReversed);
    }
}