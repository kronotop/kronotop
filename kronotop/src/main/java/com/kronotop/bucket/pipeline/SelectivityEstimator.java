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
package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.IndexStatistics;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.index.statistics.Histogram;
import com.kronotop.bucket.index.statistics.HistogramUtil;
import com.kronotop.bucket.pipeline.CompoundIndexScanNode.CompoundIndexScanFilter;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.PlannerContext;
import org.bson.BsonBinary;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Estimates index selectivity to choose the most efficient index for query execution.
 * <p>
 * Uses histogram-based percentile estimation to predict how many documents each index
 * will return. The index with the lowest estimated result count is selected.
 * <p>
 * Selectivity formulas:
 * <ul>
 *   <li>LT/LTE operators: {@code percentile × cardinality}</li>
 *   <li>GT/GTE operators: {@code (1 - percentile) × cardinality}</li>
 *   <li>Range scans: {@code (upperPercentile - lowerPercentile) × cardinality}</li>
 * </ul>
 * <p>
 * Percentiles are clamped to [{@code EPSILON}, {@code 1-EPSILON}] to avoid zero or
 * infinite estimates. When statistics are unavailable, returns {@code UNKNOWN} (infinity)
 * to deprioritize that index.
 */
public class SelectivityEstimator {

    private static final double UNKNOWN = Double.POSITIVE_INFINITY;
    private static final double EPSILON = 0.01;

    /**
     * Selects the most selective index from candidate scan nodes.
     *
     * @param ctx         planner context containing index metadata and statistics
     * @param pipelineCtx pipeline context containing parameters for operand resolution
     * @param children    candidate scan nodes (IndexScanNode, RangeScanNode, or CompoundIndexScanNode)
     * @return the scan node with lowest estimated result count
     * @throws IllegalArgumentException if children is null or empty
     */
    public static PipelineNode estimate(PlannerContext ctx, PipelineContext pipelineCtx, List<PipelineNode> children) {
        if (children == null || children.isEmpty()) {
            throw new IllegalArgumentException("children cannot be null or empty");
        }

        List<BqlValue> parameters = pipelineCtx.isParameterized() ? pipelineCtx.getParameters() : Collections.emptyList();
        List<IndexSelectivity> estimates = new ArrayList<>();

        for (int i = 0; i < children.size(); i++) {
            PipelineNode child = children.get(i);

            if (child instanceof IndexScanNode node) {
                estimates.add(
                        estimateIndexScan(ctx, node, i, parameters)
                );
            } else if (child instanceof RangeScanNode node) {
                estimates.add(
                        estimateRangeScan(ctx, node, i, parameters)
                );
            } else if (child instanceof CompoundIndexScanNode node) {
                estimates.add(
                        estimateCompoundIndexScan(ctx, node, i, parameters)
                );
            }
        }

        if (estimates.isEmpty()) {
            return children.getFirst();
        }

        Collections.sort(estimates);
        return children.get(estimates.getFirst().index());
    }

    /**
     * Estimates selectivity for a single-bound index scan (LT, LTE, GT, GTE operators).
     */
    private static IndexSelectivity estimateIndexScan(
            PlannerContext ctx,
            IndexScanNode node,
            int index,
            List<BqlValue> parameters
    ) {
        SingleFieldIndexDefinition def = node.getIndexDefinition();
        IndexStatistics stats = ctx.getMetadata().indexes().getStatistics(def.id());

        if (stats == null || stats.histogram() == null) {
            return new IndexSelectivity(UNKNOWN, index);
        }

        BsonValue value = IndexPredicateResolver.resolveIndexKeyValue(def, node, parameters);
        if (value == null) {
            return new IndexSelectivity(UNKNOWN, index);
        }

        double p = calculatePercentile(stats.histogram(), value);
        long N = stats.cardinality();

        double estimation = switch (node.predicate().op()) {
            case LT, LTE -> p * N;
            case GT, GTE -> (1.0 - p) * N;
            default -> UNKNOWN;
        };

        return new IndexSelectivity(estimation, index);
    }

    /**
     * Estimates selectivity for a range scan with both lower and upper bounds.
     */
    private static IndexSelectivity estimateRangeScan(
            PlannerContext ctx,
            RangeScanNode node,
            int index,
            List<BqlValue> parameters
    ) {
        SingleFieldIndexDefinition def = node.getIndexDefinition();
        IndexStatistics stats = ctx.getMetadata().indexes().getStatistics(def.id());

        if (stats == null || stats.histogram() == null) {
            return new IndexSelectivity(UNKNOWN, index);
        }

        IndexPredicateResolver.IndexKeyRange range = IndexPredicateResolver.resolveIndexKeyRange(def, node, parameters);
        if (range == null) {
            return new IndexSelectivity(UNKNOWN, index);
        }

        double estimation = estimateRange(
                stats.histogram(),
                range.lower(),
                range.upper(),
                stats.cardinality()
        );

        return new IndexSelectivity(estimation, index);
    }

    /**
     * Computes estimated document count for a range using percentile difference.
     * Returns UNKNOWN if the upper percentile is not greater than the lower percentile.
     */
    private static double estimateRange(
            Histogram histogram,
            BsonValue lower,
            BsonValue upper,
            long cardinality
    ) {
        double low = lower != null ? calculatePercentile(histogram, lower) : EPSILON;
        double high = upper != null ? calculatePercentile(histogram, upper) : 1.0 - EPSILON;

        if (high <= low) {
            return UNKNOWN;
        }

        return (high - low) * cardinality;
    }

    /**
     * Calculates the percentile of a value within the histogram, clamped to [EPSILON, 1-EPSILON].
     */
    private static double calculatePercentile(Histogram histogram, BsonValue value) {
        double p = HistogramUtil.findPercentile(histogram, value) / 100.0;

        if (p < EPSILON) return EPSILON;
        return Math.min(p, 1.0 - EPSILON);

    }

    /**
     * Estimates selectivity for a compound index scan by packing filter values into a
     * composite key and comparing against the compound histogram.
     */
    private static IndexSelectivity estimateCompoundIndexScan(
            PlannerContext ctx,
            CompoundIndexScanNode node,
            int index,
            List<BqlValue> parameters
    ) {
        CompoundIndexDefinition def = node.indexDefinition();
        IndexStatistics stats = ctx.getMetadata().compoundIndexes().getStatistics(def.id());

        if (stats == null || stats.histogram() == null) {
            return new IndexSelectivity(UNKNOWN, index);
        }

        Histogram histogram = stats.histogram();
        long N = stats.cardinality();

        // Partition filters into EQ prefix and range filters
        List<Object> eqValues = new ArrayList<>();
        List<CompoundIndexScanFilter> rangeFilters = new ArrayList<>();

        for (CompoundIndexScanFilter filter : node.filters()) {
            if (filter.op() == Operator.EQ) {
                BqlValue resolved = filter.operand().resolve(parameters);
                eqValues.add(SelectorCalculator.extractIndexValueFromBqlValue(resolved));
            } else {
                rangeFilters.add(filter);
            }
        }

        if (rangeFilters.isEmpty()) {
            // All-EQ: uniform bucket density assumption
            if (histogram.isEmpty()) {
                return new IndexSelectivity(UNKNOWN, index);
            }
            double estimation = (double) N / histogram.size();
            return new IndexSelectivity(estimation, index);
        }

        // Find lower/upper range filters
        CompoundIndexScanFilter lowerFilter = null;
        CompoundIndexScanFilter upperFilter = null;
        for (CompoundIndexScanFilter rf : rangeFilters) {
            if (rf.op() == Operator.GT || rf.op() == Operator.GTE) {
                lowerFilter = rf;
            } else if (rf.op() == Operator.LT || rf.op() == Operator.LTE) {
                upperFilter = rf;
            }
        }

        if (lowerFilter != null && upperFilter != null) {
            // Two-sided range
            BsonBinary lowerKey = packCompositeKey(eqValues, lowerFilter, parameters);
            BsonBinary upperKey = packCompositeKey(eqValues, upperFilter, parameters);
            double estimation = estimateRange(histogram, lowerKey, upperKey, N);
            return new IndexSelectivity(estimation, index);
        }

        // One-sided range
        CompoundIndexScanFilter rangeFilter = (lowerFilter != null) ? lowerFilter : upperFilter;
        BsonBinary compositeKey = packCompositeKey(eqValues, rangeFilter, parameters);
        double p = calculatePercentile(histogram, compositeKey);

        double estimation = switch (rangeFilter.op()) {
            case LT, LTE -> p * N;
            case GT, GTE -> (1.0 - p) * N;
            default -> UNKNOWN;
        };

        return new IndexSelectivity(estimation, index);
    }

    /**
     * Packs EQ prefix values and a range filter value into a composite BsonBinary key,
     * mirroring the format used by CompoundAnalyzeStrategy.extractKey().
     */
    private static BsonBinary packCompositeKey(
            List<Object> eqValues,
            CompoundIndexScanFilter rangeFilter,
            List<BqlValue> parameters
    ) {
        BqlValue resolved = rangeFilter.operand().resolve(parameters);
        Object rangeValue = SelectorCalculator.extractIndexValueFromBqlValue(resolved);
        Object[] parts = new Object[eqValues.size() + 1];
        for (int i = 0; i < eqValues.size(); i++) {
            parts[i] = eqValues.get(i);
        }
        parts[parts.length - 1] = rangeValue;
        return new BsonBinary(Tuple.from(parts).pack());
    }

    /**
     * Pairs an estimated document count with the index position in the candidate list.
     * Ordered by estimated count ascending (lower is better).
     */
    record IndexSelectivity(double estimated, int index)
            implements Comparable<IndexSelectivity> {
        @Override
        public int compareTo(IndexSelectivity o) {
            return Double.compare(this.estimated, o.estimated);
        }
    }
}

