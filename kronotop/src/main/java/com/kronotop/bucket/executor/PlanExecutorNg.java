/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.planner.physical.*;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class PlanExecutorNg {
    private final PlanExecutorConfig config;

    private final ExecutionHandlers executionHandlers;

    public PlanExecutorNg(Context context, PlanExecutorConfig config) {
        this.config = config;
        BucketService bucketService = context.getService(BucketService.NAME);
        CursorManager cursorManager = new CursorManager();
        FilterEvaluator filterEvaluator = new FilterEvaluator();
        DocumentRetriever documentRetriever = new DocumentRetriever(bucketService);
        IndexUtils indexUtils = new IndexUtils();
        this.executionHandlers = new ExecutionHandlers(context, config, cursorManager, filterEvaluator, documentRetriever, indexUtils);
    }

    /**
     * Executes the provided physical plan using the given configuration and transaction.
     * Delegates to appropriate execution handlers based on the physical plan type.
     *
     * @param tr the FoundationDB transaction to use for executing the physical plan
     * @return a map of document IDs (Versionstamp) to their content (ByteBuffer), sorted by document ID
     * @throws UnsupportedOperationException if the physical plan type is not supported or not yet implemented
     */
    public Map<Versionstamp, ByteBuffer> execute(Transaction tr) {
        Map<Versionstamp, ByteBuffer> results = switch (config.getPlan()) {
            case PhysicalFullScan fullScan -> executionHandlers.executeFullBucketScan(tr, fullScan.node());
            case PhysicalIndexScan indexScan -> executionHandlers.executeIndexScan(tr, indexScan.node());
            case PhysicalRangeScan rangeScan -> executionHandlers.executeRangeScan(tr, rangeScan);
            case PhysicalAnd physicalAnd -> executionHandlers.executePhysicalAnd(tr, physicalAnd);
            case PhysicalOr physicalOr -> executionHandlers.executePhysicalOr(tr, physicalOr);
            case PhysicalIndexIntersection indexIntersection ->
                    executionHandlers.executePhysicalIndexIntersection(tr, indexIntersection);
            case PhysicalTrue physicalTrue -> executionHandlers.executeFullBucketScan(tr, physicalTrue);
            default ->
                    throw new UnsupportedOperationException("Physical plan type not implemented: " + config.getPlan().getClass().getSimpleName());
        };

        // Apply final sorting once, only if reverse is enabled
        if (config.isReverse()) {
            return sortResultsDescending(results);
        }
        return results;
    }

    /**
     * Sorts results by the configured sort field in descending order for REVERSE=true.
     * This implements the manual sorting requirement for AND/OR operations.
     * Currently only supports sorting by '_id' field (versionstamp).
     */
    private Map<Versionstamp, ByteBuffer> sortResultsDescending(Map<Versionstamp, ByteBuffer> results) {
        // Only sort by _id field for now - other fields will be implemented in a future iteration
        if (DefaultIndexDefinition.ID.selector().equals(config.getSortByField())) {
            return results.entrySet().stream()
                    .sorted((e1, e2) -> e2.getKey().compareTo(e1.getKey())) // Descending order by versionstamp
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            Map.Entry::getValue,
                            (e1, e2) -> e1,
                            LinkedHashMap::new
                    ));
        }

        // For non-_id fields, return results as-is for now
        // Document scanning and field-based sorting will be implemented in a different iteration
        return results;
    }
}