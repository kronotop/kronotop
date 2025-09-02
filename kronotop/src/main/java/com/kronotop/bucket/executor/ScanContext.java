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

import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.physical.PhysicalFilter;
import com.kronotop.bucket.planner.physical.PhysicalRangeScan;

/**
 * Base class for different types of scan contexts used by SelectorCalculator.
 * Contains common fields needed for selector calculation across all scan types.
 */
abstract class ScanContext {
    protected final DirectorySubspace indexSubspace;
    protected final PlanExecutorConfig config;
    protected final Bounds bounds;

    protected ScanContext(DirectorySubspace indexSubspace, PlanExecutorConfig config, Bounds bounds) {
        this.indexSubspace = indexSubspace;
        this.config = config;
        this.bounds = bounds;
    }

    public DirectorySubspace indexSubspace() {
        return indexSubspace;
    }

    public PlanExecutorConfig config() {
        return config;
    }

    public Bounds bounds() {
        return bounds;
    }

    public boolean isReverse() {
        return config.isReverse();
    }
}

/**
 * Context for ID index scans (primary index scans).
 * Used for full bucket scans and OR operations.
 */
class IdIndexScanContext extends ScanContext {
    public IdIndexScanContext(DirectorySubspace indexSubspace, PlanExecutorConfig config, Bounds bounds) {
        super(indexSubspace, config, bounds);
    }
}

/**
 * Context for filter-based scans.
 * Used by executeIndexScan for applying specific filter conditions.
 */
class FilterScanContext extends ScanContext {
    private final PhysicalFilter filter;
    private final IndexDefinition indexDefinition;

    public FilterScanContext(DirectorySubspace indexSubspace, PlanExecutorConfig config,
                             Bounds bounds, PhysicalFilter filter, IndexDefinition indexDefinition) {
        super(indexSubspace, config, bounds);
        this.filter = filter;
        this.indexDefinition = indexDefinition;
    }

    public PhysicalFilter filter() {
        return filter;
    }

    public IndexDefinition indexDefinition() {
        return indexDefinition;
    }
}

/**
 * Context for range scan operations.
 * Used by executeRangeScan for complex range queries.
 */
class RangeScanContext extends ScanContext {
    private final PhysicalRangeScan rangeScan;
    private final IndexDefinition indexDefinition;

    public RangeScanContext(DirectorySubspace indexSubspace, PlanExecutorConfig config,
                            Bounds bounds, PhysicalRangeScan rangeScan, IndexDefinition indexDefinition) {
        super(indexSubspace, config, bounds);
        this.rangeScan = rangeScan;
        this.indexDefinition = indexDefinition;
    }

    public PhysicalRangeScan rangeScan() {
        return rangeScan;
    }

    public IndexDefinition indexDefinition() {
        return indexDefinition;
    }
}