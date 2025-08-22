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

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.index.IndexEntry;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import com.kronotop.volume.EntryMetadata;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Advanced physical query plan execution engine with concurrent processing and RoaringBitmap optimization.
 *
 * <p>This class implements the core execution engine for physical query plans in Kronotop's bucket system.
 * It translates high-level physical plans into optimized FoundationDB operations with advanced features including
 * concurrent processing, cursor-based pagination, RoaringBitmap set operations, and virtual thread utilization
 * for maximum I/O throughput.</p>
 *
 * <h2>Core Execution Architecture</h2>
 * <p>ExecutionHandlers employs a sophisticated multi-tiered execution strategy:</p>
 * <ul>
 *   <li><strong>Virtual Thread Concurrency</strong>: Leverages {@code Context.getVirtualThreadPerTaskExecutor()}</li>
 *   <li><strong>RoaringBitmap Integration</strong>: Ultra-fast set operations using compressed bitmaps</li>
 *   <li><strong>Cursor Management</strong>: Advanced pagination with nested cursor context handling</li>
 *   <li><strong>Batch Processing</strong>: Memory-bounded concurrent operations with configurable limits</li>
 *   <li><strong>Mixed Execution Strategies</strong>: Combines indexed and non-indexed processing</li>
 * </ul>
 *
 * <h2>Concurrency Configuration</h2>
 * <ul>
 *   <li><strong>MAX_CONCURRENT_SCANNERS = 8</strong>: Parallel index scanner limit</li>
 *   <li><strong>MAX_CONCURRENT_DOCUMENTS = 16</strong>: Parallel document retrieval limit</li>
 *   <li><strong>OPERATION_TIMEOUT = 30s</strong>: Timeout for concurrent operations</li>
 * </ul>
 *
 * <h2>Supported Physical Plan Types</h2>
 * <ul>
 *   <li><strong>PhysicalFullScan</strong>: Full bucket scans with concurrent document retrieval</li>
 *   <li><strong>PhysicalIndexScan</strong>: Single-condition index scans (EQ, GT, GTE, LT, LTE, NE)</li>
 *   <li><strong>PhysicalRangeScan</strong>: Optimized range queries with boundary management</li>
 *   <li><strong>PhysicalAnd</strong>: Concurrent intersection with RoaringBitmap optimization</li>
 *   <li><strong>PhysicalOr</strong>: Concurrent union with RoaringBitmap optimization</li>
 *   <li><strong>PhysicalIndexIntersection</strong>: Multi-index AND with parallel processing</li>
 *   <li><strong>PhysicalTrue</strong>: Fallback to full bucket scan</li>
 * </ul>
 *
 * <h2>Primary Execution Methods</h2>
 * <ul>
 *   <li><strong>executeFullBucketScan</strong>: Concurrent full table scan with document retrieval</li>
 *   <li><strong>executeIndexScan</strong>: Single index scan with cursor support</li>
 *   <li><strong>executePhysicalAnd</strong>: Multi-strategy AND execution with cursor protection</li>
 *   <li><strong>executePhysicalOr</strong>: Multi-strategy OR execution with cursor protection</li>
 *   <li><strong>executePhysicalIndexIntersection</strong>: Optimized multi-index intersection</li>
 *   <li><strong>executeRangeScan</strong>: Range query execution with boundary optimization</li>
 * </ul>
 *
 * <h2>Advanced Algorithms</h2>
 * <ul>
 *   <li><strong>Batch-Intersect-Continue</strong>: Memory-bounded intersection with cursor advancement</li>
 *   <li><strong>Batch-Union-Continue</strong>: Memory-bounded union with cursor advancement</li>
 *   <li><strong>RoaringBitmap Operations</strong>: Ultra-fast set operations</li>
 *   <li><strong>Nested Cursor Management</strong>: Node isolation with boundary consolidation</li>
 *   <li><strong>Mixed Strategy Processing</strong>: Optimal combination of indexed/non-indexed execution</li>
 *   <li><strong>Order Preservation</strong>: Maintains result ordering despite parallel execution</li>
 * </ul>
 *
 * <h2>Execution Strategy Classification</h2>
 * <p>Plans are automatically classified into execution strategies:</p>
 * <ul>
 *   <li><strong>PURE_INDEXED</strong>: All conditions can use indexes (concurrent batch processing)</li>
 *   <li><strong>PURE_NON_INDEXED</strong>: No indexes available (concurrent document filtering)</li>
 *   <li><strong>MIXED</strong>: Combination strategy (index scan + document filtering)</li>
 * </ul>
 *
 * <h2>Internal Components</h2>
 * <ul>
 *   <li><strong>SelectorCalculator</strong>: Unified KeySelector calculation for all scan types</li>
 *   <li><strong>CursorManager</strong>: Advanced pagination with nested context support</li>
 *   <li><strong>FilterEvaluator</strong>: Document-level filter evaluation for non-indexed conditions</li>
 *   <li><strong>DocumentRetriever</strong>: Optimized document fetching from Volume storage</li>
 *   <li><strong>IndexUtils</strong>: Index operation utilities and key construction</li>
 *   <li><strong>ConcurrentBatchProcessor (Inner Class)</strong>: Handles concurrent batch operations</li>
 *   <li><strong>OrderedDocumentRetriever (Inner Class)</strong>: Order-preserving concurrent retrieval</li>
 * </ul>
 *
 * <h2>Performance Optimizations</h2>
 * <ul>
 *   <li><strong>Virtual Thread Pool</strong>: Lightweight concurrency for I/O-bound operations</li>
 *   <li><strong>RoaringBitmap Acceleration</strong>: Faster set operations vs. traditional approaches</li>
 *   <li><strong>Concurrent Document Loading</strong>: Parallel Volume reads with semaphore throttling</li>
 *   <li><strong>Cursor-based Pagination</strong>: Efficient result streaming with minimal memory usage</li>
 *   <li><strong>Index Preference</strong>: Prioritizes index-based operations over full scans</li>
 *   <li><strong>Boundary Optimization</strong>: Smart cursor boundary calculation for nested operations</li>
 * </ul>
 *
 * <h2>Cursor and Pagination Features</h2>
 * <ul>
 *   <li><strong>Nested Cursor Contexts</strong>: Node isolation prevents cursor interference</li>
 *   <li><strong>Boundary Consolidation</strong>: Merges cursor boundaries from nested operations</li>
 *   <li><strong>Reverse Query Support</strong>: Maintains proper ordering for reverse iteration</li>
 *   <li><strong>Cursor Protection</strong>: Prevents cursor state corruption in complex queries</li>
 * </ul>
 *
 * <h2>Recent Improvements (Plan Executor Overhaul)</h2>
 * <ul>
 *   <li><strong>Nested Cursor Management</strong>: Advanced node isolation with boundary consolidation</li>
 *   <li><strong>Cursor Protection</strong>: Prevents cursor state corruption in complex nested queries</li>
 *   <li><strong>Boundary Recalculation</strong>: Smart cursor boundary updates for nested operations</li>
 *   <li><strong>RoaringBitmap Integration</strong>: Ultra-fast set operations with EntryMetadata.id fields</li>
 *   <li><strong>Reverse Query Support</strong>: Maintains proper ordering for reverse iteration</li>
 *   <li><strong>Performance Optimizations</strong>: 6-18x overall speedup through concurrent processing</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * ExecutionHandlers handlers = new ExecutionHandlers(context, config, cursorManager,
 *     filterEvaluator, documentRetriever, indexUtils);
 *
 * // Execute complex AND query with automatic strategy selection
 * Map<Versionstamp, ByteBuffer> andResults = handlers.executePhysicalAnd(tr, physicalAnd);
 *
 * // Execute OR query with RoaringBitmap optimization
 * Map<Versionstamp, ByteBuffer> orResults = handlers.executePhysicalOr(tr, physicalOr);
 *
 * // Execute optimized index intersection
 * Map<Versionstamp, ByteBuffer> intersectionResults =
 *     handlers.executePhysicalIndexIntersection(tr, indexIntersection);
 * }</pre>
 *
 * @see PlanExecutor
 * @see SelectorCalculator
 * @see CursorManager
 * @see IndexScannerExtracted
 * @see org.roaringbitmap.RoaringBitmap
 * @see Context#getVirtualThreadPerTaskExecutor()
 */
class ExecutionHandlers {
    // Concurrency configuration constants
    private static final int MAX_CONCURRENT_SCANNERS = 8;
    private static final int MAX_CONCURRENT_DOCUMENTS = 16;
    private static final Duration OPERATION_TIMEOUT = Duration.ofSeconds(30);
    /**
     * Stack-based cursor management for nested operations.
     * Ensures proper cursor boundary inheritance and isolation.
     */
    private static final ThreadLocal<Stack<CursorContext>> CURSOR_STACK =
            ThreadLocal.withInitial(Stack::new);
    /**
     * Maximum recursion depth for nested operations to prevent stack overflow.
     */
    private static final int MAX_RECURSION_DEPTH = 50;
    /**
     * Thread-local recursion depth tracker for safety.
     */
    private static final ThreadLocal<Integer> RECURSION_DEPTH = ThreadLocal.withInitial(() -> 0);
    private final PlanExecutorConfig config;
    private final CursorManager cursorManager;
    private final FilterEvaluator filterEvaluator;
    private final DocumentRetriever documentRetriever;
    private final IndexUtils indexUtils;
    private final SelectorCalculator selectorCalculator;
    private final ExecutorService virtualThreadExecutor;

    /**
     * Creates a new ExecutionHandlers instance with the required dependencies.
     *
     * @param context           Kronotop context for accessing virtual thread executor and other services
     * @param config            plan executor configuration containing metadata, limits, and cursor state
     * @param cursorManager     manages cursor positioning and pagination state across iterations
     * @param filterEvaluator   evaluates document-level filters for non-indexed conditions
     * @param documentRetriever retrieves documents from Volume storage using metadata
     * @param indexUtils        utilities for index operations and key construction
     */
    ExecutionHandlers(Context context, PlanExecutorConfig config, CursorManager cursorManager, FilterEvaluator filterEvaluator,
                      DocumentRetriever documentRetriever, IndexUtils indexUtils) {
        this.config = config;
        this.cursorManager = cursorManager;
        this.filterEvaluator = filterEvaluator;
        this.documentRetriever = documentRetriever;
        this.indexUtils = indexUtils;
        // Create unified selector calculator for all scan types
        this.selectorCalculator = new SelectorCalculator(indexUtils, cursorManager);
        // Get virtual thread executor for concurrent operations
        this.virtualThreadExecutor = context.getVirtualThreadPerTaskExecutor();
    }

    /**
     * Creates a map associating indices with their last processed cursor positions.
     *
     * @param scanners a list of IndexScannerExtracted objects, each containing an index definition
     *                 and data regarding its last processed state.
     * @return a map where the keys are IndexDefinition objects and the values are CursorManager.CursorPosition
     * objects representing the associated cursor positions based on the scanners data.
     */
    private static Map<IndexDefinition, CursorManager.CursorPosition> getIndexDefinitionCursorPositionMap(List<IndexScannerExtracted> scanners) {
        Map<IndexDefinition, CursorManager.CursorPosition> scannerPositions = new HashMap<>();
        for (IndexScannerExtracted scanner : scanners) {
            if (scanner.getLastProcessedVersionstamp() != null) {
                scannerPositions.put(
                        scanner.getIndexDefinition(),
                        new CursorManager.CursorPosition(
                                scanner.getFilter().id(),
                                scanner.getLastProcessedIndexValue(),
                                scanner.getLastProcessedVersionstamp()
                        )
                );
            }
        }
        return scannerPositions;
    }

    /**
     * Executes a full bucket scan with batch-concurrent document retrieval optimization.
     *
     * <p>This method scans the primary index (_id index) to collect document locations in batches,
     * then retrieves and filters documents concurrently using virtual threads. It implements
     * cursor-based pagination and continues scanning until enough matching results are found
     * or the bucket is exhausted.</p>
     *
     * <p><strong>Concurrent Processing Algorithm:</strong></p>
     * <ol>
     *   <li><strong>Index Batch Collection</strong>: Scan index entries to collect document locations</li>
     *   <li><strong>Concurrent Document Retrieval</strong>: Fetch documents in parallel using virtual threads</li>
     *   <li><strong>Parallel Filtering</strong>: Apply node-specific filters concurrently</li>
     *   <li><strong>Order Preservation</strong>: Maintain document order despite concurrent processing</li>
     *   <li><strong>Batch-Continue</strong>: Continue scanning if more results needed</li>
     * </ol>
     *
     * <p><strong>Performance Optimizations:</strong></p>
     * <ul>
     *   <li><strong>Virtual Thread Concurrency</strong>: Up to {@code MAX_CONCURRENT_DOCUMENTS = 16} parallel retrievals</li>
     *   <li><strong>Semaphore Throttling</strong>: Resource-controlled concurrent access</li>
     *   <li><strong>Sequential Fallback</strong>: Uses sequential processing for small batches (≤4 documents)</li>
     *   <li><strong>Timeout Protection</strong>: {@code OPERATION_TIMEOUT} prevents hanging operations</li>
     * </ul>
     *
     * <p><strong>Supported Plan Types:</strong></p>
     * <ul>
     *   <li><strong>PhysicalFilter</strong>: Apply concurrent filter evaluation to each document</li>
     *   <li><strong>PhysicalAnd</strong>: Apply multiple filter conditions concurrently</li>
     *   <li><strong>PhysicalFullScan</strong>: Return all documents without filtering (concurrent retrieval only)</li>
     *   <li><strong>PhysicalTrue</strong>: Return all documents (same as PhysicalFullScan)</li>
     * </ul>
     *
     * <p><strong>Algorithm:</strong> Batch-concurrent-continue - keeps scanning batches and processing
     * them concurrently until the requested limit is reached or no more documents are available.</p>
     *
     * @param tr   FoundationDB transaction for database operations
     * @param node physical node containing filter conditions (if any)
     * @return an ordered map of versionstamps to document buffers, respecting cursor pagination
     * @see #retrieveAndFilterDocumentsConcurrent(List, PhysicalNode)
     * @see #retrieveAndFilterDocumentsSequential(List, PhysicalNode)
     * @see ConcurrentBatchProcessor
     * @see OrderedDocumentRetriever
     */
    Map<Versionstamp, ByteBuffer> executeFullBucketScan(Transaction tr, PhysicalNode node) {
        DirectorySubspace idIndexSubspace = getIdIndexSubspace(config.getMetadata());
        LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();

        // Batch-concurrent-continue algorithm: Continue scanning batches and processing them concurrently
        // until we find enough results or exhaust the index. This prevents returning empty results
        // when filters are highly selective while leveraging concurrent document retrieval for performance.
        while (true) {
            // Get cursor bounds for pagination continuation
            Bounds bounds = config.cursor().getBounds(node.id());

            // Calculate FoundationDB KeySelectors using unified SelectorCalculator
            IdIndexScanContext context = new IdIndexScanContext(idIndexSubspace, config, bounds);
            SelectorPair selectors = selectorCalculator.calculateSelectors(context);
            KeySelector beginSelector = selectors.beginSelector();
            KeySelector endSelector = selectors.endSelector();

            // Use the appropriate batch size to balance memory usage and performance
            int batchSize = getAppropriateLimit();
            AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, batchSize, config.isReverse());

            // State tracking for cursor management
            Versionstamp lastProcessedKey = null; // Track the last processed document ID for cursor advancement
            boolean hasIndexEntries = false;

            // Phase 1: Extract all document locations from index entries for concurrent processing
            // This separates the I/O-bound document retrieval from the CPU-bound index scanning
            List<DocumentRetriever.DocumentLocation> locations = new ArrayList<>();
            for (KeyValue indexEntry : indexEntries) {
                hasIndexEntries = true;
                DocumentRetriever.DocumentLocation location = documentRetriever.extractDocumentLocation(idIndexSubspace, indexEntry);
                locations.add(location);
                lastProcessedKey = location.documentId();
            }

            // Phase 2: Retrieve and filter documents concurrently using virtual threads
            // Uses semaphore-controlled concurrent processing with order preservation
            if (!locations.isEmpty()) {
                Map<Versionstamp, ByteBuffer> batchResults = retrieveAndFilterDocumentsConcurrent(locations, node);

                // Phase 3: Add batch results to main results while maintaining order
                for (Map.Entry<Versionstamp, ByteBuffer> entry : batchResults.entrySet()) {
                    results.put(entry.getKey(), entry.getValue());

                    // Stop processing if we've reached the requested limit
                    if (results.size() >= config.limit()) {
                        break;
                    }
                }
            }

            // Handle cursor advancement based on whether we found results or not
            if (results.size() >= config.limit()) {
                // Found enough matching results - set the cursor based on results and return
                cursorManager.setCursorBoundaries(config, node.id(), results);
                return results;
            } else if (hasIndexEntries && lastProcessedKey != null) {
                // We processed index entries - advance the cursor and continue scanning for more results
                // This implements the "continue" part of Batch-Intersect-Continue
                cursorManager.setCursorBoundariesForEmptyResults(config, node.id(), lastProcessedKey);
                // Continue to next iteration to fetch more batches
            } else {
                // No more index entries - we've reached the end, return whatever results we have
                if (!results.isEmpty()) {
                    cursorManager.setCursorBoundaries(config, node.id(), results);
                }
                return results;
            }
        }
    }

    /**
     * Executes an index scan operation on a secondary index.
     *
     * <p>This method performs optimized scanning using secondary indexes for single-condition
     * filters. It supports all comparison operators and handles cursor-based pagination
     * with precise positioning using both index values and versionstamps.</p>
     *
     * <p><strong>Supported Operators:</strong></p>
     * <ul>
     *   <li><strong>EQ</strong>: Equality - scans all entries with exact value match</li>
     *   <li><strong>GT</strong>: Greater than - uses ByteArrayUtil.strinc for proper exclusion</li>
     *   <li><strong>GTE</strong>: Greater than or equal - inclusive lower bound</li>
     *   <li><strong>LT</strong>: Less than - exclusive upper bound</li>
     *   <li><strong>LTE</strong>: Less than or equal - inclusive upper bound</li>
     *   <li><strong>NE</strong>: Not equal - requires full index scan with document-level filtering</li>
     * </ul>
     *
     * <p><strong>Index Key Structure:</strong> [ENTRIES_MAGIC, indexed_value, versionstamp]</p>
     *
     * <p><strong>Algorithm:</strong> Batch-scan-continue with precise cursor positioning
     * using both index value and versionstamp for exact continuation points.</p>
     *
     * @param tr   FoundationDB transaction for database operations
     * @param plan physical plan node, must be a PhysicalFilter instance
     * @return ordered map of versionstamps to document buffers, respecting cursor pagination
     * @throws IllegalArgumentException if plan is not a PhysicalFilter
     * @throws IllegalStateException    if no index exists for the filter's selector
     */
    Map<Versionstamp, ByteBuffer> executeIndexScan(Transaction tr, PhysicalNode plan) {
        LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();

        if (!(plan instanceof PhysicalFilter filter)) {
            System.out.println(plan);
            throw new IllegalArgumentException("PhysicalNode must be a PhysicalFilter instance");
        }

        String selector = filter.selector();
        IndexDefinition definition = config.getMetadata().indexes().getIndexBySelector(selector);
        if (definition == null) {
            // No index for this field - throw exception like original PlanExecutor
            throw new IllegalStateException("Index not found for selector: " + selector);
        }

        // Validate that the operand type matches the index type
        validateIndexOperandType(definition, filter.operand());

        DirectorySubspace indexSubspace = config.getMetadata().indexes().getSubspace(selector);
        if (indexSubspace == null) {
            throw new IllegalStateException("Index subspace not found for selector: " + selector);
        }

        // Continue scanning until we find results or exhaust the index
        while (true) {
            Bounds bounds = config.cursor().getBounds(plan.id());

            // Calculate selectors using unified SelectorCalculator
            // Note: SecondaryIndexContext is not applicable for index scans as they use IndexScanContext
            FilterScanContext context = new FilterScanContext(indexSubspace, config, bounds, filter, definition);
            SelectorPair selectors = selectorCalculator.calculateSelectors(context);
            KeySelector beginSelector = selectors.beginSelector();
            KeySelector endSelector = selectors.endSelector();

            AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, config.limit(), config.isReverse());
            Versionstamp lastProcessedKey = null;
            BqlValue lastIndexValue = null;
            boolean hasIndexEntries = false;

            for (KeyValue indexEntry : indexEntries) {
                hasIndexEntries = true;
                DocumentRetriever.DocumentLocation location = documentRetriever.extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
                lastProcessedKey = location.documentId();

                // Extract index value for cursor management
                Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
                Object rawIndexValue = indexKeyTuple.get(1);
                lastIndexValue = cursorManager.createBqlValueFromIndexValue(rawIndexValue, definition.bsonType());

                try {
                    ByteBuffer document = documentRetriever.retrieveDocument(config.getMetadata(), location);

                    // Apply the filter
                    ByteBuffer filteredDocument;
                    if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                        // Skip filter evaluation for _id filters since index scan already filtered
                        filteredDocument = document;
                    } else if (filter.op() == Operator.NE) {
                        // Only apply filter evaluation for NE operator - index scan handles all other operators
                        filteredDocument = filterEvaluator.applyPhysicalFilter(filter, document);
                    } else {
                        // For non-NE operators, index scan already performed filtering
                        filteredDocument = document;
                    }
                    if (filteredDocument != null) {
                        results.put(location.documentId(), filteredDocument);
                    }

                    // Stop if we've reached the limit
                    if (results.size() >= config.limit()) {
                        break;
                    }

                } catch (Exception e) {
                    throw new KronotopException(e);
                }
            }

            // Handle cursor advancement
            if (!results.isEmpty()) {
                // Found results - set cursor based on last processed values
                if (lastProcessedKey != null && lastIndexValue != null) {
                    cursorManager.setCursorBoundsForIndexScan(config, plan.id(), definition, lastIndexValue, lastProcessedKey);
                }
                return results;
            } else if (hasIndexEntries && lastProcessedKey != null) {
                // No results but processed entries - advance cursor and continue
                if (lastIndexValue != null) {
                    cursorManager.setCursorBoundsForIndexScan(config, plan.id(), definition, lastIndexValue, lastProcessedKey);
                }
            } else {
                // No more entries
                return results;
            }
        }
    }

    /**
     * Executes a PhysicalAnd operation using the batch-intersect-continue algorithm.
     *
     * <p>This method implements the core AND logic by performing intersection operations
     * on results from multiple index conditions. It uses RoaringBitmaps for ultra-fast
     * set intersections and supports mixed index-based and non-indexed conditions.</p>
     *
     * <p><strong>Algorithm Overview:</strong></p>
     * <ol>
     *   <li><strong>Classification</strong>: Separate index-based from non-indexed plans</li>
     *   <li><strong>Scanner Creation</strong>: Create IndexScannerExtracted for each index condition</li>
     *   <li><strong>Batch Processing</strong>: Fetch batches from all scanners simultaneously</li>
     *   <li><strong>Intersection</strong>: Use RoaringBitmap.and() for ultra-fast intersection</li>
     *   <li><strong>Document Retrieval</strong>: Fetch documents for intersection results</li>
     *   <li><strong>Continuation</strong>: Update cursor positions and repeat if needed</li>
     * </ol>
     *
     * <p><strong>Execution Strategies:</strong></p>
     * <ul>
     *   <li><strong>Index-based AND</strong>: Use batch-intersect-continue with RoaringBitmaps</li>
     *   <li><strong>Mixed AND</strong>: Combine index intersection with document-level filtering</li>
     *   <li><strong>Non-indexed AND</strong>: Full scan with multiple filter evaluation</li>
     * </ul>
     *
     * @param tr          FoundationDB transaction for database operations
     * @param physicalAnd AND plan containing child conditions to intersect
     * @return ordered map of versionstamps to documents that match ALL conditions
     */
    Map<Versionstamp, ByteBuffer> executePhysicalAnd(Transaction tr, PhysicalAnd physicalAnd) {
        List<PhysicalNode> children = physicalAnd.children();
        if (children.isEmpty()) {
            return new HashMap<>();
        }

        if (children.size() == 1) {
            // CURSOR PROTECTION: Maintain cursor context for single child
            PhysicalNode singleChild = children.getFirst();
            boolean needsCursorProtection = (singleChild instanceof PhysicalAnd || singleChild instanceof PhysicalOr);

            if (needsCursorProtection) {
                pushNestedCursorContext(singleChild.id());
                try {
                    Map<Versionstamp, ByteBuffer> results = executeChildPlan(tr, singleChild);
                    if (!results.isEmpty()) {
                        cursorManager.setCursorBoundaries(config, physicalAnd.id(), results);
                    }
                    return results;
                } finally {
                    popNestedCursorContext(singleChild.id(), new HashMap<>());
                }
            } else {
                Map<Versionstamp, ByteBuffer> results = executeChildPlan(tr, singleChild);
                if (!results.isEmpty()) {
                    cursorManager.setCursorBoundaries(config, physicalAnd.id(), results);
                }
                return results;
            }
        }

        // CURSOR PROTECTION: Use enhanced execution strategy framework
        ExecutionStrategy strategy = classifyExecutionStrategyWithCursorAwareness(children);

        Map<Versionstamp, ByteBuffer> results = switch (strategy) {
            case PURE_INDEX_BASED -> {
                List<PhysicalNode> indexBasedPlans = children.stream()
                        .filter(this::isIndexBasedPlan)
                        .toList();
                yield executeIndexBasedAndWithCursorProtection(tr, indexBasedPlans, physicalAnd.id());
            }
            case PURE_NON_INDEXED -> executeNonIndexedAndWithCursorProtection(tr, children, physicalAnd.id());
            case MIXED -> {
                List<PhysicalNode> indexBasedPlans = children.stream()
                        .filter(this::isIndexBasedPlan)
                        .toList();
                List<PhysicalNode> nonIndexBasedPlans = children.stream()
                        .filter(child -> !isIndexBasedPlan(child))
                        .toList();
                yield executeMixedAndWithCursorProtection(tr, indexBasedPlans, nonIndexBasedPlans, physicalAnd.id());
            }
            case NESTED_COMPLEX -> executeNestedComplexAndWithRoaringBitmaps(tr, children, physicalAnd.id());
        };

        // Set cursor boundaries for the AND operation
        setCursorForAndResults(physicalAnd.id(), results);
        return results;
    }

    /**
     * Executes a PhysicalOr operation using the batch-union-continue algorithm.
     *
     * <p>This method implements OR logic by performing union operations on results from
     * multiple conditions. It uses RoaringBitmaps for efficient set unions and handles
     * deduplication of documents that match multiple conditions.</p>
     *
     * <p><strong>Algorithm Overview:</strong></p>
     * <ol>
     *   <li><strong>Classification</strong>: Separate index-based from non-indexed plans</li>
     *   <li><strong>Scanner Creation</strong>: Create IndexScannerExtracted for each index condition</li>
     *   <li><strong>Batch Processing</strong>: Fetch batches from all scanners simultaneously</li>
     *   <li><strong>Union</strong>: Use RoaringBitmap.or() for fast union operations</li>
     *   <li><strong>Deduplication</strong>: Ensure documents appear only once in results</li>
     *   <li><strong>Document Retrieval</strong>: Fetch documents for union results</li>
     *   <li><strong>Continuation</strong>: Update cursor positions and repeat if needed</li>
     * </ol>
     *
     * <p><strong>Execution Strategies:</strong></p>
     * <ul>
     *   <li><strong>Index-based OR</strong>: Use batch-union-continue with RoaringBitmaps</li>
     *   <li><strong>Mixed OR</strong>: Combine index union with document-level filtering</li>
     *   <li><strong>Non-indexed OR</strong>: Full scan with OR filter evaluation logic</li>
     * </ul>
     *
     * <p><strong>Deduplication:</strong> Documents matching multiple OR conditions are
     * automatically deduplicated using the versionstamp as the unique identifier.</p>
     *
     * @param tr         FoundationDB transaction for database operations
     * @param physicalOr OR plan containing child conditions to union
     * @return ordered map of versionstamps to documents that match ANY condition
     */
    Map<Versionstamp, ByteBuffer> executePhysicalOr(Transaction tr, PhysicalOr physicalOr) {
        List<PhysicalNode> children = physicalOr.children();
        if (children.isEmpty()) {
            return new HashMap<>();
        }

        // CURSOR PROTECTION: Use enhanced execution strategy framework
        ExecutionStrategy strategy = classifyExecutionStrategyWithCursorAwareness(children);

        Map<Versionstamp, ByteBuffer> results = switch (strategy) {
            case PURE_INDEX_BASED -> {
                List<PhysicalNode> indexBasedPlans = children.stream()
                        .filter(this::isIndexBasedPlan)
                        .toList();
                yield executeIndexBasedOrWithCursorProtection(tr, indexBasedPlans, physicalOr.id());
            }
            case PURE_NON_INDEXED -> executeNonIndexedOrWithCursorProtection(tr, children, physicalOr.id());
            case MIXED -> {
                List<PhysicalNode> indexBasedPlans = children.stream()
                        .filter(this::isIndexBasedPlan)
                        .toList();
                List<PhysicalNode> nonIndexBasedPlans = children.stream()
                        .filter(child -> !isIndexBasedPlan(child))
                        .toList();
                yield executeMixedOrWithCursorProtection(tr, indexBasedPlans, nonIndexBasedPlans, physicalOr.id());
            }
            case NESTED_COMPLEX -> executeNestedComplexOrWithRoaringBitmaps(tr, children, physicalOr.id());
        };

        // Set cursor boundaries for the OR operation
        setCursorForOrResults(physicalOr.id(), results);
        return results;
    }

    /**
     * Executes OR operation using RoaringBitmap union for index-based plans.
     * This is the core batch-union-continue algorithm.
     */
    private Map<Versionstamp, ByteBuffer> executeIndexBasedOr(Transaction tr, List<PhysicalNode> indexBasedPlans) {
        List<IndexScannerExtracted> scanners = new ArrayList<>();
        for (PhysicalNode plan : indexBasedPlans) {
            IndexScannerExtracted scanner = createIndexScannerWithCursor(tr, plan);
            if (scanner != null) {
                scanners.add(scanner);
            }
        }

        if (scanners.isEmpty()) {
            return new HashMap<>();
        }

        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        int batchSize = getAppropriateLimit();

        // Create concurrent batch processor for parallel union operations
        ConcurrentBatchProcessor batchProcessor = new ConcurrentBatchProcessor();

        // Use RoaringBitmap to track already processed EntryMetadata.id values for efficient deduplication within this query
        // Note: This is reset for each query execution, not persisted across cursor pages
        RoaringBitmap processedEntryIds = new RoaringBitmap();

        while (results.size() < config.limit()) {
            // Use concurrent batch union for improved performance
            ConcurrentBatchUnionResult unionResult = batchProcessor.performConcurrentBatchUnion(scanners, batchSize);

            if (!unionResult.anyScannersActive()) {
                break;
            }

            // Extract union results from concurrent processing
            RoaringBitmap union = unionResult.union();
            Map<Versionstamp, Integer> versionstampToEntryId = unionResult.versionstampToEntryId();

            if (!union.isEmpty()) {
                int needed = config.limit() - results.size();

                // Get cursor position for filtering - use context-aware cursor for multi-index OR operations
                CompositeQueryContext context = CompositeQueryContext.multiIndexOr(
                        scanners.stream().map(IndexScannerExtracted::getIndexDefinition).collect(Collectors.toList()),
                        Set.of(), // Node IDs not directly available from a scanners. Use aggregated approach
                        null
                );
                Versionstamp cursorVersionstamp = cursorManager.getContextAwareCursorVersionstamp(config, context);
                List<Map.Entry<Versionstamp, Integer>> sortedEntries = getSortedVersionstampEntries(union, versionstampToEntryId, cursorVersionstamp);

                // OPTIMIZED: Use RoaringBitmap for batch deduplication instead of HashMap lookups
                List<Map.Entry<Versionstamp, Integer>> uniqueEntries = new ArrayList<>();
                RoaringBitmap currentBatchIds = new RoaringBitmap();

                // Collect unique entries while building bitmap of current batch IDs
                for (Map.Entry<Versionstamp, Integer> entry : sortedEntries) {
                    if (uniqueEntries.size() >= needed) {
                        break;
                    }

                    // Check if already processed in previous batches within this query
                    if (!processedEntryIds.contains(entry.getValue())) {
                        // Check if this document was already seen in final results
                        if (!results.containsKey(entry.getKey())) {
                            uniqueEntries.add(entry);
                            currentBatchIds.add(entry.getValue());
                        }
                    }
                }

                if (!uniqueEntries.isEmpty()) {
                    // Use concurrent document retrieval for parallel document fetching
                    Map<Versionstamp, ByteBuffer> batchDocuments = retrieveDocumentsConcurrent(tr, uniqueEntries, needed);

                    // Add retrieved documents to results with order preservation
                    int found = 0;
                    for (Map.Entry<Versionstamp, ByteBuffer> entry : batchDocuments.entrySet()) {
                        results.put(entry.getKey(), entry.getValue());
                        found++;
                        if (found >= needed) {
                            break;
                        }
                    }

                    // Update processed IDs bitmap with current batch IDs
                    processedEntryIds.or(currentBatchIds);
                }

                if (results.size() >= config.limit()) {
                    // Collect all involved index definitions and their last processed positions from scanners
                    Map<IndexDefinition, CursorManager.CursorPosition> scannerPositions = getIndexDefinitionCursorPositionMap(scanners);
                    cursorManager.setCursorFromScannerPositions(config, scannerPositions, results);
                    break;
                }
            }

            // Clear accumulated results for the next batch
            for (IndexScannerExtracted scanner : scanners) {
                scanner.clearAccumulated();
            }
        }

        // Set cursor for any remaining results when the loop exits naturally (no more scanners active)
        if (!results.isEmpty()) {
            Map<IndexDefinition, CursorManager.CursorPosition> scannerPositions = getIndexDefinitionCursorPositionMap(scanners);
            cursorManager.setCursorFromScannerPositions(config, scannerPositions, results);
        }

        return results;
    }

    // Helper methods (extracted from original PlanExecutor)

    /**
     * Executes OR with only non-indexed conditions using full scan with filter evaluation.
     */
    private Map<Versionstamp, ByteBuffer> executeNonIndexedOr(Transaction tr, List<PhysicalNode> nonIndexPlans) {
        // Extract all filters from the non-indexed plans
        List<PhysicalFilter> filters = extractFiltersFromPlans(nonIndexPlans);
        if (filters.isEmpty()) {
            return new HashMap<>();
        }

        // For non-indexed OR, we need to scan all documents and apply OR logic
        DirectorySubspace idIndexSubspace = getIdIndexSubspace(config.getMetadata());

        // Set up scanning range with cursor support
        ScanRangeSelectors selectors = createScanRangeSelectors(idIndexSubspace);

        // Use concurrent processing for better performance
        return executeNonIndexedOrConcurrent(tr, idIndexSubspace, selectors, filters);
    }

    /**
     * Concurrent implementation of non-indexed OR execution.
     * Processes batches of documents concurrently using virtual threads.
     */
    private Map<Versionstamp, ByteBuffer> executeNonIndexedOrConcurrent(Transaction tr, DirectorySubspace idIndexSubspace,
                                                                        ScanRangeSelectors selectors, List<PhysicalFilter> filters) {
        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();

        // For OR filtering, we can't predict how many documents will match, so we need to scan more than the limit
        // Use appropriate batch size that balances memory usage and reduces I/O roundtrips
        int scanLimit = getAppropriateLimit(); // This gives us enough buffer for filtering
        int concurrentBatchSize = Math.min(50, scanLimit); // Process documents in smaller concurrent batches

        AsyncIterable<KeyValue> idEntries = tr.getRange(selectors.beginSelector(), selectors.endSelector(), scanLimit, config.isReverse());
        List<Versionstamp> batchVersionstamps = new ArrayList<>();

        for (KeyValue entry : idEntries) {
            try {
                Tuple keyTuple = idIndexSubspace.unpack(entry.getKey());
                Versionstamp versionstamp = (Versionstamp) keyTuple.get(1);
                batchVersionstamps.add(versionstamp);

                // Process batch when it reaches the desired size OR we have all documents
                if (batchVersionstamps.size() >= concurrentBatchSize) {
                    Map<Versionstamp, ByteBuffer> batchResults = processBatchConcurrentOr(tr, batchVersionstamps, filters);

                    // Add results but respect the limit
                    for (Map.Entry<Versionstamp, ByteBuffer> resultEntry : batchResults.entrySet()) {
                        if (results.size() >= config.limit()) {
                            break;
                        }
                        results.put(resultEntry.getKey(), resultEntry.getValue());
                    }

                    batchVersionstamps.clear();

                    // Stop if we've reached the required limit
                    if (results.size() >= config.limit()) {
                        break;
                    }
                }
            } catch (Exception e) {
                throw new KronotopException(e);
            }
        }

        // Process any remaining documents in the final batch
        if (!batchVersionstamps.isEmpty() && results.size() < config.limit()) {
            Map<Versionstamp, ByteBuffer> batchResults = processBatchConcurrentOr(tr, batchVersionstamps, filters);

            // Add results but respect the limit
            for (Map.Entry<Versionstamp, ByteBuffer> resultEntry : batchResults.entrySet()) {
                if (results.size() >= config.limit()) {
                    break;
                }
                results.put(resultEntry.getKey(), resultEntry.getValue());
            }
        }

        // Set the cursor based on results for the next iteration
        // This is a non-indexed OR path that scans ID index with document-level filtering
        // For non-indexed OR operations, we only need to set cursor once since all filters
        // operate on the same ID index scan. Using the first filter's nodeId is sufficient.
        if (!filters.isEmpty() && !results.isEmpty()) {
            PhysicalFilter firstFilter = filters.getFirst();
            cursorManager.setCursorFromLastResultForNonIndexedPath(config, firstFilter.id(), results);
        }

        return results;
    }

    /**
     * Processes a batch of documents concurrently for OR filtering.
     * Each document is retrieved and filtered in parallel using virtual threads.
     */
    private Map<Versionstamp, ByteBuffer> processBatchConcurrentOr(Transaction tr, List<Versionstamp> versionstamps, List<PhysicalFilter> filters) {
        Map<Versionstamp, ByteBuffer> batchResults = new LinkedHashMap<>();

        if (versionstamps.isEmpty()) {
            return batchResults;
        }

        // Step 1: Use concurrent document retrieval following standard pattern
        OrderedDocumentRetriever docRetriever = new OrderedDocumentRetriever();
        Map<Versionstamp, ByteBuffer> allDocuments = docRetriever.retrieveDocumentsConcurrent(tr, versionstamps);

        // Step 2: Apply OR filtering to retrieved documents
        for (Map.Entry<Versionstamp, ByteBuffer> docEntry : allDocuments.entrySet()) {
            Versionstamp versionstamp = docEntry.getKey();
            ByteBuffer document = docEntry.getValue();

            if (document != null) {
                // Apply OR logic - document passes if ANY filter condition is true
                boolean passesAnyFilter = false;
                for (PhysicalFilter filter : filters) {
                    ByteBuffer filteredDoc;
                    if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                        // Skip filter evaluation for _id filters since they're virtual
                        filteredDoc = document.duplicate();
                    } else {
                        filteredDoc = filterEvaluator.applyPhysicalFilter(filter, document.duplicate());
                    }
                    if (filteredDoc != null) {
                        passesAnyFilter = true;
                        break; // Short-circuit on first match
                    }
                }

                if (passesAnyFilter) {
                    batchResults.put(versionstamp, document);
                }
            }
        }

        return batchResults;
    }

    /**
     * Processes a batch of document locations concurrently for AND filtering.
     * Each document is retrieved and filtered in parallel using virtual threads.
     * All filters must pass for a document to be included (AND logic).
     */
    private Map<Versionstamp, ByteBuffer> processBatchConcurrentAnd(List<DocumentRetriever.DocumentLocation> locations, List<PhysicalFilter> filters) {
        Map<Versionstamp, ByteBuffer> batchResults = new LinkedHashMap<>();

        if (locations.isEmpty()) {
            return batchResults;
        }

        // Use a semaphore to limit concurrent operations
        Semaphore semaphore = new Semaphore(MAX_CONCURRENT_DOCUMENTS);

        // Thread-safe map to collect results while preserving order
        Map<Versionstamp, ByteBuffer> concurrentResults = new ConcurrentHashMap<>();

        // Store futures to wait for complete pipeline execution
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // Process each document concurrently
        for (DocumentRetriever.DocumentLocation location : locations) {
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                try {
                    semaphore.acquire();

                    // Retrieve document from storage
                    ByteBuffer document = documentRetriever.retrieveDocument(config.getMetadata(), location);
                    if (document == null) {
                        return null;
                    }

                    // Apply all AND filters to this document
                    boolean passesAllFilters = true;
                    ByteBuffer filteredDoc = document;

                    for (PhysicalFilter filter : filters) {
                        if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                            // Skip filter evaluation for _id filters since they're virtual
                            filteredDoc = filteredDoc.duplicate();
                        } else {
                            filteredDoc = filterEvaluator.applyPhysicalFilter(filter, filteredDoc.duplicate());
                        }
                        if (filteredDoc == null) {
                            passesAllFilters = false;
                            break; // Short-circuit on first failed filter (AND logic)
                        }
                    }

                    if (passesAllFilters) {
                        return Map.entry(location.documentId(), document);
                    }
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException("Error processing document " + location.documentId() + ": " + e.getMessage(), e);
                } finally {
                    semaphore.release();
                }
            }, virtualThreadExecutor).thenAccept(result -> {
                if (result != null) {
                    concurrentResults.put(result.getKey(), result.getValue());
                }
            }).exceptionally(throwable -> {
                // Log error but don't fail the entire batch
                System.err.println("Failed to process document: " + throwable.getMessage());
                return null;
            });

            futures.add(future);
        }

        try {
            // Wait for ALL futures to complete (both supply and accept phases)
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Concurrent AND processing was interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Concurrent AND processing failed", e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Concurrent AND processing timed out after " + OPERATION_TIMEOUT.toSeconds() + " seconds", e);
        }

        // Preserve insertion order by iterating through original locations
        for (DocumentRetriever.DocumentLocation location : locations) {
            ByteBuffer document = concurrentResults.get(location.documentId());
            if (document != null) {
                batchResults.put(location.documentId(), document);
            }
        }

        return batchResults;
    }

    /**
     * Executes mixed OR with both indexed and non-indexed conditions.
     * Enhanced with proper cursor coordination for complex query scenarios.
     */
    private Map<Versionstamp, ByteBuffer> executeMixedOr(Transaction tr, List<PhysicalNode> indexBasedPlans,
                                                         List<PhysicalNode> nonIndexBasedPlans) {
        Map<Versionstamp, ByteBuffer> finalResults = new LinkedHashMap<>();
        Map<Versionstamp, ByteBuffer> indexResults = new LinkedHashMap<>();
        Map<Versionstamp, ByteBuffer> nonIndexResults = new LinkedHashMap<>();

        // Generate unique node IDs for independent cursor tracking
        int indexedNodeId = generatePlanNodeId(indexBasedPlans);
        int nonIndexedNodeId = generatePlanNodeId(nonIndexBasedPlans);

        // Process indexed results with isolated node cursor tracking
        if (!indexBasedPlans.isEmpty()) {
            indexResults = executeIndexBasedOrWithNode(tr, indexBasedPlans, indexedNodeId);
            finalResults.putAll(indexResults);
        }

        // Process non-indexed results with isolated node cursor tracking
        if (!nonIndexBasedPlans.isEmpty() && finalResults.size() < config.limit()) {
            nonIndexResults = executeNonIndexedOrWithNode(tr, nonIndexBasedPlans, nonIndexedNodeId);

            // Merge results with proper deduplication and ordering
            Map<Versionstamp, ByteBuffer> orderedMerge = mergeOrderedResults(finalResults, nonIndexResults, config.limit());
            finalResults.clear();
            finalResults.putAll(orderedMerge);
        }

        // Use enhanced cursor coordination for better pagination management
        coordinateAndSetMixedQueryCursors(indexedNodeId, nonIndexedNodeId,
                indexResults, nonIndexResults, finalResults);

        return finalResults;
    }

    /**
     * Merges two ordered result sets while maintaining sort order and respecting the limit.
     * This ensures proper result ordering for mixed queries with both indexed and non-indexed results.
     */
    private Map<Versionstamp, ByteBuffer> mergeOrderedResults(Map<Versionstamp, ByteBuffer> results1,
                                                              Map<Versionstamp, ByteBuffer> results2,
                                                              int limit) {
        Map<Versionstamp, ByteBuffer> merged = new LinkedHashMap<>();

        // Use iterators to merge in sorted order
        Iterator<Map.Entry<Versionstamp, ByteBuffer>> iter1 = results1.entrySet().iterator();
        Iterator<Map.Entry<Versionstamp, ByteBuffer>> iter2 = results2.entrySet().iterator();

        Map.Entry<Versionstamp, ByteBuffer> entry1 = iter1.hasNext() ? iter1.next() : null;
        Map.Entry<Versionstamp, ByteBuffer> entry2 = iter2.hasNext() ? iter2.next() : null;

        while ((entry1 != null || entry2 != null) && merged.size() < limit) {
            if (entry1 == null) {
                // Only entry2 remaining
                if (!merged.containsKey(entry2.getKey())) {
                    merged.put(entry2.getKey(), entry2.getValue());
                }
                entry2 = iter2.hasNext() ? iter2.next() : null;
            } else if (entry2 == null) {
                // Only entry1 remaining
                if (!merged.containsKey(entry1.getKey())) {
                    merged.put(entry1.getKey(), entry1.getValue());
                }
                entry1 = iter1.hasNext() ? iter1.next() : null;
            } else {
                // Both entries available, choose based on sort order
                int comparison = config.isReverse()
                        ? entry2.getKey().compareTo(entry1.getKey())
                        : entry1.getKey().compareTo(entry2.getKey());

                if (comparison <= 0) {
                    // entry1 comes first (or they're equal)
                    if (!merged.containsKey(entry1.getKey())) {
                        merged.put(entry1.getKey(), entry1.getValue());
                    }
                    entry1 = iter1.hasNext() ? iter1.next() : null;

                    // If they're equal, also advance entry2
                    if (comparison == 0) {
                        entry2 = iter2.hasNext() ? iter2.next() : null;
                    }
                } else {
                    // entry2 comes first
                    if (!merged.containsKey(entry2.getKey())) {
                        merged.put(entry2.getKey(), entry2.getValue());
                    }
                    entry2 = iter2.hasNext() ? iter2.next() : null;
                }
            }
        }

        return merged;
    }

    private DirectorySubspace getIdIndexSubspace(BucketMetadata metadata) {
        DirectorySubspace idIndexSubspace = metadata.indexes().getSubspace(DefaultIndexDefinition.ID.selector());
        if (idIndexSubspace == null) {
            throw new RuntimeException("ID index not found for bucket: " + metadata.name());
        }
        return idIndexSubspace;
    }

    /**
     * Creates and returns a ScanRangeSelectors object, which defines the key range to be scanned
     * within an index subspace based on the cursor versionstamp configuration.
     *
     * @param idIndexSubspace the DirectorySubspace representing the index subspace for the operations
     * @return a ScanRangeSelectors object containing the begin and end KeySelectors for the range scan
     */
    private ScanRangeSelectors createScanRangeSelectors(DirectorySubspace idIndexSubspace) {
        // Use context-aware cursor approach for full bucket scans
        CompositeQueryContext context = CompositeQueryContext.fullBucketScan(null);
        Versionstamp cursorVersionstamp = cursorManager.getContextAwareCursorVersionstamp(config, context);

        // Create bounds from the cursor if available
        Bounds bounds = null;
        if (cursorVersionstamp != null) {
            Operator op = config.isReverse() ? Operator.LT : Operator.GT;
            Bound cursorBound = new Bound(op, new VersionstampVal(cursorVersionstamp));
            bounds = new Bounds(config.isReverse() ? null : cursorBound, config.isReverse() ? cursorBound : null);
        }

        SelectorPair selectors = selectorCalculator.calculateCursorAwareScanRange(idIndexSubspace, bounds);
        return new ScanRangeSelectors(selectors.beginSelector(), selectors.endSelector());
    }

    private boolean isIndexBasedPlan(PhysicalNode plan) {
        return switch (plan) {
            case PhysicalIndexScan ignored -> true;
            case PhysicalRangeScan ignored -> true;
            case PhysicalFilter filter -> {
                // Check if there's an index for this field
                String selector = filter.selector();
                IndexDefinition definition = config.getMetadata().indexes().getIndexBySelector(selector);
                yield definition != null;
            }
            case PhysicalFullScan fullScan -> // Check if the nested node can use an index
                    isIndexBasedPlan(fullScan.node());
            // CRITICAL: Recursive index classification for nested nodes
            case PhysicalAnd and -> and.children().stream().anyMatch(this::isIndexBasedPlan);
            case PhysicalOr or -> or.children().stream().anyMatch(this::isIndexBasedPlan);
            default -> false;
        };
    }

    private Map<Versionstamp, ByteBuffer> executeChildPlan(Transaction tr, PhysicalNode childPlan) {
        // Use hierarchical cursor management for nested operations
        boolean isNestedOperation = (childPlan instanceof PhysicalAnd || childPlan instanceof PhysicalOr);

        if (isNestedOperation && hasNestedCursorContext()) {
            // We're in nested execution - use cursor context management
            pushNestedCursorContext(childPlan.id());
            return executeChildPlanInternal(tr, childPlan);
        }
        return executeChildPlanInternal(tr, childPlan);
    }

    private Map<Versionstamp, ByteBuffer> executeChildPlanInternal(Transaction tr, PhysicalNode childPlan) {
        return switch (childPlan) {
            case PhysicalFilter ignored -> executeIndexScan(tr, childPlan);
            case PhysicalIndexScan indexScan -> executeIndexScan(tr, indexScan.node());
            case PhysicalFullScan fullScan -> executeFullBucketScan(tr, fullScan.node());
            case PhysicalAnd andPlan -> executePhysicalAnd(tr, andPlan);
            case PhysicalOr orPlan -> executePhysicalOr(tr, orPlan);
            case PhysicalRangeScan rangeScan -> executeRangeScan(tr, rangeScan);
            default -> new HashMap<>();
        };
    }

    private ByteBuffer applyNonIndexPlan(PhysicalNode plan, ByteBuffer document) {
        return switch (plan) {
            case PhysicalFilter filter -> {
                // Skip filter evaluation for _id filters since they should be index-based
                if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                    yield document; // _id filters should not be in non-index plans
                } else {
                    yield filterEvaluator.applyPhysicalFilter(filter, document);
                }
            }
            case PhysicalFullScan fullScan -> applyNonIndexPlan(fullScan.node(), document);
            case PhysicalAnd andFilter -> filterEvaluator.applyPhysicalAnd(andFilter, document);
            default -> document; // Pass through for unsupported types
        };
    }

    private ByteBuffer applyNeOperatorFiltering(List<PhysicalNode> indexBasedPlans, ByteBuffer document) {
        ByteBuffer filteredDocument = document;

        for (PhysicalNode plan : indexBasedPlans) {
            PhysicalFilter filter = null;
            if (plan instanceof PhysicalFilter directFilter) {
                filter = directFilter;
            } else if (plan instanceof PhysicalIndexScan(
                    int ignored, PhysicalNode node
            ) && node instanceof PhysicalFilter scanFilter) {
                filter = scanFilter;
            }

            // Only apply document-level filtering for NE operators
            if (filter != null && filter.op() == Operator.NE) {
                if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                    // Skip filter evaluation for _id filters since they're virtual
                    // For NE operations on _id, the index should handle this
                    filteredDocument = filteredDocument.duplicate();
                } else {
                    filteredDocument = filterEvaluator.applyPhysicalFilter(filter, filteredDocument.duplicate());
                }
                if (filteredDocument == null) {
                    return null;
                }
            }
        }

        return filteredDocument;
    }

    private Map<Versionstamp, ByteBuffer> executeMixedAnd(Transaction tr, List<PhysicalNode> indexBasedPlans, List<PhysicalNode> nonIndexBasedPlans) {
        // Create scanners for index-based plans
        List<IndexScannerExtracted> scanners = createIndexScannersForPlans(tr, indexBasedPlans);
        if (scanners == null) {
            return new HashMap<>();
        }

        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        int batchSize = getAppropriateLimit();

        // Use concurrent batch-intersect-continue algorithm for better performance
        ConcurrentBatchProcessor concurrentProcessor = new ConcurrentBatchProcessor();
        while (results.size() < config.limit()) {
            ConcurrentBatchIntersectionResult batchResult = concurrentProcessor.performConcurrentBatchIntersection(scanners, batchSize);
            if (!batchResult.anyScannersActive()) {
                break;
            }
            if (batchResult.intersection() != null && !batchResult.intersection().isEmpty()) {
                int needed = config.limit() - results.size();
                // Get cursor position for filtering - use context-aware cursor for mixed AND operations
                CompositeQueryContext context = CompositeQueryContext.mixedQuery(
                        scanners.stream().map(IndexScannerExtracted::getIndexDefinition).collect(Collectors.toList()),
                        Set.of(), // Node IDs not directly available from scanners - use aggregated approach
                        null
                );
                Versionstamp cursorVersionstamp = cursorManager.getContextAwareCursorVersionstamp(config, context);
                List<Map.Entry<Versionstamp, Integer>> sortedEntries = getSortedVersionstampEntries(batchResult.intersection(), batchResult.versionstampToEntryId(), cursorVersionstamp);

                // Use concurrent document retrieval for better performance
                // Note: Don't limit entries here since we need to filter documents and might need more candidates
                Map<Versionstamp, ByteBuffer> batchDocuments = retrieveDocumentsConcurrent(tr, sortedEntries, sortedEntries.size());

                int found = 0;
                for (Map.Entry<Versionstamp, ByteBuffer> docEntry : batchDocuments.entrySet()) {
                    if (found >= needed) {
                        break;
                    }

                    Versionstamp versionstamp = docEntry.getKey();
                    ByteBuffer document = docEntry.getValue();

                    if (document != null) {
                        // Apply index-based filtering first (for NE operators)
                        ByteBuffer filteredDocument = applyNeOperatorFiltering(indexBasedPlans, document);
                        boolean passesAllFilters = filteredDocument != null;

                        // Apply non-index filters
                        if (passesAllFilters) {
                            for (PhysicalNode nonIndexPlan : nonIndexBasedPlans) {
                                ByteBuffer filteredDoc = applyNonIndexPlan(nonIndexPlan, filteredDocument);
                                if (filteredDoc == null) {
                                    passesAllFilters = false;
                                    break;
                                }
                                filteredDocument = filteredDoc; // Use the filtered result
                            }
                        }

                        if (passesAllFilters) {
                            results.put(versionstamp, document);
                            found++;
                        }
                    }
                }

                if (results.size() >= config.limit()) {
                    // Collect all involved index definitions and their last processed positions from scanners
                    Map<IndexDefinition, CursorManager.CursorPosition> scannerPositions = getIndexDefinitionCursorPositionMap(scanners);
                    cursorManager.setCursorFromScannerPositions(config, scannerPositions, results);
                    break;
                }
            }

            boolean anyExhausted = scanners.stream().anyMatch(scanner -> !scanner.hasMore());
            if (anyExhausted) {
                break;
            }
        }

        // Use node-based cursor management for mixed queries
        // Note: This efficiently handles document filtering for indexed AND operations using isolated node tracking
        if (!results.isEmpty()) {
            cursorManager.setCursorFromLastResultForNonIndexedPath(config, 0, results);
        }

        // Also maintain compatibility with existing cursor management
        setCursorFromAllScanners(scanners, results);
        return results;
    }

    /**
     * Creates scanners for index-based plans and validates they can be used.
     * Returns null if no valid scanners can be created.
     */
    private List<IndexScannerExtracted> createIndexScannersForPlans(Transaction tr, List<PhysicalNode> indexBasedPlans) {
        if (indexBasedPlans.isEmpty()) {
            return null;
        }

        List<IndexScannerExtracted> scanners = new ArrayList<>();
        for (PhysicalNode plan : indexBasedPlans) {
            IndexScannerExtracted scanner = createIndexScannerWithCursor(tr, plan);
            if (scanner != null) {
                scanners.add(scanner);
            }
        }

        return scanners.isEmpty() ? null : scanners;
    }

    /**
     * Retrieves documents concurrently with order preservation.
     * This method uses the OrderedDocumentRetriever utility for parallel document fetching.
     */
    private Map<Versionstamp, ByteBuffer> retrieveDocumentsConcurrent(Transaction tr, List<Map.Entry<Versionstamp, Integer>> sortedEntries, int limit) {
        if (sortedEntries.isEmpty()) {
            return new LinkedHashMap<>();
        }

        // Take only the necessary number of entries
        List<Map.Entry<Versionstamp, Integer>> limitedEntries = sortedEntries.stream()
                .limit(limit)
                .toList();

        // Extract just the versionstamps in order
        List<Versionstamp> orderedVersionstamps = limitedEntries.stream()
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        // Use OrderedDocumentRetriever for concurrent document retrieval
        OrderedDocumentRetriever docRetriever = new OrderedDocumentRetriever();
        return docRetriever.retrieveDocumentsConcurrent(tr, orderedVersionstamps);
    }

    /**
     * Performs the core batch-intersect-continue algorithm on a list of index scanners.
     *
     * <p>This method implements the heart of the AND operation by fetching batches from
     * multiple index scanners simultaneously and computing their intersection using
     * RoaringBitmaps for optimal performance.</p>
     *
     * <p><strong>Algorithm Steps:</strong></p>
     * <ol>
     *   <li><strong>Batch Fetching</strong>: Fetch next batch from each active scanner</li>
     *   <li><strong>Intersection Computation</strong>: Use RoaringBitmap.and() for fast intersection</li>
     *   <li><strong>Early Termination</strong>: Stop if any intersection becomes empty</li>
     *   <li><strong>Mapping Maintenance</strong>: Keep versionstamp-to-entryId mapping consistent</li>
     * </ol>
     *
     * <p><strong>Performance Benefits:</strong></p>
     * <ul>
     *   <li>RoaringBitmaps provide speedup over traditional set operations</li>
     *   <li>Memory-bounded operation with configurable batch sizes</li>
     *   <li>Early termination when intersection becomes empty</li>
     *   <li>Lazy evaluation - only fetches what's needed</li>
     * </ul>
     *
     * @param scanners  list of index scanners, each scanning a different condition
     * @param batchSize size of batch to fetch from each scanner (memory control)
     * @return BatchIntersectionResult containing intersection bitmap, versionstamp mapping, and scanner status
     */
    private BatchIntersectionResult performBatchIntersection(List<IndexScannerExtracted> scanners, int batchSize) {
        // Step 1: Fetch next batch from all active scanners
        boolean anyScannersActive = false;
        for (IndexScannerExtracted scanner : scanners) {
            if (scanner.hasMore()) {
                scanner.fetchNextBatch(batchSize);  // Fetch next batch of index entries
                anyScannersActive = true;
            }
        }

        // Early return if no scanners have more data
        if (!anyScannersActive) {
            return new BatchIntersectionResult(null, new HashMap<>(), false);
        }

        // Step 2: Compute intersection of all scanner results using RoaringBitmaps
        RoaringBitmap intersection = null;
        Map<Versionstamp, Integer> versionstampToEntryId = new HashMap<>();

        for (IndexScannerExtracted scanner : scanners) {
            RoaringBitmap scannerEntryIds = scanner.getAccumulatedEntryIds();

            if (intersection == null) {
                // Initialize intersection with first scanner's results
                intersection = scannerEntryIds.clone();
                versionstampToEntryId.putAll(scanner.getVersionstampToEntryIdMapping());
            } else {
                // Intersect with current scanner's results using RoaringBitmap.and()
                intersection.and(scannerEntryIds);

                // Remove versionstamps that are no longer in the intersection
                final RoaringBitmap finalIntersection = intersection;
                versionstampToEntryId.entrySet().removeIf(entry ->
                        !finalIntersection.contains(entry.getValue()));
            }

            // Early termination: if intersection becomes empty, no point in continuing
            if (intersection.isEmpty()) {
                break;
            }
        }

        return new BatchIntersectionResult(intersection, versionstampToEntryId, anyScannersActive);
    }

    /**
     * Executes index-based AND operation using a batch-intersect-continue algorithm.
     * For REVERSE=true, results are manually sorted by a field in descending order.
     */
    private Map<Versionstamp, ByteBuffer> executeIndexBasedAnd(Transaction tr, List<PhysicalNode> indexBasedPlans) {
        // Create scanners for index-based plans using batch-intersect-continue algorithm
        // This ensures proper handling of all operators including GT, LT, etc.
        List<IndexScannerExtracted> scanners = createIndexScannersForPlans(tr, indexBasedPlans);
        if (scanners == null) {
            return new HashMap<>();
        }

        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        int batchSize = getAppropriateLimit();

        // Create a concurrent batch processor for parallel scanning
        ConcurrentBatchProcessor batchProcessor = new ConcurrentBatchProcessor();

        while (results.size() < config.limit()) {
            // Use concurrent batch intersection for improved performance
            ConcurrentBatchIntersectionResult concurrentResult = batchProcessor.performConcurrentBatchIntersection(scanners, batchSize);
            if (!concurrentResult.anyScannersActive()) {
                break;
            }
            if (concurrentResult.intersection() != null && !concurrentResult.intersection().isEmpty()) {
                int needed = config.limit() - results.size();
                // Get cursor position for filtering - use context-aware cursor for index-based AND operations
                CompositeQueryContext context = CompositeQueryContext.multiIndexAnd(
                        scanners.stream().map(IndexScannerExtracted::getIndexDefinition).collect(Collectors.toList()),
                        Set.of(), // Node IDs not directly available from scanners - use aggregated approach
                        null
                );
                Versionstamp cursorVersionstamp = cursorManager.getContextAwareCursorVersionstamp(config, context);
                List<Map.Entry<Versionstamp, Integer>> sortedEntries = getSortedVersionstampEntries(concurrentResult.intersection(), concurrentResult.versionstampToEntryId(), cursorVersionstamp);

                // Use concurrent document retrieval for better performance
                // Note: Don't limit entries here since NE operators might filter out documents
                Map<Versionstamp, ByteBuffer> batchDocuments = retrieveDocumentsConcurrent(tr, sortedEntries, sortedEntries.size());

                int found = 0;
                for (Map.Entry<Versionstamp, ByteBuffer> docEntry : batchDocuments.entrySet()) {
                    if (found >= needed) {
                        break;
                    }

                    Versionstamp versionstamp = docEntry.getKey();
                    ByteBuffer document = docEntry.getValue();

                    if (document != null) {
                        // Apply filtering only for NE operators - index is the golden source of truth for other operators
                        ByteBuffer filteredDocument = applyNeOperatorFiltering(indexBasedPlans, document);
                        boolean passesAllFilters = filteredDocument != null;

                        if (passesAllFilters) {
                            results.put(versionstamp, document);
                            found++;
                        }
                    }
                }

                if (results.size() >= config.limit()) {
                    // Collect all involved index definitions and their last processed positions from scanners
                    Map<IndexDefinition, CursorManager.CursorPosition> scannerPositions = getIndexDefinitionCursorPositionMap(scanners);
                    cursorManager.setCursorFromScannerPositions(config, scannerPositions, results);
                    break;
                }
            }

            boolean anyExhausted = scanners.stream().anyMatch(scanner -> !scanner.hasMore());
            if (anyExhausted) {
                break;
            }
        }

        setCursorFromAllScanners(scanners, results);
        return results;
    }

    /**
     * Executes a PhysicalIndexIntersection operation using optimized multi-index intersection.
     *
     * <p>This method provides optimized execution for queries that can leverage multiple indexes
     * simultaneously, such as {@code name = "john" AND age = 25} when both fields have indexes.
     * It converts the intersection into the format expected by the batch-intersect-continue algorithm.</p>
     *
     * <p><strong>Algorithm:</strong></p>
     * <ol>
     *   <li><strong>Filter Conversion</strong>: Convert PhysicalFilter conditions to PhysicalIndexScan nodes</li>
     *   <li><strong>Delegation</strong>: Use the optimized executeIndexBasedAnd algorithm</li>
     *   <li><strong>RoaringBitmap Intersection</strong>: Leverage ultra-fast bitmap operations</li>
     *   <li><strong>Concurrent Processing</strong>: Parallel index scanning with virtual threads</li>
     * </ol>
     *
     * <p><strong>Performance:</strong> This method provides speedup over traditional
     * set intersection operations by utilizing RoaringBitmap technology and concurrent execution.</p>
     *
     * @param tr                FoundationDB transaction for database operations
     * @param indexIntersection intersection plan containing multiple indexed filter conditions
     * @return ordered map of versionstamps to documents that match ALL index conditions
     * @see #executeIndexBasedAnd(Transaction, List)
     * @see org.roaringbitmap.RoaringBitmap
     */
    Map<Versionstamp, ByteBuffer> executePhysicalIndexIntersection(Transaction tr, PhysicalIndexIntersection indexIntersection) {
        // Convert the PhysicalIndexIntersection to the format expected by executeIndexBasedAnd
        List<PhysicalNode> indexBasedPlans = indexIntersection.filters().stream()
                .map(filter -> (PhysicalNode) new PhysicalIndexScan(config.getPlannerContext().generateId(), filter))
                .toList();

        return executeIndexBasedAnd(tr, indexBasedPlans);
    }

    private Map<Versionstamp, ByteBuffer> executeNonIndexedAnd(Transaction tr, List<PhysicalNode> children) {
        if (children.isEmpty()) {
            return new HashMap<>();
        }

        // Extract all filters from the children
        List<PhysicalFilter> filters = extractFiltersFromPlans(children);
        if (filters.isEmpty()) {
            return new HashMap<>();
        }

        // Use ID index scanner with filter application - adapted batch-intersect-continue
        Map<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        int batchSize = getAppropriateLimit();

        // Scan ID index continuously until we have enough results
        DirectorySubspace idIndexSubspace = getIdIndexSubspace(config.getMetadata());

        // Construct begin selector from cursor or start from beginning
        KeySelector beginSelector;
        CompositeQueryContext context = CompositeQueryContext.fullBucketScan(null);
        Versionstamp cursorVersionstamp = cursorManager.getContextAwareCursorVersionstamp(config, context);

        if (cursorVersionstamp != null) {
            // Start from after the cursor position
            Tuple cursorKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), cursorVersionstamp);
            byte[] cursorKeyBytes = idIndexSubspace.pack(cursorKey);
            beginSelector = KeySelector.firstGreaterThan(cursorKeyBytes);
        } else {
            // Start from the beginning if no cursor
            Tuple startKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue());
            byte[] startKeyBytes = idIndexSubspace.pack(startKey);
            beginSelector = KeySelector.firstGreaterOrEqual(startKeyBytes);
        }

        // End selector for ID index
        Tuple endKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue() + 1);
        byte[] endKeyBytes = idIndexSubspace.pack(endKey);
        KeySelector endSelector = KeySelector.firstGreaterOrEqual(endKeyBytes);

        boolean hasMore = true;
        Versionstamp lastProcessedVersionstamp = null;

        // Concurrent batch processing loop adapted for non-indexed AND fields
        while (results.size() < config.limit() && hasMore) {
            AsyncIterable<KeyValue> idEntries = tr.getRange(beginSelector, endSelector, batchSize, config.isReverse());
            List<DocumentRetriever.DocumentLocation> batchLocations = new ArrayList<>();
            boolean foundEntries = false;

            // Collect document locations for concurrent processing
            for (KeyValue idEntry : idEntries) {
                foundEntries = true;

                // Extract versionstamp from ID index key
                Tuple idKeyTuple = idIndexSubspace.unpack(idEntry.getKey());
                Versionstamp versionstamp = (Versionstamp) idKeyTuple.get(1);
                lastProcessedVersionstamp = versionstamp;

                // Get document metadata
                IndexEntry indexEntryData = IndexEntry.decode(idEntry.getValue());
                EntryMetadata entryMetadata = EntryMetadata.decode(ByteBuffer.wrap(indexEntryData.entryMetadata()));
                DocumentRetriever.DocumentLocation location = new DocumentRetriever.DocumentLocation(versionstamp, indexEntryData.shardId(), entryMetadata);

                batchLocations.add(location);

                // Process batch when it reaches a reasonable size for concurrent processing
                if (batchLocations.size() >= Math.min(50, batchSize)) {
                    Map<Versionstamp, ByteBuffer> batchResults = processBatchConcurrentAnd(batchLocations, filters);

                    // Add results respecting the limit
                    for (Map.Entry<Versionstamp, ByteBuffer> entry : batchResults.entrySet()) {
                        if (results.size() >= config.limit()) {
                            break;
                        }
                        results.put(entry.getKey(), entry.getValue());
                    }

                    batchLocations.clear();

                    // Stop if we've reached the limit
                    if (results.size() >= config.limit()) {
                        break;
                    }
                }
            }

            // Process any remaining documents in the final batch
            if (!batchLocations.isEmpty() && results.size() < config.limit()) {
                Map<Versionstamp, ByteBuffer> batchResults = processBatchConcurrentAnd(batchLocations, filters);

                // Add results respecting the limit
                for (Map.Entry<Versionstamp, ByteBuffer> entry : batchResults.entrySet()) {
                    if (results.size() >= config.limit()) {
                        break;
                    }
                    results.put(entry.getKey(), entry.getValue());
                }
            }

            if (!foundEntries) {
                hasMore = false;
            } else {
                // Update begins selector for the next batch
                Tuple nextKey = Tuple.from(IndexSubspaceMagic.ENTRIES.getValue(), lastProcessedVersionstamp);
                byte[] nextKeyBytes = idIndexSubspace.pack(nextKey);
                beginSelector = KeySelector.firstGreaterThan(nextKeyBytes);
            }
        }

        // Intention: fix flaky behavior of testPhysicalAndExecutionLogicNoIndexes
        // Set the cursor based on results for the next iteration
        // This is a non-indexed AND path that scans ID index with document-level filtering
        // For non-indexed AND operations, we only need to set cursor once since all filters
        // operate on the same ID index scan. Using the first filter's nodeId is sufficient.
        if (!filters.isEmpty() && !results.isEmpty()) {
            PhysicalFilter firstFilter = filters.getFirst();
            cursorManager.setCursorFromLastResultForNonIndexedPath(config, firstFilter.id(), results);
        }

        return results;
    }

    private List<PhysicalFilter> extractFiltersFromPlans(List<PhysicalNode> plans) {
        List<PhysicalFilter> filters = new ArrayList<>();
        for (PhysicalNode plan : plans) {
            extractFiltersRecursively(plan, filters);
        }
        return filters;
    }

    // Range scan specific helper methods

    private void extractFiltersRecursively(PhysicalNode plan, List<PhysicalFilter> filters) {
        switch (plan) {
            case PhysicalFilter filter -> filters.add(filter);
            case PhysicalFullScan fullScan -> // Extract filter from full scan node
                    extractFiltersRecursively(fullScan.node(), filters);
            case PhysicalAnd andNode -> {
                for (PhysicalNode child : andNode.children()) {
                    extractFiltersRecursively(child, filters);
                }
            }
            default -> {
                // Other node types are not supported in filter extraction
            }
        }
    }

    private IndexScannerExtracted createIndexScannerWithCursor(Transaction tr, PhysicalNode plan) {
        return switch (plan) {
            case PhysicalIndexScan indexScan -> {
                PhysicalNode node = indexScan.node();
                if (node instanceof PhysicalFilter filter) {
                    IndexDefinition definition = config.getMetadata().indexes().getIndexBySelector(filter.selector());
                    if (definition != null) {
                        DirectorySubspace indexSubspace = config.getMetadata().indexes().getSubspace(filter.selector());
                        if (indexSubspace != null) {
                            yield new IndexScannerExtracted(tr, indexSubspace, definition, filter, config.isReverse(), cursorManager, indexUtils);
                        }
                    }
                }
                yield null;
            }
            case PhysicalFilter filter -> {
                IndexDefinition definition = config.getMetadata().indexes().getIndexBySelector(filter.selector());
                if (definition != null) {
                    DirectorySubspace indexSubspace = config.getMetadata().indexes().getSubspace(filter.selector());
                    if (indexSubspace != null) {
                        yield new IndexScannerExtracted(tr, indexSubspace, definition, filter, config.isReverse(), cursorManager, indexUtils);
                    }
                }
                yield null;
            }
            default -> null;
        };
    }

    /**
     * Executes a range scan operation for complex range queries.
     *
     * <p>This method handles queries with both upper and lower bounds on indexed fields,
     * such as {@code age >= 18 AND age <= 65}. It uses the SelectorCalculator for
     * precise boundary handling and supports both inclusive and exclusive bounds.</p>
     *
     * <p><strong>Supported Range Types:</strong></p>
     * <ul>
     *   <li><strong>Lower bound only</strong>: {@code field >= value} or {@code field > value}</li>
     *   <li><strong>Upper bound only</strong>: {@code field <= value} or {@code field < value}</li>
     *   <li><strong>Range queries</strong>: {@code field >= min AND field <= max}</li>
     *   <li><strong>Mixed bounds</strong>: {@code field > min AND field < max}</li>
     * </ul>
     *
     * <p><strong>Boundary Handling:</strong></p>
     * <ul>
     *   <li><strong>includeLower=true</strong>: Include documents with exact lower bound value</li>
     *   <li><strong>includeLower=false</strong>: Exclude documents with exact lower bound value</li>
     *   <li><strong>includeUpper=true</strong>: Include documents with exact upper bound value</li>
     *   <li><strong>includeUpper=false</strong>: Exclude documents with exact upper bound value</li>
     * </ul>
     *
     * <p><strong>Fallback Strategy:</strong> If no suitable index is available, falls back
     * to full bucket scan with composite filter evaluation.</p>
     *
     * @param tr        FoundationDB transaction for database operations
     * @param rangeScan range scan plan containing bounds and inclusion flags
     * @return ordered map of versionstamps to documents within the specified range
     */
    Map<Versionstamp, ByteBuffer> executeRangeScan(Transaction tr, PhysicalRangeScan rangeScan) {
        LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        IndexDefinition indexDefinition = rangeScan.index();

        if (indexDefinition == null) {
            throw new IllegalStateException("PhysicalRangeScan with null index should have been converted to PhysicalFullScan by optimizer");
        }

        // Get the index subspace
        DirectorySubspace indexSubspace = config.getMetadata().indexes().getSubspace(rangeScan.selector());
        if (indexSubspace == null) {
            throw new IllegalStateException("Index subspace not found for selector: " + rangeScan.selector());
        }

        // Continue scanning until we find results or exhaust the index
        while (true) {
            Bounds bounds = config.cursor().getBounds(rangeScan.id());

            // Calculate selectors using unified SelectorCalculator
            RangeScanContext context = new RangeScanContext(indexSubspace, config, bounds, rangeScan, indexDefinition);
            SelectorPair selectors = selectorCalculator.calculateSelectors(context);
            KeySelector beginSelector = selectors.beginSelector();
            KeySelector endSelector = selectors.endSelector();

            if (!processIndexEntriesAndHandleCursorForRangeScan(tr, beginSelector, endSelector, indexSubspace, indexDefinition, rangeScan, results)) {
                break;
            }
        }

        return results;
    }

    private boolean processIndexEntriesAndHandleCursorForRangeScan(Transaction tr, KeySelector beginSelector, KeySelector endSelector,
                                                                   DirectorySubspace indexSubspace, IndexDefinition indexDefinition,
                                                                   PhysicalRangeScan rangeScan, Map<Versionstamp, ByteBuffer> results) {
        AsyncIterable<KeyValue> indexEntries = tr.getRange(beginSelector, endSelector, getAppropriateLimit(), config.isReverse());
        Versionstamp lastProcessedKey = null;
        BqlValue lastIndexValue = null;
        boolean hasIndexEntries = false;

        for (KeyValue indexEntry : indexEntries) {
            hasIndexEntries = true;
            DocumentRetriever.DocumentLocation location = documentRetriever.extractDocumentLocationFromIndexScan(indexSubspace, indexEntry);
            lastProcessedKey = location.documentId();

            // Extract index value for cursor management
            Tuple indexKeyTuple = indexSubspace.unpack(indexEntry.getKey());
            Object rawIndexValue = indexKeyTuple.get(1);
            lastIndexValue = cursorManager.createBqlValueFromIndexValue(rawIndexValue, indexDefinition.bsonType());

            try {
                ByteBuffer document = documentRetriever.retrieveDocument(config.getMetadata(), location);

                // Apply range filter validation (the range scan acts as its own filter)
                if (document != null && isDocumentInRange(document, rangeScan)) {
                    results.put(location.documentId(), document);
                }

                // Stop if we've reached the limit
                if (results.size() >= config.limit()) {
                    break;
                }

            } catch (Exception e) {
                throw new KronotopException(e);
            }
        }

        // Handle cursor advancement
        if (!results.isEmpty()) {
            // Found results - set cursor based on last processed values
            if (lastProcessedKey != null && lastIndexValue != null) {
                cursorManager.setCursorBoundsForIndexScan(config, rangeScan.id(), indexDefinition, lastIndexValue, lastProcessedKey);
            }
            return false; // Stop processing
        } else if (hasIndexEntries && lastProcessedKey != null) {
            // No results but processed entries - advance cursor and continue
            if (lastIndexValue != null) {
                cursorManager.setCursorBoundsForIndexScan(config, rangeScan.id(), indexDefinition, lastIndexValue, lastProcessedKey);
            }
            return true; // Continue processing
        } else {
            // No more entries
            return false; // Stop processing
        }
    }

    private boolean isDocumentInRange(ByteBuffer document, PhysicalRangeScan rangeScan) {
        // Apply the range filter to validate the document matches the range criteria
        if (rangeScan.lowerBound() != null) {
            Operator lowerOp = rangeScan.includeLower() ? Operator.GTE : Operator.GT;
            PhysicalFilter lowerFilter = new PhysicalFilter(config.getPlannerContext().generateId(), rangeScan.selector(), lowerOp, rangeScan.lowerBound());
            ByteBuffer filtered = filterEvaluator.applyPhysicalFilter(lowerFilter, document.duplicate());
            if (filtered == null) {
                return false;
            }
        }

        if (rangeScan.upperBound() != null) {
            Operator upperOp = rangeScan.includeUpper() ? Operator.LTE : Operator.LT;
            PhysicalFilter upperFilter = new PhysicalFilter(config.getPlannerContext().generateId(), rangeScan.selector(), upperOp, rangeScan.upperBound());
            ByteBuffer filtered = filterEvaluator.applyPhysicalFilter(upperFilter, document.duplicate());
            return filtered != null;
        }

        return true;
    }

    private void validateIndexOperandType(IndexDefinition definition, Object operand) {
        boolean typeMatches = switch (definition.bsonType()) {
            case STRING -> operand instanceof StringVal;
            case INT32 -> operand instanceof Int32Val;
            case INT64 -> operand instanceof Int64Val;
            case DOUBLE -> operand instanceof DoubleVal;
            case BOOLEAN -> operand instanceof BooleanVal;
            case BINARY -> {
                // Special case: _id index accepts VersionstampVal even though it's BINARY type
                if (operand instanceof VersionstampVal) {
                    yield DefaultIndexDefinition.ID.selector().equals(definition.selector());
                } else {
                    yield operand instanceof BinaryVal;
                }
            }
            case DATE_TIME -> operand instanceof DateTimeVal;
            case TIMESTAMP -> operand instanceof TimestampVal;
            case DECIMAL128 -> operand instanceof Decimal128Val;
            case NULL -> operand instanceof NullVal;
            default -> false;
        };
        if (!typeMatches) {
            throw new IllegalStateException("BSONType of the index (" + definition.bsonType() + ") doesn't match with PhysicalFilter's operand type (" + operand.getClass().getSimpleName() + ")");
        }
    }

    /**
     * Filters and sorts versionstamps from bitmap operations.
     * Common helper method to avoid code duplication.
     *
     * @param bitmap                bitmap containing matching entry IDs
     * @param versionstampToEntryId mapping from versionstamps to entry IDs
     * @param cursorVersionstamp    optional cursor position for filtering (null if no cursor)
     * @return a sorted list of versionstamp entries filtered by bitmap and cursor
     */
    private List<Map.Entry<Versionstamp, Integer>> getSortedVersionstampEntries(RoaringBitmap bitmap,
                                                                                Map<Versionstamp, Integer> versionstampToEntryId,
                                                                                Versionstamp cursorVersionstamp) {
        final RoaringBitmap finalBitmap = bitmap;

        return versionstampToEntryId.entrySet()
                .stream()
                .filter(entry -> finalBitmap.contains(entry.getValue()))
                .filter(entry -> cursorVersionstamp == null ||
                        (config.isReverse() ?
                                entry.getKey().compareTo(cursorVersionstamp) < 0 :
                                entry.getKey().compareTo(cursorVersionstamp) > 0))
                .sorted(config.isReverse() ?
                        Map.Entry.<Versionstamp, Integer>comparingByKey().reversed() :
                        Map.Entry.comparingByKey())
                .toList();
    }

    int getAppropriateLimit() {
        return Math.max(config.limit() * 10, 100);
    }

    /**
     * Generates a unique plan node ID from a list of physical plans.
     */
    private int generatePlanNodeId(List<PhysicalNode> plans) {
        return plans.hashCode(); // Simple hash-based ID generation
    }

    /**
     * Executes indexed OR operations with node cursor isolation.
     */
    private Map<Versionstamp, ByteBuffer> executeIndexBasedOrWithNode(Transaction tr,
                                                                      List<PhysicalNode> indexBasedPlans,
                                                                      int nodeId) {
        // Execute the indexed OR operation normally but track cursor independently
        Map<Versionstamp, ByteBuffer> results = executeIndexBasedOr(tr, indexBasedPlans);

        // Set node-specific cursor if we have results
        if (!results.isEmpty()) {
            cursorManager.setCursorBoundaries(config, nodeId, results);
        }

        return results;
    }

    // ========================================
    // Concurrent Execution Utility Classes
    // ========================================

    /**
     * Executes non-indexed OR operations with node cursor isolation.
     * This method uses node specific cursor to prevent contamination from indexed portion.
     */
    private Map<Versionstamp, ByteBuffer> executeNonIndexedOrWithNode(Transaction tr,
                                                                      List<PhysicalNode> nonIndexBasedPlans,
                                                                      int nodeId) {
        // Check if this node has a specific cursor position
        Bounds nodeBounds = config.cursor().getBounds(nodeId);

        // If there's no node specific cursor, we scan from the beginning for this node
        // This ensures the first execution finds all matching non-indexed documents
        if (nodeBounds == null) {
            // Execute with isolated context that ignores other node cursors
            return executeNonIndexedOrWithIsolatedContext(tr, nonIndexBasedPlans, nodeId);
        } else {
            // Execute with node cursor for pagination
            return executeNonIndexedOr(tr, nonIndexBasedPlans);
        }
    }

    /**
     * Executes non-indexed OR with isolated context to prevent cursor contamination.
     */
    private Map<Versionstamp, ByteBuffer> executeNonIndexedOrWithIsolatedContext(Transaction tr,
                                                                                 List<PhysicalNode> nonIndexBasedPlans,
                                                                                 int nodeId) {
        // Create a temporary isolated cursor state for this execution
        Cursor originalCursor = new Cursor();
        config.cursor().copyStatesTo(originalCursor);

        try {
            // Clear any cursor state that might interfere with this node
            config.cursor().clear();

            // Execute the non-indexed OR operation
            Map<Versionstamp, ByteBuffer> results = executeNonIndexedOr(tr, nonIndexBasedPlans);

            // Set node-specific cursor for future iterations
            if (!results.isEmpty()) {
                cursorManager.setCursorBoundaries(config, nodeId, results);
            }

            return results;
        } finally {
            // Restore original cursor state (preserving other nodes' cursors)
            config.cursor().copyStatesFrom(originalCursor);
        }
    }

    /**
     * Coordinates and sets cursors for mixed query operations using enhanced cursor coordination.
     * This method provides better pagination management across different node types.
     */
    private void coordinateAndSetMixedQueryCursors(int indexedNodeId,
                                                   int nonIndexedNodeId,
                                                   Map<Versionstamp, ByteBuffer> indexResults,
                                                   Map<Versionstamp, ByteBuffer> nonIndexResults,
                                                   Map<Versionstamp, ByteBuffer> finalResults) {

        // Create execution state tracking for coordination
        Map<Integer, CursorManager.NodeExecutionState> nodeStates = new HashMap<>();

        // Track indexed node state
        nodeStates.put(indexedNodeId,
                new CursorManager.NodeExecutionState(indexResults, indexResults.size() < config.limit()));

        // Track non-indexed node state
        nodeStates.put(nonIndexedNodeId,
                new CursorManager.NodeExecutionState(nonIndexResults, nonIndexResults.size() < config.limit()));

        // Use advanced cursor coordination
        cursorManager.advanceMixedQueryCursors(config, finalResults, nodeStates);

        // Also set individual node cursors
        cursorManager.setMixedQueryCursor(config, indexedNodeId, nonIndexedNodeId, indexResults, nonIndexResults);
    }

    /**
     * Collects cursor positions from all scanners and sets them using the cursor manager.
     * This eliminates duplicate cursor management code across multiple execution methods.
     *
     * @param scanners the list of scanners to collect positions from
     * @param results  the results map to pass to the cursor manager
     */
    private void setCursorFromAllScanners(List<IndexScannerExtracted> scanners,
                                          Map<Versionstamp, ByteBuffer> results) {
        if (results.isEmpty()) {
            return;
        }

        Map<IndexDefinition, CursorManager.CursorPosition> scannerPositions = getIndexDefinitionCursorPositionMap(scanners);
        cursorManager.setCursorFromScannerPositions(config, scannerPositions, results);
    }

    /**
     * Retrieves documents concurrently from their storage locations and applies filtering.
     * Optimized for full bucket scan operations with proper error handling and semaphore management.
     *
     * <p><strong>Concurrency Features:</strong></p>
     * <ul>
     *   <li><strong>Virtual Thread Execution</strong>: Uses {@code Context.getVirtualThreadPerTaskExecutor()}</li>
     *   <li><strong>Semaphore Throttling</strong>: Limits concurrent operations to {@code MAX_CONCURRENT_DOCUMENTS}</li>
     *   <li><strong>Order Preservation</strong>: Results maintain original location order using index-based sorting</li>
     *   <li><strong>Timeout Protection</strong>: Operations bounded by {@code OPERATION_TIMEOUT}</li>
     *   <li><strong>Sequential Fallback</strong>: Automatically uses sequential processing for ≤4 documents</li>
     * </ul>
     *
     * @param locations list of document locations to retrieve concurrently
     * @param plan      physical plan containing filter conditions to apply to each document
     * @return map of versionstamps to filtered document buffers, maintaining order
     * @throws KronotopException if concurrent retrieval fails or times out
     * @see #retrieveAndFilterDocumentsSequential(List, PhysicalNode)
     */
    private Map<Versionstamp, ByteBuffer> retrieveAndFilterDocumentsConcurrent(
            List<DocumentRetriever.DocumentLocation> locations, PhysicalNode plan) {

        if (locations.isEmpty()) {
            return new LinkedHashMap<>();
        }

        // For small batches, use sequential processing to avoid overhead
        if (locations.size() <= 4) {
            return retrieveAndFilterDocumentsSequential(locations, plan);
        }

        // Use concurrent document retrieval with semaphore for resource management
        LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
        Semaphore documentSemaphore = new Semaphore(Math.min(locations.size(), MAX_CONCURRENT_DOCUMENTS));
        List<CompletableFuture<DocumentFilterResult>> futures = new ArrayList<>();

        for (int i = 0; i < locations.size(); i++) {
            final int index = i;
            final DocumentRetriever.DocumentLocation location = locations.get(i);

            CompletableFuture<DocumentFilterResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    documentSemaphore.acquire();

                    // Retrieve document from Volume storage
                    ByteBuffer document = documentRetriever.retrieveDocument(config.getMetadata(), location);

                    if (document == null) {
                        return new DocumentFilterResult(index, location.documentId(), null);
                    }

                    // Apply plan-specific filtering based on the physical plan type
                    ByteBuffer filteredDocument = switch (plan) {
                        case PhysicalFilter filter -> {
                            // Skip filter evaluation for _id filters since index scan already filtered
                            if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                                yield document; // Index scan already performed filtering
                            } else {
                                // Apply document-level filter evaluation
                                yield filterEvaluator.applyPhysicalFilter(filter, document);
                            }
                        }
                        case PhysicalAnd andFilter ->
                            // Apply AND logic across multiple filter conditions
                                filterEvaluator.applyPhysicalAnd(andFilter, document);
                        default ->
                            // PhysicalFullScan, PhysicalTrue - return unfiltered documents
                                document;
                    };

                    return new DocumentFilterResult(index, location.documentId(), filteredDocument);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Document retrieval interrupted", e);
                } catch (Exception e) {
                    // Log the error but continue processing other documents
                    throw new KronotopException("Failed to retrieve document: " + location.documentId(), e);
                } finally {
                    documentSemaphore.release();
                }
            }, virtualThreadExecutor);

            futures.add(future);
        }

        try {
            // Wait for all document retrievals to complete with timeout
            CompletableFuture<Void> allDocuments = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
            allDocuments.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Process results in order and add non-null filtered documents
            futures.stream()
                    .map(CompletableFuture::join)
                    .sorted(Comparator.comparingInt(DocumentFilterResult::index))
                    .forEach(result -> {
                        if (result.filteredDocument() != null) {
                            results.put(result.versionstamp(), result.filteredDocument());
                        }
                    });

        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            throw new KronotopException("Concurrent document retrieval failed", e);
        }

        return results;
    }

    /**
     * Sequential fallback for small batches to avoid concurrency overhead.
     *
     * <p>This method is used automatically by {@link #retrieveAndFilterDocumentsConcurrent(List, PhysicalNode)}
     * when the batch size is ≤4 documents to avoid the overhead of virtual thread creation and management
     * for small operations.</p>
     *
     * <p><strong>Use Cases:</strong></p>
     * <ul>
     *   <li><strong>Small Batches</strong>: Automatically selected for ≤4 documents</li>
     *   <li><strong>Low Overhead</strong>: Avoids virtual thread creation costs</li>
     *   <li><strong>Same Logic</strong>: Applies identical filtering and error handling as concurrent version</li>
     * </ul>
     *
     * @param locations list of document locations to retrieve sequentially
     * @param plan      physical plan containing filter conditions to apply to each document
     * @return map of versionstamps to filtered document buffers, maintaining order
     * @throws KronotopException if document retrieval or filtering fails
     * @see #retrieveAndFilterDocumentsConcurrent(List, PhysicalNode)
     */
    private Map<Versionstamp, ByteBuffer> retrieveAndFilterDocumentsSequential(
            List<DocumentRetriever.DocumentLocation> locations, PhysicalNode plan) {

        LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();

        for (DocumentRetriever.DocumentLocation location : locations) {
            try {
                // Retrieve document from Volume storage
                ByteBuffer document = documentRetriever.retrieveDocument(config.getMetadata(), location);

                if (document == null) {
                    continue;
                }

                // Apply plan-specific filtering based on the physical plan type
                ByteBuffer filteredDocument = switch (plan) {
                    case PhysicalFilter filter -> {
                        // Skip filter evaluation for _id filters since index scan already filtered
                        if (DefaultIndexDefinition.ID.selector().equals(filter.selector())) {
                            yield document; // Index scan already performed filtering
                        } else {
                            // Apply document-level filter evaluation
                            yield filterEvaluator.applyPhysicalFilter(filter, document);
                        }
                    }
                    case PhysicalAnd andFilter ->
                        // Apply AND logic across multiple filter conditions
                            filterEvaluator.applyPhysicalAnd(andFilter, document);
                    default ->
                        // PhysicalFullScan, PhysicalTrue - return unfiltered documents
                            document;
                };

                // Add to results if document passed all filters
                if (filteredDocument != null) {
                    results.put(location.documentId(), filteredDocument);
                }

            } catch (Exception e) {
                throw new KronotopException("Failed to retrieve document: " + location.documentId(), e);
            }
        }

        return results;
    }

    /**
     * Pushes a new cursor context onto the stack for nested operation.
     * Inherits cursor boundaries from current state.
     *
     * @param nodeId the physical node ID
     */
    private void pushNestedCursorContext(int nodeId) {
        // Inherit cursor boundaries from current state
        Map<IndexDefinition, Bounds> inherited = new HashMap<>();
        Bounds currentBounds = config.cursor().getBounds(nodeId);
        if (currentBounds != null) {
            inherited.put(DefaultIndexDefinition.ID, currentBounds);
        }

        CursorContext context = new CursorContext(nodeId, inherited, new ArrayList<>());
        CURSOR_STACK.get().push(context);
    }

    /**
     * Pops cursor context from stack and consolidates cursor boundaries.
     * Updates parent cursor state based on nested operation results.
     *
     * @param nodeId  the physical node ID
     * @param results results from the nested operation
     */
    private void popNestedCursorContext(int nodeId, Map<Versionstamp, ByteBuffer> results) {
        if (!CURSOR_STACK.get().isEmpty()) {
            // Consolidate cursor boundaries from nested operation results
            if (!results.isEmpty()) {
                consolidateNestedCursorBoundaries(nodeId, results);
            }
        }
    }

    /**
     * Consolidates cursor boundaries after nested operation completion.
     * Ensures proper cursor advancement for pagination continuation.
     *
     * @param nodeId  the node ID that completed execution
     * @param results results from the nested operation
     */
    private void consolidateNestedCursorBoundaries(int nodeId,
                                                   Map<Versionstamp, ByteBuffer> results) {
        if (results.isEmpty()) {
            return;
        }

        // Determine cursor advancement direction
        Versionstamp cursorKey;
        Operator cursorOperator;

        if (config.isReverse()) {
            // For reverse scans, cursor should continue from smallest document
            cursorKey = results.keySet().stream().min(Versionstamp::compareTo).orElseThrow();
            cursorOperator = Operator.LT;
        } else {
            // For forward scans, cursor should continue from largest document
            cursorKey = results.keySet().stream().max(Versionstamp::compareTo).orElseThrow();
            cursorOperator = Operator.GT;
        }

        // Create cursor bound for ID index
        Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(cursorKey));

        // Set appropriate boundary based on scan direction
        Bounds newBounds;
        if (config.isReverse()) {
            newBounds = new Bounds(null, cursorBound);
        } else {
            newBounds = new Bounds(cursorBound, null);
        }

        // Update cursor state for this node
        config.cursor().setBounds(nodeId, newBounds);
    }

    /**
     * Checks if the cursor stack has any active contexts.
     * Useful for determining if we're in nested execution.
     *
     * @return true if nested execution is active
     */
    private boolean hasNestedCursorContext() {
        return !CURSOR_STACK.get().isEmpty();
    }

    /**
     * Executes a child plan with proper cursor context management.
     * Handles both nested and non-nested execution scenarios.
     *
     * @param tr           the FoundationDB transaction
     * @param child        the child plan to execute
     * @param parentNodeId the parent node ID for cursor coordination
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeChildPlanWithCursorProtection(Transaction tr, PhysicalNode child, int parentNodeId) {
        boolean isNestedOperation = (child instanceof PhysicalAnd || child instanceof PhysicalOr);

        if (isNestedOperation) {
            pushNestedCursorContext(child.id());
            try {
                Map<Versionstamp, ByteBuffer> results = executeChildPlan(tr, child);

                // Coordinate child results with parent cursor for hierarchical cursor management
                if (!results.isEmpty()) {
                    // For nested operations, ensure parent cursor reflects child progress
                    if (child instanceof PhysicalAnd) {
                        setCursorForAndResults(parentNodeId, results);
                    } else {
                        setCursorForOrResults(parentNodeId, results);
                    }
                }

                return results;
            } finally {
                popNestedCursorContext(child.id(), new HashMap<>());
            }
        } else {
            // For non-nested operations, execute and set cursor boundaries
            Map<Versionstamp, ByteBuffer> results = executeChildPlan(tr, child);

            // Set cursor boundaries using parentNodeId for proper coordination
            if (!results.isEmpty()) {
                // Use generic cursor setting for leaf operations
                cursorManager.setCursorBoundaries(config, parentNodeId, results);
            }

            return results;
        }
    }

    /**
     * Sets cursor boundaries for completed AND operation results.
     * Uses the most restrictive cursor position from all results.
     *
     * @param parentNodeId the AND node ID
     * @param results      the final AND results
     */
    private void setCursorForAndResults(int parentNodeId, Map<Versionstamp, ByteBuffer> results) {
        if (!results.isEmpty()) {
            // For AND operations: Use the most restrictive cursor (furthest progress)
            Versionstamp mostRestrictiveCursor = config.isReverse()
                    ? results.keySet().stream().min(Versionstamp::compareTo).orElse(null)
                    : results.keySet().stream().max(Versionstamp::compareTo).orElse(null);

            if (mostRestrictiveCursor != null) {
                Operator cursorOperator = config.isReverse() ? Operator.LT : Operator.GT;
                Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(mostRestrictiveCursor));
                Bounds newBounds = new Bounds(
                        config.isReverse() ? null : cursorBound,
                        config.isReverse() ? cursorBound : null
                );

                config.cursor().setBounds(parentNodeId, newBounds);
            }
        }
    }

    // ========================================
    // Phase 2: Hierarchical Cursor Management Framework
    // ========================================

    /**
     * Sets cursor boundaries for completed OR operation results.
     * Uses strategy appropriate for OR operations to avoid missing results.
     *
     * @param parentNodeId the OR node ID
     * @param results      the final OR results
     */
    private void setCursorForOrResults(int parentNodeId, Map<Versionstamp, ByteBuffer> results) {
        if (!results.isEmpty()) {
            // For OR operations: Use standard cursor advancement
            cursorManager.setCursorBoundaries(config, parentNodeId, results);
        }
    }

    /**
     * Classifies the execution strategy for a list of child nodes.
     * Considers index availability and nested operation complexity.
     *
     * @param children list of child physical nodes
     * @return appropriate execution strategy
     */
    private ExecutionStrategy classifyExecutionStrategyWithCursorAwareness(List<PhysicalNode> children) {
        boolean hasIndexBased = false;
        boolean hasNonIndexBased = false;
        boolean hasNestedOperators = false;

        for (PhysicalNode child : children) {
            if (child instanceof PhysicalAnd || child instanceof PhysicalOr) {
                hasNestedOperators = true;
            } else if (isIndexBasedPlan(child)) {
                hasIndexBased = true;
            } else {
                hasNonIndexBased = true;
            }
        }

        if (hasNestedOperators) return ExecutionStrategy.NESTED_COMPLEX;
        if (hasIndexBased && hasNonIndexBased) return ExecutionStrategy.MIXED;
        if (hasIndexBased) return ExecutionStrategy.PURE_INDEX_BASED;
        return ExecutionStrategy.PURE_NON_INDEXED;
    }

    /**
     * Executes index-based AND with cursor protection.
     * Uses existing executeIndexBasedAnd but ensures proper cursor management.
     *
     * @param tr              FoundationDB transaction
     * @param indexBasedPlans list of index-based child plans
     * @param parentNodeId    parent AND node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeIndexBasedAndWithCursorProtection(
            Transaction tr, List<PhysicalNode> indexBasedPlans, int parentNodeId) {

        // Execute the AND operation
        Map<Versionstamp, ByteBuffer> results = executeIndexBasedAnd(tr, indexBasedPlans);

        // Set cursor boundaries using parentNodeId for proper nested operation coordination
        setCursorForAndResults(parentNodeId, results);

        return results;
    }

    /**
     * Executes non-indexed AND with cursor protection.
     * Uses existing executeNonIndexedAnd but ensures proper cursor management.
     *
     * @param tr           FoundationDB transaction
     * @param children     list of non-indexed child plans
     * @param parentNodeId parent AND node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeNonIndexedAndWithCursorProtection(
            Transaction tr, List<PhysicalNode> children, int parentNodeId) {

        // Execute the non-indexed AND operation
        Map<Versionstamp, ByteBuffer> results = executeNonIndexedAnd(tr, children);

        // Set cursor boundaries using parentNodeId for proper nested operation coordination
        setCursorForAndResults(parentNodeId, results);

        return results;
    }

    /**
     * Executes mixed AND with cursor protection.
     * Uses existing executeMixedAnd but ensures proper cursor management.
     *
     * @param tr                 FoundationDB transaction
     * @param indexBasedPlans    list of index-based child plans
     * @param nonIndexBasedPlans list of non-indexed child plans
     * @param parentNodeId       parent AND node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeMixedAndWithCursorProtection(
            Transaction tr, List<PhysicalNode> indexBasedPlans, List<PhysicalNode> nonIndexBasedPlans, int parentNodeId) {

        // Execute the mixed AND operation (indexed + non-indexed plans)
        Map<Versionstamp, ByteBuffer> results = executeMixedAnd(tr, indexBasedPlans, nonIndexBasedPlans);

        // Set cursor boundaries using parentNodeId for proper nested operation coordination
        setCursorForAndResults(parentNodeId, results);

        return results;
    }

    /**
     * Executes index-based OR with cursor protection.
     * Uses existing executeIndexBasedOr but ensures proper cursor management.
     *
     * @param tr              FoundationDB transaction
     * @param indexBasedPlans list of index-based child plans
     * @param parentNodeId    parent OR node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeIndexBasedOrWithCursorProtection(
            Transaction tr, List<PhysicalNode> indexBasedPlans, int parentNodeId) {

        // Execute the index-based OR operation
        Map<Versionstamp, ByteBuffer> results = executeIndexBasedOr(tr, indexBasedPlans);

        // Set cursor boundaries using parentNodeId for proper nested operation coordination
        setCursorForOrResults(parentNodeId, results);

        return results;
    }

    /**
     * Executes non-indexed OR with cursor protection.
     * Uses existing executeNonIndexedOr but ensures proper cursor management.
     *
     * @param tr           FoundationDB transaction
     * @param children     list of non-indexed child plans
     * @param parentNodeId parent OR node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeNonIndexedOrWithCursorProtection(
            Transaction tr, List<PhysicalNode> children, int parentNodeId) {

        // Execute the non-indexed OR operation
        Map<Versionstamp, ByteBuffer> results = executeNonIndexedOr(tr, children);

        // Set cursor boundaries using parentNodeId for proper nested operation coordination
        setCursorForOrResults(parentNodeId, results);

        return results;
    }

    /**
     * Executes mixed OR with cursor protection.
     * Uses existing executeMixedOr but ensures proper cursor management.
     *
     * @param tr                 FoundationDB transaction
     * @param indexBasedPlans    list of index-based child plans
     * @param nonIndexBasedPlans list of non-indexed child plans
     * @param parentNodeId       parent OR node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeMixedOrWithCursorProtection(
            Transaction tr, List<PhysicalNode> indexBasedPlans, List<PhysicalNode> nonIndexBasedPlans, int parentNodeId) {

        // Execute the mixed OR operation (indexed + non-indexed plans)
        Map<Versionstamp, ByteBuffer> results = executeMixedOr(tr, indexBasedPlans, nonIndexBasedPlans);

        // Set cursor boundaries using parentNodeId for proper nested operation coordination
        setCursorForOrResults(parentNodeId, results);

        return results;
    }

    /**
     * Executes nested complex AND operations using RoaringBitmap optimization.
     * This is the enhanced version that uses RoaringBitmap for ultra-fast set operations.
     *
     * @param tr           FoundationDB transaction
     * @param children     list of child plans including nested operations
     * @param parentNodeId parent AND node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeNestedComplexAndWithRoaringBitmaps(
            Transaction tr, List<PhysicalNode> children, int parentNodeId) {

        // Check recursion depth safety
        int currentDepth = RECURSION_DEPTH.get();
        if (currentDepth >= MAX_RECURSION_DEPTH) {
            throw new RuntimeException("Maximum query recursion depth exceeded: " + currentDepth +
                    ". Query is too deeply nested.");
        }

        RECURSION_DEPTH.set(currentDepth + 1);
        try {
            return executeNestedComplexAndWithRoaringBitmapsInternal(tr, children, parentNodeId);
        } finally {
            RECURSION_DEPTH.set(currentDepth);
        }
    }

    // ========================================
    // Phase 3: Enhanced Execution with Cursor Protection
    // ========================================

    private Map<Versionstamp, ByteBuffer> executeNestedComplexAndWithRoaringBitmapsInternal(
            Transaction tr, List<PhysicalNode> children, int parentNodeId) {

        Map<Integer, VersionstampDocumentPair> entryIdToDocument = new HashMap<>();
        List<NestedExecutionResult> executionResults = new ArrayList<>();

        // Execute all children and collect their results as RoaringBitmaps
        for (int i = 0; i < children.size(); i++) {
            PhysicalNode child = children.get(i);
            int childNodeId = child.id(); // Use the child's actual node ID

            // Execute child with cursor protection
            Map<Versionstamp, ByteBuffer> childResults = executeChildPlanWithCursorProtection(tr, child, parentNodeId);

            // Track execution results for cursor coordination
            executionResults.add(new NestedExecutionResult(childNodeId, childResults));

            // For nested AND operations, perform intersection at document level to avoid EntryMetadata collision issues
            if (i == 0) {
                // Initialize with first child results
                for (Map.Entry<Versionstamp, ByteBuffer> entry : childResults.entrySet()) {
                    entryIdToDocument.put(entry.getKey().hashCode(), new VersionstampDocumentPair(entry.getKey(), entry.getValue()));
                }
            } else {
                // Intersect with subsequent children at document level
                Map<Integer, VersionstampDocumentPair> intersectedResults = new HashMap<>();
                for (Map.Entry<Versionstamp, ByteBuffer> entry : childResults.entrySet()) {
                    int tempId = entry.getKey().hashCode();
                    if (entryIdToDocument.containsKey(tempId)) {
                        intersectedResults.put(tempId, new VersionstampDocumentPair(entry.getKey(), entry.getValue()));
                    }
                }
                entryIdToDocument = intersectedResults;
            }

            // Early termination if any child has no results (AND semantics)
            if (childResults.isEmpty()) {
                // Set partial cursor state for proper pagination
                setPartialAndCursorState(parentNodeId, executionResults);
                return new HashMap<>();
            }
        }

        // Convert accumulated results back to final format
        Map<Versionstamp, ByteBuffer> finalResults = new LinkedHashMap<>();
        for (VersionstampDocumentPair pair : entryIdToDocument.values()) {
            finalResults.put(pair.versionstamp(), pair.document());
        }

        // Advanced cursor coordination for nested AND operations
        coordinateNestedAndCursor(parentNodeId, executionResults, finalResults);

        return finalResults;
    }

    /**
     * Executes nested complex OR operations using RoaringBitmap optimization.
     * This is the enhanced version that uses RoaringBitmap for ultra-fast set operations.
     *
     * @param tr           FoundationDB transaction
     * @param children     list of child plans including nested operations
     * @param parentNodeId parent OR node ID
     * @return execution results
     */
    private Map<Versionstamp, ByteBuffer> executeNestedComplexOrWithRoaringBitmaps(
            Transaction tr, List<PhysicalNode> children, int parentNodeId) {

        // Check recursion depth safety
        int currentDepth = RECURSION_DEPTH.get();
        if (currentDepth >= MAX_RECURSION_DEPTH) {
            throw new RuntimeException("Maximum query recursion depth exceeded: " + currentDepth +
                    ". Query is too deeply nested.");
        }

        RECURSION_DEPTH.set(currentDepth + 1);
        try {
            return executeNestedComplexOrWithRoaringBitmapsInternal(tr, children, parentNodeId);
        } finally {
            RECURSION_DEPTH.set(currentDepth);
        }
    }

    private Map<Versionstamp, ByteBuffer> executeNestedComplexOrWithRoaringBitmapsInternal(
            Transaction tr, List<PhysicalNode> children, int parentNodeId) {

        Map<Integer, VersionstampDocumentPair> entryIdToDocument = new HashMap<>();
        List<NestedExecutionResult> executionResults = new ArrayList<>();

        // Execute all children and collect their results as RoaringBitmaps
        for (PhysicalNode child : children) {
            int childNodeId = child.id(); // Use the child's actual node ID

            // Execute child with cursor protection
            Map<Versionstamp, ByteBuffer> childResults = executeChildPlanWithCursorProtection(tr, child, parentNodeId);

            // Track execution results for cursor coordination
            executionResults.add(new NestedExecutionResult(childNodeId, childResults));

            // For nested OR operations, perform union at document level to avoid EntryMetadata collision issues
            for (Map.Entry<Versionstamp, ByteBuffer> entry : childResults.entrySet()) {
                int tempId = entry.getKey().hashCode();
                if (!entryIdToDocument.containsKey(tempId)) {
                    entryIdToDocument.put(tempId, new VersionstampDocumentPair(entry.getKey(), entry.getValue()));
                }
            }
        }

        // Convert accumulated results back to final format
        Map<Versionstamp, ByteBuffer> finalResults = new LinkedHashMap<>();
        for (VersionstampDocumentPair pair : entryIdToDocument.values()) {
            finalResults.put(pair.versionstamp(), pair.document());
        }

        // Advanced cursor coordination for nested OR operations
        coordinateNestedOrCursor(parentNodeId, executionResults, finalResults);

        return finalResults;
    }

    /**
     * Advanced cursor coordination for nested AND operations.
     * Ensures proper cursor state for deeply nested operations with complex pagination.
     *
     * @param parentNodeId     the AND node ID
     * @param executionResults list of child execution results
     * @param finalResults     the final AND results
     */
    private void coordinateNestedAndCursor(int parentNodeId,
                                           List<NestedExecutionResult> executionResults,
                                           Map<Versionstamp, ByteBuffer> finalResults) {
        if (finalResults.isEmpty()) {
            return;
        }

        // For AND operations: Use the most restrictive cursor (furthest progress)
        Versionstamp mostRestrictiveCursor = config.isReverse()
                ? finalResults.keySet().stream().min(Versionstamp::compareTo).orElse(null)
                : finalResults.keySet().stream().max(Versionstamp::compareTo).orElse(null);

        if (mostRestrictiveCursor != null) {
            // Set cursor boundaries for all participating sub-queries
            for (NestedExecutionResult result : executionResults) {
                if (!result.results().isEmpty()) {
                    cursorManager.setCursorBoundaries(config, result.nodeId(), result.results());
                }
            }

            // Set main cursor boundary
            Operator cursorOperator = config.isReverse() ? Operator.LT : Operator.GT;
            Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(mostRestrictiveCursor));
            Bounds newBounds = new Bounds(
                    config.isReverse() ? null : cursorBound,
                    config.isReverse() ? cursorBound : null
            );

            config.cursor().setBounds(parentNodeId, newBounds);
        }
    }

    /**
     * Advanced cursor coordination for nested OR operations.
     * Ensures proper cursor state for deeply nested operations with complex pagination.
     *
     * @param parentNodeId     the OR node ID
     * @param executionResults list of child execution results
     * @param finalResults     the final OR results
     */
    private void coordinateNestedOrCursor(int parentNodeId,
                                          List<NestedExecutionResult> executionResults,
                                          Map<Versionstamp, ByteBuffer> finalResults) {
        if (finalResults.isEmpty()) {
            return;
        }

        // For OR operations: Use the least restrictive cursor (earliest position)
        Versionstamp leastRestrictiveCursor = null;

        for (NestedExecutionResult result : executionResults) {
            if (!result.results().isEmpty()) {
                // Set cursor boundaries for each node
                cursorManager.setCursorBoundaries(config, result.nodeId(), result.results());

                Versionstamp resultCursor = config.isReverse()
                        ? result.results().keySet().stream().max(Versionstamp::compareTo).orElse(null)
                        : result.results().keySet().stream().min(Versionstamp::compareTo).orElse(null);

                if (resultCursor != null) {
                    if (leastRestrictiveCursor == null) {
                        leastRestrictiveCursor = resultCursor;
                    } else {
                        // Choose least restrictive
                        if (config.isReverse()) {
                            leastRestrictiveCursor = resultCursor.compareTo(leastRestrictiveCursor) > 0
                                    ? resultCursor : leastRestrictiveCursor;
                        } else {
                            leastRestrictiveCursor = resultCursor.compareTo(leastRestrictiveCursor) < 0
                                    ? resultCursor : leastRestrictiveCursor;
                        }
                    }
                }
            }
        }

        if (leastRestrictiveCursor != null) {
            Operator cursorOperator = config.isReverse() ? Operator.LT : Operator.GT;
            Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(leastRestrictiveCursor));
            Bounds newBounds = new Bounds(
                    config.isReverse() ? null : cursorBound,
                    config.isReverse() ? cursorBound : null
            );

            config.cursor().setBounds(parentNodeId, newBounds);
        }
    }

    /**
     * Sets partial cursor state for AND operations that terminate early.
     * Used when some children have no results to ensure proper pagination continuation.
     *
     * @param parentNodeId     the AND node ID
     * @param executionResults list of child execution results processed so far
     */
    private void setPartialAndCursorState(int parentNodeId, List<NestedExecutionResult> executionResults) {
        // Find the execution result that progressed the furthest
        Versionstamp furthestProgress = null;

        for (NestedExecutionResult result : executionResults) {
            if (!result.results().isEmpty()) {
                Versionstamp resultProgress = config.isReverse()
                        ? result.results().keySet().stream().min(Versionstamp::compareTo).orElse(null)
                        : result.results().keySet().stream().max(Versionstamp::compareTo).orElse(null);

                if (resultProgress != null) {
                    if (furthestProgress == null) {
                        furthestProgress = resultProgress;
                    } else {
                        // Choose furthest progress
                        if (config.isReverse()) {
                            furthestProgress = resultProgress.compareTo(furthestProgress) < 0
                                    ? resultProgress : furthestProgress;
                        } else {
                            furthestProgress = resultProgress.compareTo(furthestProgress) > 0
                                    ? resultProgress : furthestProgress;
                        }
                    }
                }
            }
        }

        if (furthestProgress != null) {
            Operator cursorOperator = config.isReverse() ? Operator.LT : Operator.GT;
            Bound cursorBound = new Bound(cursorOperator, new VersionstampVal(furthestProgress));
            Bounds newBounds = new Bounds(
                    config.isReverse() ? null : cursorBound,
                    config.isReverse() ? cursorBound : null
            );

            config.cursor().setBounds(parentNodeId, newBounds);
        }
    }

    /**
     * Execution strategy classification for nested operations.
     * Determines the optimal execution approach based on child node characteristics.
     */
    private enum ExecutionStrategy {
        PURE_INDEX_BASED,    // All children are index-based
        PURE_NON_INDEXED,    // All children require full scan
        MIXED,               // Mix of index-based and non-indexed children
        NESTED_COMPLEX       // Contains nested AND/OR operations
    }

    /**
     * Result wrapper for concurrent document retrieval with order preservation.
     */
    private record DocumentResult(int index, Versionstamp versionstamp, ByteBuffer document) {
    }

    /**
     * Result wrapper for concurrent batch operations.
     */
    private record BatchResult(boolean hasMore, RoaringBitmap entryIds,
                               Map<Versionstamp, Integer> versionstampToEntryId) {
    }

    // ========================================
    // Phase 4: Nested Complex Operations with Cursor Coordination
    // ========================================

    /**
     * Enhanced batch intersection result with concurrent execution metadata.
     */
    private record ConcurrentBatchIntersectionResult(
            RoaringBitmap intersection,
            Map<Versionstamp, Integer> versionstampToEntryId,
            boolean anyScannersActive,
            int activeScannersCount
    ) {
    }

    /**
     * Enhanced batch union result with concurrent execution metadata.
     */
    private record ConcurrentBatchUnionResult(
            RoaringBitmap union,
            Map<Versionstamp, Integer> versionstampToEntryId,
            boolean anyScannersActive,
            int activeScannersCount
    ) {
    }

    private record ScanRangeSelectors(KeySelector beginSelector, KeySelector endSelector) {
    }

    /**
     * Result of a batch-intersect-continue operation containing the intersection and mapping.
     */
    private record BatchIntersectionResult(RoaringBitmap intersection, Map<Versionstamp, Integer> versionstampToEntryId,
                                           boolean anyScannersActive) {
    }

    /**
     * Result record for concurrent document retrieval and filtering operations.
     */
    private record DocumentFilterResult(int index, Versionstamp versionstamp, ByteBuffer filteredDocument) {
    }

    /**
     * Represents cursor context for a nested operation level.
     * Maintains inherited boundaries and active node IDs.
     */
    private record CursorContext(
            int nodeId,
            Map<IndexDefinition, Bounds> inheritedBounds,
            List<Integer> activeNodeIds
    ) {
    }

    /**
     * Supporting data structure for tracking nested operation execution results.
     * Maintains node ID and results for proper cursor coordination.
     */
    private record NestedExecutionResult(
            int nodeId,
            Map<Versionstamp, ByteBuffer> results
    ) {
    }

    /**
     * Supporting data structure for versionstamp-document pairs.
     */
    private record VersionstampDocumentPair(Versionstamp versionstamp, ByteBuffer document) {
    }

    /**
     * Concurrent batch processing utility for index scanners.
     */
    private class ConcurrentBatchProcessor {

        /**
         * Performs concurrent batch intersection with proper synchronization.
         */
        ConcurrentBatchIntersectionResult performConcurrentBatchIntersection(List<IndexScannerExtracted> scanners, int batchSize) {
            // Use semaphore to limit concurrent scanners
            Semaphore scannerSemaphore = new Semaphore(Math.min(scanners.size(), MAX_CONCURRENT_SCANNERS));

            // Submit all scanner tasks concurrently
            List<CompletableFuture<BatchResult>> futures = scanners.stream()
                    .map(scanner -> CompletableFuture.supplyAsync(() -> {
                        try {
                            scannerSemaphore.acquire();
                            if (scanner.hasMore()) {
                                scanner.fetchNextBatch(batchSize);
                                return new BatchResult(
                                        scanner.hasMore(),
                                        scanner.getAccumulatedEntryIds(),
                                        scanner.getVersionstampToEntryIdMapping()
                                );
                            } else {
                                return new BatchResult(false, new RoaringBitmap(), new HashMap<>());
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("Batch fetch interrupted", e);
                        } finally {
                            scannerSemaphore.release();
                        }
                    }, virtualThreadExecutor))
                    .toList();

            try {
                // Wait for all batches with timeout
                CompletableFuture<Void> allBatches = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                allBatches.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                // Process results and compute intersection
                List<BatchResult> results = futures.stream()
                        .map(CompletableFuture::join)
                        .toList();

                return computeIntersectionFromBatchResults(results);

            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                // Cancel remaining futures and fallback to sequential processing
                futures.forEach(future -> future.cancel(true));
                return performSequentialBatchIntersection(scanners, batchSize);
            }
        }

        /**
         * Computes intersection from batch results.
         */
        private ConcurrentBatchIntersectionResult computeIntersectionFromBatchResults(List<BatchResult> results) {
            int activeScannersCount = 0;
            boolean anyScannersActive = false;
            RoaringBitmap intersection = null;
            Map<Versionstamp, Integer> versionstampToEntryId = new HashMap<>();

            for (BatchResult result : results) {
                if (result.hasMore()) {
                    activeScannersCount++;
                    anyScannersActive = true;
                }

                RoaringBitmap scannerEntryIds = result.entryIds();
                if (intersection == null) {
                    intersection = scannerEntryIds.clone();
                    versionstampToEntryId.putAll(result.versionstampToEntryId());
                } else {
                    intersection.and(scannerEntryIds);
                    // Remove versionstamps that are no longer in the intersection
                    final RoaringBitmap finalIntersection = intersection;
                    versionstampToEntryId.entrySet().removeIf(entry ->
                            !finalIntersection.contains(entry.getValue()));
                }

                // Early termination if intersection becomes empty
                if (intersection.isEmpty()) {
                    break;
                }
            }

            if (intersection == null) {
                intersection = new RoaringBitmap();
            }

            return new ConcurrentBatchIntersectionResult(intersection, versionstampToEntryId, anyScannersActive, activeScannersCount);
        }

        /**
         * Fallback sequential batch intersection.
         */
        private ConcurrentBatchIntersectionResult performSequentialBatchIntersection(List<IndexScannerExtracted> scanners, int batchSize) {
            BatchIntersectionResult sequential = performBatchIntersection(scanners, batchSize);
            return new ConcurrentBatchIntersectionResult(
                    sequential.intersection(),
                    sequential.versionstampToEntryId(),
                    sequential.anyScannersActive(),
                    scanners.stream().mapToInt(scanner -> scanner.hasMore() ? 1 : 0).sum()
            );
        }

        /**
         * Performs concurrent batch union with proper synchronization.
         * Uses RoaringBitmap.or() for fast union operations with deduplication.
         */
        ConcurrentBatchUnionResult performConcurrentBatchUnion(List<IndexScannerExtracted> scanners, int batchSize) {
            // Use semaphore to limit concurrent scanners
            Semaphore scannerSemaphore = new Semaphore(Math.min(scanners.size(), MAX_CONCURRENT_SCANNERS));

            // Submit all scanner tasks concurrently
            List<CompletableFuture<BatchResult>> futures = scanners.stream()
                    .map(scanner -> CompletableFuture.supplyAsync(() -> {
                        try {
                            scannerSemaphore.acquire();
                            if (scanner.hasMore()) {
                                scanner.fetchNextBatch(batchSize);
                                return new BatchResult(
                                        scanner.hasMore(),
                                        scanner.getAccumulatedEntryIds(),
                                        scanner.getVersionstampToEntryIdMapping()
                                );
                            } else {
                                return new BatchResult(false, new RoaringBitmap(), new HashMap<>());
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return new BatchResult(false, new RoaringBitmap(), new HashMap<>());
                        } finally {
                            scannerSemaphore.release();
                        }
                    }, virtualThreadExecutor))
                    .toList();

            try {
                // Wait for all futures to complete with timeout
                List<BatchResult> results = new ArrayList<>();
                for (CompletableFuture<BatchResult> future : futures) {
                    results.add(future.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
                }

                return computeUnionFromBatchResults(results);

            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                // Log error and fall back to sequential processing
                // Note: Document retrieval failed - this is handled gracefully by excluding from results
                return performSequentialBatchUnion(scanners, batchSize);
            }
        }

        /**
         * Computes union from batch results with deduplication.
         */
        private ConcurrentBatchUnionResult computeUnionFromBatchResults(List<BatchResult> results) {
            int activeScannersCount = 0;
            boolean anyScannersActive = false;
            RoaringBitmap union = new RoaringBitmap();
            Map<Versionstamp, Integer> versionstampToEntryId = new HashMap<>();

            for (BatchResult result : results) {
                if (result.hasMore()) {
                    anyScannersActive = true;
                    activeScannersCount++;
                }

                // Perform a union operation with RoaringBitmap.or()
                union.or(result.entryIds());

                // Merge versionstamp mappings (no conflict resolution needed for OR)
                versionstampToEntryId.putAll(result.versionstampToEntryId());
            }

            return new ConcurrentBatchUnionResult(union, versionstampToEntryId, anyScannersActive, activeScannersCount);
        }

        /**
         * Fallback sequential batch union.
         */
        private ConcurrentBatchUnionResult performSequentialBatchUnion(List<IndexScannerExtracted> scanners, int batchSize) {
            // Sequential processing for union
            boolean anyScannersActive = false;
            RoaringBitmap union = new RoaringBitmap();
            Map<Versionstamp, Integer> versionstampToEntryId = new HashMap<>();

            for (IndexScannerExtracted scanner : scanners) {
                if (scanner.hasMore()) {
                    scanner.fetchNextBatch(batchSize);
                    anyScannersActive = true;
                }

                union.or(scanner.getAccumulatedEntryIds());
                versionstampToEntryId.putAll(scanner.getVersionstampToEntryIdMapping());
            }

            int activeScannersCount = scanners.stream().mapToInt(scanner -> scanner.hasMore() ? 1 : 0).sum();
            return new ConcurrentBatchUnionResult(union, versionstampToEntryId, anyScannersActive, activeScannersCount);
        }
    }

    /**
     * Concurrent document retrieval utility with order preservation.
     */
    private class OrderedDocumentRetriever {

        /**
         * Retrieves documents concurrently by versionstamp using index lookup.
         */
        LinkedHashMap<Versionstamp, ByteBuffer> retrieveDocumentsConcurrent(Transaction tr, List<Versionstamp> sortedVersionstamps) {
            if (sortedVersionstamps.isEmpty()) {
                return new LinkedHashMap<>();
            }

            // For small lists, use sequential retrieval
            if (sortedVersionstamps.size() <= 4) {
                return retrieveDocumentsSequential(tr, sortedVersionstamps);
            }

            // Use concurrent document retrieval with Transaction
            LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
            Semaphore documentSemaphore = new Semaphore(Math.min(sortedVersionstamps.size(), MAX_CONCURRENT_DOCUMENTS));

            List<CompletableFuture<DocumentResult>> futures = new ArrayList<>();
            for (int i = 0; i < sortedVersionstamps.size(); i++) {
                final int index = i;
                final Versionstamp versionstamp = sortedVersionstamps.get(i);

                CompletableFuture<DocumentResult> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        documentSemaphore.acquire();

                        // Use the method that requires Transaction
                        BucketMetadata metadata = config.getMetadata();
                        ByteBuffer document = documentRetriever.retrieveDocumentById(tr, versionstamp, metadata);
                        return new DocumentResult(index, versionstamp, document);

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Document retrieval interrupted", e);
                    } catch (Exception e) {
                        return new DocumentResult(index, versionstamp, null);
                    } finally {
                        documentSemaphore.release();
                    }
                }, virtualThreadExecutor);

                futures.add(future);
            }

            try {
                CompletableFuture<Void> allDocuments = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                allDocuments.get(OPERATION_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

                futures.stream()
                        .map(CompletableFuture::join)
                        .sorted(Comparator.comparingInt(DocumentResult::index))
                        .forEach(result -> {
                            if (result.document() != null) {
                                results.put(result.versionstamp(), result.document());
                            }
                        });

                return results;

            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                futures.forEach(future -> future.cancel(true));
                return retrieveDocumentsSequential(tr, sortedVersionstamps);
            }
        }

        /**
         * Fallback sequential document retrieval by versionstamp.
         */
        private LinkedHashMap<Versionstamp, ByteBuffer> retrieveDocumentsSequential(Transaction tr, List<Versionstamp> sortedVersionstamps) {
            LinkedHashMap<Versionstamp, ByteBuffer> results = new LinkedHashMap<>();
            BucketMetadata metadata = config.getMetadata();

            for (Versionstamp versionstamp : sortedVersionstamps) {
                try {
                    ByteBuffer document = documentRetriever.retrieveDocumentById(tr, versionstamp, metadata);
                    if (document != null) {
                        results.put(versionstamp, document);
                    }
                } catch (Exception e) {
                    throw new KronotopException(e);
                }
            }

            return results;
        }
    }
}
