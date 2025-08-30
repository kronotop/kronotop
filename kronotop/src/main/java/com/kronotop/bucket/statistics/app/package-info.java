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

/**
 * Adaptive Prefix Partitioning (APP) histogram implementation for Kronotop.
 * 
 * <h2>Overview</h2>
 * This package implements the APP algorithm as described in the specification document.
 * APP is a lexicographic histogram for byte arrays that provides range selectivity 
 * estimation for query optimization.
 * 
 * <h2>Key Features</h2>
 * <ul>
 *   <li><strong>Self-Maintaining</strong> - No background workers required</li>
 *   <li><strong>Deterministic Geometry</strong> - Equal-width partitioning with quartile splits</li>
 *   <li><strong>Online Maintenance</strong> - Split/merge operations happen during writes</li>
 *   <li><strong>Hotspot Mitigation</strong> - Counter sharding for high-traffic leaves</li>
 *   <li><strong>Transactional</strong> - All operations within FoundationDB transactions</li>
 * </ul>
 * 
 * <h2>Core Classes</h2>
 * <ul>
 *   <li>{@link APPConfiguration} - Configuration constants and defaults</li>
 *   <li>{@link APPLeafId} - Unique identifier for histogram leaves</li>
 *   <li>{@link APPMetadata} - Metadata stored with each leaf</li>
 *   <li>{@link APPKeySchema} - FoundationDB key layout implementation</li>
 *   <li>{@link APPGeometry} - Geometric helper functions</li>
 * </ul>
 * 
 * <h2>Key Schema</h2>
 * The implementation uses the following FoundationDB key layout:
 * <pre>
 * HIST/&lt;indexId&gt;/L/&lt;lowerPad&gt;&lt;depthByte&gt;                = &lt;meta&gt;   # leaf boundary
 * HIST/&lt;indexId&gt;/C/&lt;lowerPad&gt;&lt;depthByte&gt;/&lt;shardId&gt;      = &lt;i64&gt;    # counter shards
 * HIST/&lt;indexId&gt;/F/&lt;lowerPad&gt;&lt;depthByte&gt;                = &lt;flags&gt;  # maintenance flags
 * IDX/&lt;indexId&gt;/&lt;valueBytes&gt;/&lt;docRef&gt;                   = ∅        # index entries
 * </pre>
 * 
 * <h2>Algorithm Details</h2>
 * <p>The APP histogram partitions the lexicographic space of byte arrays using a 
 * deterministic tree structure where:</p>
 * <ul>
 *   <li>Each leaf covers a range [L_pad, U_pad) of fixed width S(d) = 256^(D_max - d)</li>
 *   <li>Splits create 4 children using quartile geometry</li>
 *   <li>Merges consolidate 4 siblings back into their parent</li>
 *   <li>Range estimation uses partial coverage ratios across intersecting leaves</li>
 * </ul>
 * 
 * <h2>Implementation Status</h2>
 * <p><strong>Phase 1: Foundation & Architecture</strong> ✅ COMPLETE</p>
 * <ul>
 *   <li>✅ Core data structures and configuration</li>
 *   <li>✅ FoundationDB key schema</li>
 *   <li>✅ Geometric helper functions</li>
 *   <li>✅ Comprehensive unit tests</li>
 *   <li>✅ Configuration integration</li>
 * </ul>
 * 
 * <p><strong>Next Phases:</strong></p>
 * <ul>
 *   <li>Phase 2: Leaf lookup and boundary management</li>
 *   <li>Phase 3: Write path operations (Add/Delete/Update)</li>
 *   <li>Phase 4: Maintenance operations (Split/Merge)</li>
 *   <li>Phase 5: Range estimation engine</li>
 *   <li>Phase 6: Concurrency and performance optimizations</li>
 *   <li>Phase 7: Query optimizer integration</li>
 * </ul>
 * 
 * @author Kronotop Development Team
 * @since 0.13
 */
package com.kronotop.bucket.statistics.app;