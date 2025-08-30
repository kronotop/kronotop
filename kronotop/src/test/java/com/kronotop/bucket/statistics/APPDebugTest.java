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

package com.kronotop.bucket.statistics;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.BaseStandaloneInstanceTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test to analyze the testMergeThreshold failure.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class APPDebugTest extends BaseStandaloneInstanceTest {

    private FDBAdaptivePrefixHistogram histogram;
    private final String bucketName = "maintenance_test";
    private final String fieldName = "test_field";

    @BeforeEach
    public void setUp() {
        histogram = new FDBAdaptivePrefixHistogram(instance.getContext().getFoundationDB());
    }

    @Test
    public void debugMergeThresholdIssue() {
        // Same metadata as failing test
        APPHistogramMetadata metadata = new APPHistogramMetadata(
                3,     // maxDepth  
                4,     // fanout
                100,   // splitThreshold (high to prevent splits)
                10,    // mergeThreshold (high to enable merges)
                1      // shardCount
        );
        
        histogram.initialize(bucketName, fieldName, metadata);
        
        // Check initial state
        APPEstimator estimator = histogram.createEstimator(bucketName, fieldName);
        long countAfterInit = estimator.getTotalCount();
        System.out.println("Count after initialization: " + countAfterInit);
        
        // Print leaf structure after initialization
        printLeafStructure();
        
        // Add one item and check
        histogram.add(bucketName, fieldName, "item0".getBytes(StandardCharsets.UTF_8));
        long countAfterOne = estimator.getTotalCount();
        System.out.println("Count after adding 1 item: " + countAfterOne);
        
        // Add another item and check
        histogram.add(bucketName, fieldName, "item1".getBytes(StandardCharsets.UTF_8));
        long countAfterTwo = estimator.getTotalCount();
        System.out.println("Count after adding 2 items: " + countAfterTwo);
        
        // Print leaf structure after additions
        printLeafStructure();
        
        // Add remaining items
        for (int i = 2; i < 5; i++) {
            histogram.add(bucketName, fieldName, ("item" + i).getBytes(StandardCharsets.UTF_8));
        }
        
        long finalCount = estimator.getTotalCount();
        System.out.println("Final count after adding 5 items: " + finalCount);
        
        // Print final leaf structure
        printLeafStructure();
        
        // This should be 5, not 15
        assertEquals(5, finalCount, "Expected 5 items, got " + finalCount);
    }
    
    private void printLeafStructure() {
        APPHistogramMetadata metadata = histogram.getMetadata(bucketName, fieldName);
        System.out.println("=== Leaf Structure ===");
        
        try (var tr = histogram.getDatabase().createTransaction()) {
            var subspace = histogram.getHistogramSubspace(tr, bucketName, fieldName);
            
            byte[] beginKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX));
            byte[] endKey = subspace.pack(Tuple.from(APPKeySchema.LEAF_PREFIX + "\u0000"));
            
            Range range = new Range(beginKey, endKey);
            List<KeyValue> results = tr.getRange(range).asList().join();
            
            System.out.println("Found " + results.size() + " leaves:");
            for (KeyValue kv : results) {
                Tuple tuple = subspace.unpack(kv.getKey());
                byte[] compoundKey = (byte[]) tuple.get(1);
                APPLeaf leaf = APPLeaf.fromCompoundKey(compoundKey, metadata.maxDepth());
                
                long leafCount = histogram.getLeafCount(tr, subspace, leaf, metadata);
                System.out.println("  Leaf at depth " + leaf.depth() + 
                                 ", lowerBound=" + java.util.Arrays.toString(leaf.lowerBound()) +
                                 ", count=" + leafCount);
            }
        }
        System.out.println("=== End Leaf Structure ===");
    }
}