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

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class APPKeySchemaTest {

    @Test
    public void testRootLeafCoversAllSpace() {
        APPHistogramMetadata metadata = APPHistogramMetadata.defaultMetadata();
        
        // Create root leaf at depth 1
        byte[] rootLowerBound = new byte[]{(byte) 0, (byte) 0, (byte) 0}; // All zeros
        APPLeaf rootLeaf = APPLeaf.create(rootLowerBound, 1, metadata.maxDepth());
        
        System.out.println("Root leaf: " + rootLeaf);
        System.out.println("Lower bound: " + Arrays.toString(rootLeaf.lowerBound()));
        System.out.println("Upper bound: " + Arrays.toString(rootLeaf.upperBound()));
        
        // Test that it contains various keys
        byte[] testKey1 = "apple".getBytes(StandardCharsets.UTF_8); 
        byte[] testKey1Pad = APPKeySchema.canonicalizeLowerBound(testKey1, metadata.maxDepth());
        System.out.println("Test key 'apple': " + Arrays.toString(testKey1));
        System.out.println("Test key 'apple' padded: " + Arrays.toString(testKey1Pad));
        System.out.println("Contains 'apple': " + rootLeaf.contains(testKey1Pad));
        
        assertTrue(rootLeaf.contains(testKey1Pad), "Root leaf should contain 'apple'");
        
        // Test edge cases
        byte[] zeroKey = new byte[]{(byte) 0, (byte) 0, (byte) 0};
        assertTrue(rootLeaf.contains(zeroKey), "Root leaf should contain zero key");
        
        byte[] maxKey = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        // Note: maxKey might not be contained because upper bound is exclusive
        boolean containsMax = rootLeaf.contains(maxKey);
        System.out.println("Contains max key: " + containsMax);
    }
    
    @Test
    public void testBytesToLongConversion() {
        // Test the bytes-to-long conversion for different lengths
        byte[] test1 = new byte[]{1, 2, 3};
        long value1 = APPKeySchema.bytesToLong(test1);
        byte[] back1 = APPKeySchema.longToBytes(value1, 3);
        
        System.out.println("Original: " + Arrays.toString(test1));
        System.out.println("As long: " + value1);
        System.out.println("Back to bytes: " + Arrays.toString(back1));
        
        assertArrayEquals(test1, back1, "Round trip conversion should work");
    }
}