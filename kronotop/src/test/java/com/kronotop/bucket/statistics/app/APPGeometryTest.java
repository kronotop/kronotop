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

package com.kronotop.bucket.statistics.app;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class APPGeometryTest {
    
    @Test
    void testRightPadWithZeros() {
        byte[] input = {0x12, 0x34};
        byte[] expected = {0x12, 0x34, 0x00, 0x00};
        byte[] result = APPGeometry.rightPad(input, 4, (byte) 0x00);
        
        assertArrayEquals(expected, result);
    }
    
    @Test
    void testRightPadWithOnes() {
        byte[] input = {0x12, 0x34};
        byte[] expected = {0x12, 0x34, (byte) 0xFF, (byte) 0xFF};
        byte[] result = APPGeometry.rightPad(input, 4, (byte) 0xFF);
        
        assertArrayEquals(expected, result);
    }
    
    @Test
    void testRightPadTruncation() {
        byte[] input = {0x12, 0x34, 0x56, 0x78};
        byte[] expected = {0x12, 0x34};
        byte[] result = APPGeometry.rightPad(input, 2, (byte) 0x00);
        
        assertArrayEquals(expected, result);
    }
    
    @Test
    void testRightPadExactLength() {
        byte[] input = {0x12, 0x34};
        byte[] result = APPGeometry.rightPad(input, 2, (byte) 0x00);
        
        assertArrayEquals(input, result);
    }
    
    @Test
    void testRightPadNull() {
        assertThrows(IllegalArgumentException.class, () -> 
            APPGeometry.rightPad(null, 4, (byte) 0x00));
    }
    
    @Test
    void testLeafWidthDepth1() {
        // S(1) = 256^(3-1) = 256^2 = 65536 for maxDepth=3
        long width = APPGeometry.leafWidth(1, 3);
        assertEquals(65536L, width);
    }
    
    @Test
    void testLeafWidthDepth2() {
        // S(2) = 256^(3-2) = 256^1 = 256 for maxDepth=3
        long width = APPGeometry.leafWidth(2, 3);
        assertEquals(256L, width);
    }
    
    @Test
    void testLeafWidthDepth3() {
        // S(3) = 256^(3-3) = 256^0 = 1 for maxDepth=3
        long width = APPGeometry.leafWidth(3, 3);
        assertEquals(1L, width);
    }
    
    @Test
    void testLeafWidthInvalidDepth() {
        assertThrows(IllegalArgumentException.class, () -> 
            APPGeometry.leafWidth(0, 3));
        assertThrows(IllegalArgumentException.class, () -> 
            APPGeometry.leafWidth(4, 3));
    }
    
    @Test
    void testComputeUpperBound() {
        // Test at depth 2 with maxDepth 3
        // Width should be 256, so upper = lower + 256
        byte[] lowerPad = {0x12, 0x34, 0x00}; // maxDepth = 3
        byte[] expected = {0x12, 0x35, 0x00}; // 0x3400 + 256 = 0x3500
        
        byte[] result = APPGeometry.computeUpperBound(lowerPad, 2, 3);
        assertArrayEquals(expected, result);
    }
    
    @Test
    void testComputeParent() {
        // Child at depth 2, parent should be at depth 1
        byte[] childLowerPad = {0x12, 0x34, 0x00}; // depth 2
        APPLeafId child = new APPLeafId(childLowerPad, 2);
        
        APPLeafId parent = APPGeometry.computeParent(child);
        
        assertEquals(1, parent.depth());
        assertEquals(3, parent.getMaxDepth());
    }
    
    @Test
    void testComputeParentRootLeaf() {
        byte[] rootLowerPad = {0x00, 0x00, 0x00};
        APPLeafId root = new APPLeafId(rootLowerPad, 1);
        
        assertThrows(IllegalArgumentException.class, () -> 
            APPGeometry.computeParent(root));
    }
    
    @Test
    void testComputeChildren() {
        // Parent at depth 1, children should be at depth 2
        byte[] parentLowerPad = {0x00, 0x00, 0x00}; // depth 1
        APPLeafId parent = new APPLeafId(parentLowerPad, 1);
        
        List<APPLeafId> children = APPGeometry.computeChildren(parent);
        
        assertEquals(4, children.size());
        for (APPLeafId child : children) {
            assertEquals(2, child.depth());
            assertEquals(3, child.getMaxDepth());
        }
    }
    
    @Test
    void testComputeChildrenMaxDepth() {
        byte[] leafPad = {0x00, 0x00, 0x00};
        APPLeafId leaf = new APPLeafId(leafPad, 3); // Already at max depth
        
        assertThrows(IllegalArgumentException.class, () -> 
            APPGeometry.computeChildren(leaf));
    }
    
    @Test
    void testComputeSiblingsRoot() {
        byte[] rootPad = {0x00, 0x00, 0x00};
        APPLeafId root = new APPLeafId(rootPad, 1);
        
        List<APPLeafId> siblings = APPGeometry.computeSiblings(root);
        
        assertEquals(1, siblings.size());
        assertEquals(root, siblings.get(0));
    }
    
    @Test
    void testComputeSiblingsNonRoot() {
        byte[] leafPad = {0x12, 0x34, 0x00};
        APPLeafId leaf = new APPLeafId(leafPad, 2);
        
        List<APPLeafId> siblings = APPGeometry.computeSiblings(leaf);
        
        assertEquals(4, siblings.size());
        for (APPLeafId sibling : siblings) {
            assertEquals(2, sibling.depth());
        }
    }
    
    @Test
    void testIsLexicographicallyOrdered() {
        byte[] a = {0x12, 0x34};
        byte[] b = {0x12, 0x35};
        byte[] c = {0x13, 0x00};
        
        assertTrue(APPGeometry.isLexicographicallyOrdered(a, b));
        assertTrue(APPGeometry.isLexicographicallyOrdered(a, c));
        assertTrue(APPGeometry.isLexicographicallyOrdered(b, c));
        assertFalse(APPGeometry.isLexicographicallyOrdered(b, a));
        assertFalse(APPGeometry.isLexicographicallyOrdered(c, a));
    }
    
    @Test
    void testParentChildRelationship() {
        // Test that parent-child relationships are consistent
        byte[] parentPad = {0x00, 0x00, 0x00};
        APPLeafId parent = new APPLeafId(parentPad, 1);
        
        List<APPLeafId> children = APPGeometry.computeChildren(parent);
        
        for (APPLeafId child : children) {
            APPLeafId computedParent = APPGeometry.computeParent(child);
            assertEquals(parent.depth(), computedParent.depth());
            assertArrayEquals(parent.lowerPad(), computedParent.lowerPad());
        }
    }
    
    @Test
    void testSiblingRelationship() {
        // Test that siblings share the same parent
        byte[] leafPad = {0x12, 0x34, 0x00};
        APPLeafId leaf = new APPLeafId(leafPad, 2);
        
        List<APPLeafId> siblings = APPGeometry.computeSiblings(leaf);
        APPLeafId expectedParent = APPGeometry.computeParent(leaf);
        
        for (APPLeafId sibling : siblings) {
            if (sibling.depth() > 1) { // Skip root level
                APPLeafId siblingParent = APPGeometry.computeParent(sibling);
                assertEquals(expectedParent.depth(), siblingParent.depth());
                assertArrayEquals(expectedParent.lowerPad(), siblingParent.lowerPad());
            }
        }
    }
}