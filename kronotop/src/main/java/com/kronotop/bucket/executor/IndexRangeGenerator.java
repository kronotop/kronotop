// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.executor;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexBuilder;
import com.kronotop.bucket.planner.physical.PhysicalIndexScan;

/**
 * The {@code IndexRangeGenerator} class provides functionality to create index ranges
 * based on specified values, index details, and subspaces. It defines methods to create
 * key-based ranges with specific boundaries determined from the given parameters.
 */
class IndexRangeGenerator {

    public static byte[] beginningOfIndexRange(Subspace indexSubspace, Index index) {
        return indexSubspace.pack(IndexBuilder.beginningOfIndexRange(index));
    }

    /**
     * Creates an {@code IndexRange} that begins with the first key greater than or equal to the specified value
     * and ends with the first key greater than or equal to the incremented starting range.
     *
     * @param physicalIndexScan the {@code PhysicalIndexScan} object that provides the index details
     * @param value             the value used to determine the lower boundary of the range
     * @param indexSubspace     the {@code Subspace} representing the index subspace in which the range is defined
     * @return an {@code IndexRange} defining the range with the specified beginning and end key selectors
     */
    protected static IndexRange greaterThanOrEqual(PhysicalIndexScan physicalIndexScan, Object value, Subspace indexSubspace) {
        Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(value);
        KeySelector begin = KeySelector.firstGreaterOrEqual(indexSubspace.pack(tuple));
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(beginningOfIndexRange(indexSubspace, physicalIndexScan.getIndex())));
        return new IndexRange(begin, end);
    }

    /**
     * Creates an {@code IndexRange} that begins with the first key greater than or equal to the packed tuple
     * created from the specified value and ends with the first key greater than or equal to the incremented key.
     *
     * @param physicalIndexScan the {@code PhysicalIndexScan} object that provides the index details
     * @param value             the value used to define the specific range for the index
     * @param indexSubspace     the {@code Subspace} representing the index subspace in which the range is defined
     * @return an {@code IndexRange} defining the range with the specified beginning and end key selectors
     */
    protected static IndexRange equal(PhysicalIndexScan physicalIndexScan, Object value, Subspace indexSubspace) {
        Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(value);
        byte[] key = indexSubspace.pack(tuple);
        KeySelector begin = KeySelector.firstGreaterOrEqual(key);
        KeySelector end = KeySelector.firstGreaterOrEqual(ByteArrayUtil.strinc(key));
        return new IndexRange(begin, end);
    }

    /**
     * Creates an {@code IndexRange} that begins with the first key greater than the packed tuple
     * constructed from the specified value and ends with the first key greater than the incremented key.
     *
     * @param physicalIndexScan the {@code PhysicalIndexScan} object that provides the index details
     * @param value             the value used to define the starting point of the range
     * @param indexSubspace     the {@code Subspace} representing the index subspace in which the range is defined
     * @return an {@code IndexRange} defining the range with the specified beginning and end key selectors
     */
    protected static IndexRange greaterThan(PhysicalIndexScan physicalIndexScan, Object value, Subspace indexSubspace) {
        Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(value);
        byte[] key = indexSubspace.pack(tuple);
        KeySelector begin = KeySelector.firstGreaterThan(key);
        KeySelector end = KeySelector.firstGreaterThan(ByteArrayUtil.strinc(beginningOfIndexRange(indexSubspace, physicalIndexScan.getIndex())));
        return new IndexRange(begin, end);
    }

    /**
     * Creates an {@code IndexRange} that begins with the first key greater than or equal to an empty key in the
     * specified index subspace and ends with the last key less than or equal to the packed tuple created
     * from the given value.
     *
     * @param physicalIndexScan the {@code PhysicalIndexScan} object that provides the index details
     * @param value             the value used to determine the upper boundary of the range
     * @param indexSubspace     the {@code Subspace} representing the index subspace in which the range is defined
     * @return an {@code IndexRange} defining the range with the specified beginning and end key selectors
     */
    protected static IndexRange lessThan(PhysicalIndexScan physicalIndexScan, Object value, Subspace indexSubspace) {
        Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(value);
        byte[] key = indexSubspace.pack(tuple);
        KeySelector begin = KeySelector.firstGreaterOrEqual(beginningOfIndexRange(indexSubspace, physicalIndexScan.getIndex()));
        KeySelector end = KeySelector.lastLessOrEqual(key);
        return new IndexRange(begin, end);
    }

    /**
     * Creates an {@code IndexRange} that begins with the first key greater than or equal to an empty key
     * in the specified index subspace and ends with the last key less than or equal to the packed tuple
     * created from the given value.
     *
     * @param physicalIndexScan the {@code PhysicalIndexScan} object that provides the index details
     * @param value the value used to determine the upper boundary of the range
     * @param indexSubspace the {@code Subspace} representing the index subspace in which the range is defined
     * @return an {@code IndexRange} defining the range with the specified beginning and end key selectors
     */
    protected static IndexRange lessThanOrEqual(PhysicalIndexScan physicalIndexScan, Object value, Subspace indexSubspace) {
        Tuple tuple = IndexBuilder.beginningOfIndexRange(physicalIndexScan.getIndex()).addObject(value);
        byte[] key = indexSubspace.pack(tuple);
        KeySelector begin = KeySelector.firstGreaterOrEqual(beginningOfIndexRange(indexSubspace, physicalIndexScan.getIndex()));
        KeySelector end = KeySelector.firstGreaterThan(key);
        return new IndexRange(begin, end);
    }
}
