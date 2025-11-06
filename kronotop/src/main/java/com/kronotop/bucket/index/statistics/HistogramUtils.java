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

package com.kronotop.bucket.index.statistics;


import com.kronotop.bucket.BSONUtil;
import org.bson.BsonValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * Utility class for building and querying histograms from sorted BSON values.
 * Works efficiently with up to ~1000 elements.
 */
public final class HistogramUtils {

    private HistogramUtils() {
        // Utility class; prevent instantiation
    }

    /**
     * Builds a histogram by partitioning sorted BSON values into buckets of approximately equal size.
     * The histogram contains at most 10 buckets, with each bucket tracking its min/max value range and count.
     *
     * @param values sorted TreeSet of BSON values to partition into histogram buckets
     * @return list of histogram buckets ordered by value range, empty if input is empty
     */
    public static List<HistogramBucket> buildHistogram(TreeSet<BsonValue> values) {
        List<HistogramBucket> buckets = new ArrayList<>();
        int size = values.size();
        if (size == 0) return buckets;

        int bucketCount = Math.min(10, size);
        int bucketSize = (int) Math.ceil(size / (double) bucketCount);

        Iterator<BsonValue> iterator = values.iterator();
        BsonValue bucketStart = iterator.next();
        BsonValue current = bucketStart;
        int count = 1;

        while (iterator.hasNext()) {
            current = iterator.next();
            if (count % bucketSize == 0) {
                buckets.add(new HistogramBucket(bucketStart, current, bucketSize));
                bucketStart = current;
            }
            count++;
        }

        // Add final bucket if not already covered
        if (buckets.isEmpty() || !buckets.get(buckets.size() - 1).max().equals(current)) {
            buckets.add(new HistogramBucket(bucketStart, current, count % bucketSize));
        }

        return buckets;
    }

    /**
     * Locates the histogram bucket containing the specified BSON value using binary search.
     *
     * @param buckets list of histogram buckets sorted by value range
     * @param value   BSON value to locate within the histogram
     * @return matching bucket if value falls within its [min, max] range, null if value is outside all buckets
     */
    public static HistogramBucket findBucket(List<HistogramBucket> buckets, BsonValue value) {
        if (buckets.isEmpty()) return null;

        int low = 0, high = buckets.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            HistogramBucket b = buckets.get(mid);


            if (BSONUtil.compareBsonValues(value, b.min()) < 0) {
                high = mid - 1;
            } else if (BSONUtil.compareBsonValues(value, b.max()) > 0) {
                low = mid + 1;
            } else {
                return b; // value within [min, max]
            }
        }
        return null;
    }

    /**
     * Estimates the percentile rank of a BSON value within the histogram's value distribution.
     * Returns the bucket's position as a percentile (0-100), with values below the first bucket returning 0.0
     * and values above the last bucket returning 100.0.
     *
     * @param buckets list of histogram buckets sorted by value range
     * @param value   BSON value to compute percentile for
     * @return estimated percentile rank between 0.0 and 100.0, or 0.0 if buckets is empty
     */
    public static double findPercentile(List<HistogramBucket> buckets, BsonValue value) {
        if (buckets.isEmpty()) return 0.0;

        HistogramBucket bucket = findBucket(buckets, value);
        if (bucket == null) {
            if (BSONUtil.compareBsonValues(value, buckets.get(0).min()) < 0) return 0.0;
            return 100.0;
        }

        int index = buckets.indexOf(bucket);
        int totalBuckets = buckets.size();

        double percentile = ((index + 1) / (double) totalBuckets) * 100.0;
        return Math.max(0, Math.min(100, percentile));
    }
}
