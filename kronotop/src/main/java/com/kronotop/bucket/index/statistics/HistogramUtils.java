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

import java.util.*;

/**
 * Utility class for building and querying histograms from sorted BSON values.
 * Works efficiently with up to ~1000 elements.
 */
public final class HistogramUtils {

    private HistogramUtils() {
        // Utility class; prevent instantiation
    }

    /**
     * Builds a simple histogram from a sorted TreeSet of BsonValue instances.
     * Each bucket represents an approximate percentile range.
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
     * Finds which histogram bucket a given BSON value falls into using binary search.
     * Returns null if the value is outside all bucket ranges.
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

    public static double findPercentile(List<HistogramBucket> buckets, BsonValue value) {
        if (buckets.isEmpty()) return 0.0;

        HistogramBucket bucket = findBucket(buckets, value);
        if (bucket == null) {
            // değer aralıktan küçükse -> 0, büyükse -> 100
            if (BSONUtil.compareBsonValues(value, buckets.get(0).min()) < 0) return 0.0;
            return 100.0;
        }

        int index = buckets.indexOf(bucket);
        int totalBuckets = buckets.size();

        // bucket'ın sırası yüzdesel olarak
        double percentile = ((index + 1) / (double) totalBuckets) * 100.0;
        return Math.max(0, Math.min(100, percentile));
    }


    // Example usage
    public static void main(String[] args) {
        TreeSet<BsonValue> set = new TreeSet<>(Comparator.comparingLong(v -> v.asInt64().getValue()));
        for (int i = 0; i < 1000; i++) {
            set.add(new org.bson.BsonInt64(i));
        }

        List<HistogramBucket> histogram = buildHistogram(set);
        histogram.forEach(System.out::println);

        BsonValue testValue = new org.bson.BsonInt64(120);
        HistogramBucket bucket = findBucket(histogram, testValue);
        System.out.println("Value " + testValue + " is in bucket: " + bucket);
        System.out.println(findPercentile(histogram, testValue));
    }

}
