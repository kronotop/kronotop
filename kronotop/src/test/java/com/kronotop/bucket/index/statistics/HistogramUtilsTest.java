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

import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.*;

class HistogramUtilsTest {

    @Test
    void testBuildHistogram_EmptyInput() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertTrue(histogram.isEmpty());
    }

    @Test
    void testBuildHistogram_SingleValue() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        values.add(new BsonInt32(42));

        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertEquals(1, histogram.size());
        assertEquals(42, histogram.get(0).min().asInt32().getValue());
        assertEquals(42, histogram.get(0).max().asInt32().getValue());
        assertEquals(0, histogram.get(0).count());
    }

    @Test
    void testBuildHistogram_TwoValues() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        values.add(new BsonInt32(10));
        values.add(new BsonInt32(20));

        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertEquals(1, histogram.size());
        assertEquals(10, histogram.get(0).min().asInt32().getValue());
        assertEquals(20, histogram.get(0).max().asInt32().getValue());
    }

    @Test
    void testBuildHistogram_TenValues() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 10; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertTrue(histogram.size() <= 10);
        assertEquals(1, histogram.get(0).min().asInt32().getValue());
        assertEquals(10, histogram.get(histogram.size() - 1).max().asInt32().getValue());
    }

    @Test
    void testBuildHistogram_MaxTenBuckets() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 100; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertTrue(histogram.size() <= 10);
        assertEquals(1, histogram.get(0).min().asInt32().getValue());
        assertEquals(100, histogram.get(histogram.size() - 1).max().asInt32().getValue());
    }

    @Test
    void testBuildHistogram_BucketSizeCalculation() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 50; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertFalse(histogram.isEmpty());
        assertEquals(1, histogram.get(0).min().asInt32().getValue());
        assertEquals(50, histogram.get(histogram.size() - 1).max().asInt32().getValue());
    }

    @Test
    void testFindBucket_EmptyBuckets() {
        Histogram histogram = Histogram.create();
        BsonValue value = new BsonInt32(42);

        HistogramBucket result = HistogramUtils.findBucket(histogram, value);

        assertNull(result);
    }

    @Test
    void testFindBucket_ValueInRange() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 10; i++) {
            values.add(new BsonInt32(i * 10));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(50);

        HistogramBucket result = HistogramUtils.findBucket(histogram, searchValue);

        assertNotNull(result);
        assertTrue(searchValue.asInt32().getValue() >= result.min().asInt32().getValue());
        assertTrue(searchValue.asInt32().getValue() <= result.max().asInt32().getValue());
    }

    @Test
    void testFindBucket_ValueBelowRange() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 10; i <= 20; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(5);

        HistogramBucket result = HistogramUtils.findBucket(histogram, searchValue);

        assertNull(result);
    }

    @Test
    void testFindBucket_ValueAboveRange() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 10; i <= 20; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(25);

        HistogramBucket result = HistogramUtils.findBucket(histogram, searchValue);

        assertNull(result);
    }

    @Test
    void testFindBucket_ValueAtMinBoundary() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 10; i <= 20; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(10);

        HistogramBucket result = HistogramUtils.findBucket(histogram, searchValue);

        assertNotNull(result);
        assertEquals(10, result.min().asInt32().getValue());
    }

    @Test
    void testFindBucket_ValueAtMaxBoundary() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 10; i <= 20; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(20);

        HistogramBucket result = HistogramUtils.findBucket(histogram, searchValue);

        assertNotNull(result);
        assertEquals(20, result.max().asInt32().getValue());
    }

    @Test
    void testFindPercentile_EmptyBuckets() {
        Histogram histogram = Histogram.create();
        BsonValue value = new BsonInt32(42);

        double percentile = HistogramUtils.findPercentile(histogram, value);

        assertEquals(0.0, percentile);
    }

    @Test
    void testFindPercentile_ValueBelowRange() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 10; i <= 20; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(5);

        double percentile = HistogramUtils.findPercentile(histogram, searchValue);

        assertEquals(0.0, percentile);
    }

    @Test
    void testFindPercentile_ValueAboveRange() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 10; i <= 20; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(25);

        double percentile = HistogramUtils.findPercentile(histogram, searchValue);

        assertEquals(100.0, percentile);
    }

    @Test
    void testFindPercentile_ValueInFirstBucket() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 100; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(5);

        double percentile = HistogramUtils.findPercentile(histogram, searchValue);

        assertTrue(percentile > 0.0 && percentile <= 20.0);
    }

    @Test
    void testFindPercentile_ValueInLastBucket() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 100; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonInt32(95);

        double percentile = HistogramUtils.findPercentile(histogram, searchValue);

        assertTrue(percentile >= 80.0 && percentile <= 100.0);
    }

    @Test
    void testFindPercentile_RangeValidation() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparingInt(a -> a.asInt32().getValue()));
        for (int i = 1; i <= 50; i++) {
            values.add(new BsonInt32(i));
        }

        Histogram histogram = HistogramUtils.buildHistogram(values);

        for (int i = 1; i <= 50; i++) {
            BsonValue searchValue = new BsonInt32(i);
            double percentile = HistogramUtils.findPercentile(histogram, searchValue);

            assertTrue(percentile >= 0.0 && percentile <= 100.0);
        }
    }

    @Test
    void testBuildHistogram_WithStringValues() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparing(a -> a.asString().getValue()));
        values.add(new BsonString("apple"));
        values.add(new BsonString("banana"));
        values.add(new BsonString("cherry"));
        values.add(new BsonString("date"));
        values.add(new BsonString("elderberry"));

        Histogram histogram = HistogramUtils.buildHistogram(values);

        assertFalse(histogram.isEmpty());
        assertEquals("apple", histogram.get(0).min().asString().getValue());
        assertEquals("elderberry", histogram.get(histogram.size() - 1).max().asString().getValue());
    }

    @Test
    void testFindBucket_WithStringValues() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparing(a -> a.asString().getValue()));
        values.add(new BsonString("apple"));
        values.add(new BsonString("banana"));
        values.add(new BsonString("cherry"));
        values.add(new BsonString("date"));
        values.add(new BsonString("elderberry"));

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonString("cherry");

        HistogramBucket result = HistogramUtils.findBucket(histogram, searchValue);

        assertNotNull(result);
    }

    @Test
    void testFindPercentile_WithStringValues() {
        TreeSet<BsonValue> values = new TreeSet<>(Comparator.comparing(a -> a.asString().getValue()));
        values.add(new BsonString("apple"));
        values.add(new BsonString("banana"));
        values.add(new BsonString("cherry"));
        values.add(new BsonString("date"));
        values.add(new BsonString("elderberry"));

        Histogram histogram = HistogramUtils.buildHistogram(values);
        BsonValue searchValue = new BsonString("cherry");

        double percentile = HistogramUtils.findPercentile(histogram, searchValue);

        assertTrue(percentile >= 0.0 && percentile <= 100.0);
    }
}
