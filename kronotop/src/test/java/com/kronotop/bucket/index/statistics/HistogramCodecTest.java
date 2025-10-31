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

import org.bson.BsonInt64;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class HistogramCodecTest {
    @Test
    void test() {
        List<HistogramBucket> histogram = List.of(
                new HistogramBucket(new BsonInt64(10), new BsonInt64(34), 10),
                new HistogramBucket(new BsonInt64(35), new BsonInt64(45), 10)
        );
        System.out.println(Arrays.toString(HistogramCodec.encode(histogram, System.currentTimeMillis())));
    }
}