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

import java.nio.ByteBuffer;
import java.util.List;

public class HistogramCodec {
    public static final byte[] CRLF = new byte[]{13, 10};

    public static byte[] encode(List<HistogramBucket> histogram, long version) {
        int total = 0;
        byte[][] items = new byte[histogram.size()][];
        for (int i = 0; i < histogram.size(); i++) {
            HistogramBucket bucket = histogram.get(i);
            byte[] data = HistogramBucketCodec.encode(bucket);
            total += data.length;
            total += CRLF.length;
            items[i] = data;
        }

        total += 8; // version
        ByteBuffer buffer = ByteBuffer.allocate(total);
        buffer.putLong(version);
        for (byte[] item : items) {
            buffer.put(item);
            buffer.put(CRLF);
        }
        return buffer.array();
    }
}
