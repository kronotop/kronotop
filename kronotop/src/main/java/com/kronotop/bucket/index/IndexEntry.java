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

package com.kronotop.bucket.index;

import com.kronotop.volume.EntryMetadata;

import java.util.Arrays;

public record IndexEntry(int shardId, byte[] entryMetadata) {
    public static int SHARD_ID_SIZE = 4;

    public static IndexEntry decode(byte[] encoded) {
        int shardId = ((encoded[0] & 0xFF) << 24) |
                ((encoded[1] & 0xFF) << 16) |
                ((encoded[2] & 0xFF) << 8) |
                (encoded[3] & 0xFF);
        byte[] metadata = Arrays.copyOfRange(encoded, SHARD_ID_SIZE, encoded.length);
        return new IndexEntry(shardId, metadata);
    }

    // 4 bytes for shardId (int), plus entry metadata length
    public byte[] encode() {
        byte[] result = new byte[SHARD_ID_SIZE + EntryMetadata.SIZE];
        result[0] = (byte) ((shardId >>> 24) & 0xFF);
        result[1] = (byte) ((shardId >>> 16) & 0xFF);
        result[2] = (byte) ((shardId >>> 8) & 0xFF);
        result[3] = (byte) (shardId & 0xFF);
        System.arraycopy(entryMetadata, 0, result, SHARD_ID_SIZE, EntryMetadata.SIZE);
        return result;
    }
}
